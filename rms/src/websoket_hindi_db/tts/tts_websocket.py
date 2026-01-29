import asyncio
import base64
import json
import os
import re
import subprocess
import tempfile
import time
from pathlib import Path
from typing import Optional, Tuple

import websockets


def get_audio_duration(input_path: str) -> Optional[float]:
    try:
        cmd = [
            "ffprobe",
            "-v", "error",
            "-show_entries", "format=duration",
            "-of", "default=noprint_wrappers=1:nokey=1",
            input_path,
        ]
        result = subprocess.run(cmd, capture_output=True, text=True)
        if result.returncode == 0:
            return float(result.stdout.strip())
        return None
    except Exception:
        return None


def trim_to_5_seconds(input_path: str, output_path: str) -> bool:
    try:
        cmd = [
            "ffmpeg",
            "-y",
            "-i", input_path,
            "-t", "5",
            "-filter:a", "atempo=1.5", 
            "-ar", "16000",
            "-ac", "1",
            "-acodec", "pcm_s16le",
            "-map_metadata", "-1",
            output_path,
        ]
        print("  ↳ Trimming voice reference to 5 seconds...")
        result = subprocess.run(cmd, capture_output=True, text=True)
        if result.returncode == 0:
            print(f"  ✓ Voice reference trimmed: {output_path}")
            return True
        print(f"  ✗ FFmpeg error: {result.stderr}")
        return False
    except FileNotFoundError:
        print("  ✗ Error: ffmpeg not found. Please install ffmpeg.")
        return False
    except Exception as e:
        print(f"  ✗ Error: {e}")
        return False


def process_audio_file_for_voice_reference(input_path: str) -> Tuple[Optional[bytes], bool]:
    """
    If >500KB or >10s, trim to 5 seconds.
    Returns: (audio_bytes, trimmed_flag)
    """
    try:
        file_size_bytes = Path(input_path).stat().st_size
        file_size_kb = file_size_bytes / 1024
        duration = get_audio_duration(input_path) or 0.0

        print(f"  ↳ Voice reference file: {file_size_kb:.1f}KB, {duration:.1f}s")

        needs_trimming = file_size_bytes > (500 * 1024) or (duration > 10.0)

        if not needs_trimming:
            print("  ✓ Voice reference within limits, using as-is")
            return Path(input_path).read_bytes(), False

        print(f"  ⚠️  Voice reference needs trimming (size: {file_size_kb:.1f}KB, duration: {duration:.1f}s)")

        with tempfile.NamedTemporaryFile(suffix=".wav", delete=False) as tmp:
            temp_output = tmp.name

        success = trim_to_5_seconds(input_path, temp_output)
        if not success:
            print("  ⚠️  Trimming failed, using original file (may cause issues)")
            return Path(input_path).read_bytes(), False

        audio_bytes = Path(temp_output).read_bytes()
        try:
            os.unlink(temp_output)
        except Exception:
            pass

        trimmed_kb = len(audio_bytes) / 1024
        print(f"  ✓ Trimmed to: {trimmed_kb:.1f}KB")
        return audio_bytes, True

    except Exception as e:
        print(f"  ✗ Error processing voice reference: {e}")
        return None, False


class XTTSPersistentClient:
    """Persistent WebSocket client for TTS with auto-reconnect + optional voice cloning"""

    def __init__(self, server_url: str, voice_clone_path: Optional[str] = None):
        self.server_url = server_url
        self.voice_clone_path = voice_clone_path

        self.ws = None
        self.lock = asyncio.Lock()
        self.connected = False
        self.reconnect_attempts = 0
        self.max_reconnect_attempts = 5
        self.reconnect_delay = 1.0

        self.voice_reference_b64 = None
        self.voice_reference_loaded = False
        self.voice_reference_trimmed = False

        # Language to model mapping
        self.language_model_map = {
            "hi": "xtts",      # Hindi - XTTS v2
            "en": "xtts",      # English - XTTS v2
            "gu": "mms_tts_guj"  # Gujarati - MMS
        }

        # Language fallback texts
        self.fallback_texts = {
            "hi": "माफ़ कीजिए, कृपया फिर से बोलिए।",
            "en": "Sorry, please say that again.",
            "gu": "માફ કરશો, કૃપા કરીને ફરીથી બોલો."
        }

        if self.voice_clone_path:
            self._preload_voice_reference()

    def _preload_voice_reference(self):
        try:
            voice_path = Path(self.voice_clone_path)
            if not voice_path.exists():
                print(f"⚠️ Voice reference file not found: {self.voice_clone_path}")
                return

            print(f"📁 Loading voice reference: {self.voice_clone_path}")
            audio_bytes, trimmed = process_audio_file_for_voice_reference(self.voice_clone_path)
            if audio_bytes is None:
                print("❌ Failed to load voice reference")
                return

            self.voice_reference_trimmed = trimmed
            self.voice_reference_b64 = base64.b64encode(audio_bytes).decode("ascii")
            self.voice_reference_loaded = True

            b64_size_kb = len(self.voice_reference_b64) / 1024
            audio_size_kb = len(audio_bytes) / 1024
            status = "(trimmed to 5s)" if trimmed else "(original)"
            print(f"✅ Voice reference loaded {status} ({audio_size_kb:.1f}KB audio, {b64_size_kb:.1f}KB base64)")

        except Exception as e:
            print(f"❌ Error loading voice reference: {e}")

    async def connect(self) -> bool:
        async with self.lock:
            if self.connected and self.ws:
                try:
                    if self.ws.state == websockets.protocol.State.OPEN:
                        return True
                except Exception:
                    self.connected = False
                    self.ws = None

            try:
                print("🔗 Connecting to TTS WebSocket...")
                self.ws = await websockets.connect(
                    self.server_url,
                    ping_interval=10,
                    ping_timeout=20,
                    close_timeout=30,
                    max_size=50 * 1024 * 1024,
                    compression=None,
                )
                self.connected = True
                self.reconnect_attempts = 0
                print("✅ TTS WebSocket connected")
                return True

            except Exception as e:
                self.connected = False
                self.ws = None
                self.reconnect_attempts += 1

                if self.reconnect_attempts <= self.max_reconnect_attempts:
                    print(
                        f"⚠️ TTS WebSocket connection failed "
                        f"(attempt {self.reconnect_attempts}/{self.max_reconnect_attempts}): {e}"
                    )
                    await asyncio.sleep(self.reconnect_delay * self.reconnect_attempts)
                    return await self.connect()

                print("❌ Failed to connect to TTS WebSocket after max attempts")
                return False

    async def ensure_connection(self) -> bool:
        if not self.connected or not self.ws:
            return await self.connect()

        try:
            if self.ws.state != websockets.protocol.State.OPEN:
                print(f"⚠️ TTS WebSocket state={self.ws.state}, reconnecting...")
                self.connected = False
                self.ws = None
                return await self.connect()
            return True
        except Exception as e:
            print(f"⚠️ TTS WebSocket check failed: {e}, reconnecting...")
            self.connected = False
            self.ws = None
            return await self.connect()

    @staticmethod
    def _safe_text_cleanup(text: str) -> str:
        """
        Clean text WITHOUT deleting any language characters.
        - removes control chars
        - collapses whitespace
        """
        if not text:
            return ""
        text = re.sub(r"[\x00-\x08\x0B\x0C\x0E-\x1F\x7F]", " ", text)
        text = re.sub(r"\s+", " ", text).strip()
        return text

    def _fallback_text(self, language: str) -> str:
        return "माफ़ कीजिए, कृपया फिर से बोलिए।" if language == "hi" else "Sorry, please say that again."
    
    async def tts(self, text: str, prompt_id: int, language: str = "hi") -> Tuple[Optional[bytes], float]:
        start_time = time.time()

         # Validate language
        if language not in self.language_model_map:
            print(f"⚠️ Unsupported language: {language}, defaulting to 'hi'")
            language = "hi"

        # Get model ID based on language
        model_id = self.language_model_map[language]

        # ✅ DO NOT remove Hindi characters
        text_clean = self._safe_text_cleanup(text)
        if not text_clean:
            text_clean = self._fallback_text(language)

        print(f"🎤 TTS [{language.upper()}] Input: {text_clean}")
        print(f"🔤 Using model: {model_id}")
        
        for attempt in range(3):
            try:
                if not await self.ensure_connection():
                    print("❌ TTS connection failed, cannot send request")
                    return None, time.time() - start_time

                request = {
                    "model_id": model_id,
                    "prompt": text_clean,
                    "prompt_id": prompt_id,
                    # # ✅ Respect requested language ("hi" or "en")
                    # "language": language,
                }

                # Only add voice cloning for TTS model (Hindi/English)
                if model_id == "xtts" and self.voice_reference_loaded:
                    request["voice_cloning"] = True
                    request["voice_reference"] = self.voice_reference_b64
                    voice_status = "trimmed to 5s" if self.voice_reference_trimmed else "original"
                    print(f"🎤 Voice cloning enabled for XTTS ({voice_status})")

                await asyncio.wait_for(self.ws.send(json.dumps(request)), timeout=5.0)
                response = await asyncio.wait_for(self.ws.recv(), timeout=30.0)

                if not response:
                    print("⚠️ TTS returned empty response frame")
                    raise Exception("Empty WS response")

                try:
                    response_data = json.loads(response)
                except Exception as e:
                    print(f"⚠️ TTS returned non-JSON: {response[:300]}")
                    raise

                elapsed_time = time.time() - start_time

                if "error" in response_data:
                    err = response_data.get("error")
                    if err is None or (isinstance(err, str) and err.strip() == ""):
                        print("⚠️ TTS returned blank error -> forcing reconnect")
                        self.connected = False
                        if self.ws:
                            try:
                                await self.ws.close()
                            except:
                                pass
                        self.ws = None
                        # retry once after reconnect
                        await asyncio.sleep(0.2)
                        continue
                    return None, elapsed_time

                if "audio_b64" in response_data:
                    return base64.b64decode(response_data["audio_b64"]), elapsed_time

                return None, elapsed_time

            except websockets.exceptions.ConnectionClosed as e:
                print(f"⚠️ TTS connection closed: {e}, reconnecting...")
                self.connected = False
                self.ws = None
                if attempt < 2:
                    await asyncio.sleep(1.0)
                    continue
                return None, time.time() - start_time

            except asyncio.TimeoutError:
                print(f"⚠️ TTS timeout on attempt {attempt + 1}")
                if attempt < 2:
                    await asyncio.sleep(1.0)
                    continue
                return None, time.time() - start_time

            except Exception as e:
                print(f"⚠️ TTS error on attempt {attempt + 1}: {e}")
                if attempt < 2:
                    await asyncio.sleep(1.0)
                    continue
                return None, time.time() - start_time

        return None, time.time() - start_time

    async def close(self):
        async with self.lock:
            if self.ws:
                try:
                    await self.ws.close()
                    self.connected = False
                    print("🔌 TTS WebSocket closed")
                except Exception:
                    pass
                finally:
                    self.ws = None


import asyncio
import base64
import json
import os
import re
import subprocess
import tempfile
import time
from pathlib import Path
from typing import Optional, Tuple, List, Dict, Any
import websockets
import numpy as np
import logging

logger = logging.getLogger(__name__)

def get_audio_duration(input_path: str) -> Optional[float]:
    """Get audio duration using ffprobe"""
    try:
        cmd = [
            "ffprobe",
            "-v", "error",
            "-show_entries", "format=duration",
            "-of", "default=noprint_wrappers=1:nokey=1",
            input_path,
        ]
        result = subprocess.run(cmd, capture_output=True, text=True)
        if result.returncode == 0:
            return float(result.stdout.strip())
        return None
    except Exception as e:
        logger.error(f"Error getting audio duration: {e}")
        return None

def trim_to_5_seconds(input_path: str, output_path: str) -> bool:
    """Trim audio to 5 seconds using ffmpeg"""
    try:
        cmd = [
            "ffmpeg",
            "-y",
            "-i", input_path,
            "-t", "5",
            "-filter:a", "atempo=1.5", 
            "-ar", "16000",
            "-ac", "1",
            "-acodec", "pcm_s16le",
            "-map_metadata", "-1",
            output_path,
        ]
        logger.info("Trimming voice reference to 5 seconds...")
        result = subprocess.run(cmd, capture_output=True, text=True)
        if result.returncode == 0:
            logger.info(f"Voice reference trimmed: {output_path}")
            return True
        logger.error(f"FFmpeg error: {result.stderr}")
        return False
    except FileNotFoundError:
        logger.error("Error: ffmpeg not found. Please install ffmpeg.")
        return False
    except Exception as e:
        logger.error(f"Error trimming audio: {e}")
        return False

def process_audio_file_for_voice_reference(input_path: str) -> Tuple[Optional[bytes], bool]:
    """
    Process audio file for voice cloning
    Returns: (audio_bytes, trimmed_flag)
    """
    try:
        file_size_bytes = Path(input_path).stat().st_size
        file_size_kb = file_size_bytes / 1024
        duration = get_audio_duration(input_path) or 0.0

        logger.info(f"Voice reference file: {file_size_kb:.1f}KB, {duration:.1f}s")

        needs_trimming = file_size_bytes > (500 * 1024) or (duration > 10.0)

        if not needs_trimming:
            logger.info("Voice reference within limits, using as-is")
            return Path(input_path).read_bytes(), False

        logger.warning(f"Voice reference needs trimming (size: {file_size_kb:.1f}KB, duration: {duration:.1f}s)")

        with tempfile.NamedTemporaryFile(suffix=".wav", delete=False) as tmp:
            temp_output = tmp.name

        success = trim_to_5_seconds(input_path, temp_output)
        if not success:
            logger.warning("Trimming failed, using original file (may cause issues)")
            return Path(input_path).read_bytes(), False

        audio_bytes = Path(temp_output).read_bytes()
        try:
            os.unlink(temp_output)
        except Exception:
            pass

        trimmed_kb = len(audio_bytes) / 1024
        logger.info(f"Trimmed to: {trimmed_kb:.1f}KB")
        return audio_bytes, True

    except Exception as e:
        logger.error(f"Error processing voice reference: {e}")
        return None, False

class XTTSStreamingClient:
    """Streaming WebSocket client for TTS with chunked output"""

    def __init__(self, server_url: str, voice_clone_path: Optional[str] = None):
        self.server_url = server_url
        self.voice_clone_path = voice_clone_path

        self.ws = None
        self.lock = asyncio.Lock()
        self.connected = False
        self.reconnect_attempts = 0
        self.max_reconnect_attempts = 5
        self.reconnect_delay = 1.0

        self.voice_reference_b64 = None
        self.voice_reference_loaded = False
        self.voice_reference_trimmed = False

        # Language to model mapping
        self.language_model_map = {
            "hi": "xtts",      # Hindi - XTTS v2
            "en": "xtts",      # English - XTTS v2
            "gu": "mms_tts_guj"  # Gujarati - MMS
        }

        # Streaming metrics
        self.metrics = {
            "chunks_generated": 0,
            "total_chars": 0,
            "synthesis_latency": [],
            "errors": 0,
            "first_chunk_latency": 0
        }

        if self.voice_clone_path:
            self._preload_voice_reference()

    def _preload_voice_reference(self):
        """Preload voice reference for cloning"""
        try:
            voice_path = Path(self.voice_clone_path)
            if not voice_path.exists():
                logger.warning(f"Voice reference file not found: {self.voice_clone_path}")
                return

            logger.info(f"Loading voice reference: {self.voice_clone_path}")
            audio_bytes, trimmed = process_audio_file_for_voice_reference(self.voice_clone_path)
            if audio_bytes is None:
                logger.error("Failed to load voice reference")
                return

            self.voice_reference_trimmed = trimmed
            self.voice_reference_b64 = base64.b64encode(audio_bytes).decode("ascii")
            self.voice_reference_loaded = True

            b64_size_kb = len(self.voice_reference_b64) / 1024
            audio_size_kb = len(audio_bytes) / 1024
            status = "(trimmed to 5s)" if trimmed else "(original)"
            logger.info(f"Voice reference loaded {status} ({audio_size_kb:.1f}KB audio, {b64_size_kb:.1f}KB base64)")

        except Exception as e:
            logger.error(f"Error loading voice reference: {e}")

    async def connect(self) -> bool:
        """Establish WebSocket connection"""
        async with self.lock:
            if self.connected and self.ws:
                try:
                    if self.ws.state == websockets.protocol.State.OPEN:
                        return True
                except Exception:
                    self.connected = False
                    self.ws = None

            try:
                logger.info("🔗 Connecting to TTS WebSocket...")
                self.ws = await websockets.connect(
                    self.server_url,
                    ping_interval=30,
                    ping_timeout=30,
                    close_timeout=30,
                    max_size=10 * 1024 * 1024,
                    compression=None,
                )
                self.connected = True
                self.reconnect_attempts = 0
                logger.info("✅ TTS WebSocket connected")
                return True

            except Exception as e:
                self.connected = False
                self.ws = None
                self.reconnect_attempts += 1
                self.metrics["errors"] += 1

                if self.reconnect_attempts <= self.max_reconnect_attempts:
                    logger.warning(
                        f"TTS WebSocket connection failed "
                        f"(attempt {self.reconnect_attempts}/{self.max_reconnect_attempts}): {e}"
                    )
                    await asyncio.sleep(self.reconnect_delay * self.reconnect_attempts)
                    return await self.connect()

                logger.error("❌ Failed to connect to TTS WebSocket after max attempts")
                return False

    async def ensure_connection(self) -> bool:
        """Ensure we have a valid connection"""
        if not self.connected or not self.ws:
            return await self.connect()

        try:
            if self.ws.state != websockets.protocol.State.OPEN:
                logger.warning(f"TTS WebSocket state={self.ws.state}, reconnecting...")
                self.connected = False
                self.ws = None
                return await self.connect()
            return True
        except Exception as e:
            logger.warning(f"TTS WebSocket check failed: {e}, reconnecting...")
            self.connected = False
            self.ws = None
            return await self.connect()

    @staticmethod
    def _clean_text(text: str) -> str:
        """Clean text while preserving language characters"""
        if not text:
            return ""
        
        # Remove control characters
        text = re.sub(r"[\x00-\x08\x0B\x0C\x0E-\x1F\x7F]", " ", text)
        
        # Collapse whitespace
        text = re.sub(r"\s+", " ", text).strip()
        
        return text

    async def streaming_tts(self, text: str, prompt_id: int, language: str = "hi") -> Tuple[Optional[asyncio.Queue], float]:
        """Streaming TTS with proper sample rate handling"""
        start_time = time.time()
        logger.info(f"Starting streaming TTS (Prompt ID: {prompt_id})")

        # Validate language
        if language not in self.language_model_map:
            logger.warning(f"Unsupported language: {language}, defaulting to 'hi'")
            language = "hi"

        # Get model ID
        model_id = self.language_model_map[language]

        # Clean text
        text_clean = self._clean_text(text)
        if not text_clean:
            text_clean = "माफ़ कीजिए, कृपया फिर से बोलिए।" if language == "hi" else "Sorry, please say that again."

        logger.info(f"🎤 TTS [{language.upper()}] Input: {len(text_clean)} chars")
        logger.info(f"🔤 Using model: {model_id}")
        
        # Create output queue for streaming audio chunks
        audio_queue = asyncio.Queue()
        
        for attempt in range(2):
            try:
                if not await self.ensure_connection():
                    logger.error("❌ TTS connection failed")
                    return None, time.time() - start_time

                # Prepare request
                request = {
                    "model_id": model_id,
                    "prompt": text_clean,
                    "prompt_id": prompt_id,
                    "language": language,
                    "task": "tts",
                    "stream": False,  # Explicitly set to false for single response
                }

                # Add voice cloning for XTTS model
                if model_id == "xtts" and self.voice_reference_loaded:
                    request["voice_cloning"] = True
                    request["voice_reference"] = self.voice_reference_b64
                    voice_status = "trimmed to 5s" if self.voice_reference_trimmed else "original"
                    logger.info(f"🎤 Voice cloning enabled for XTTS ({voice_status})")

                logger.debug(f"Sending TTS request: {prompt_id}")
                
                # Send request
                try:
                    await asyncio.wait_for(self.ws.send(json.dumps(request)), timeout=10.0)
                except asyncio.TimeoutError:
                    logger.error("Timeout sending TTS request")
                    if attempt < 1:
                        self.connected = False
                        self.ws = None
                        await asyncio.sleep(1.0)
                        continue
                    return None, time.time() - start_time

                # Wait for response
                try:
                    response = await asyncio.wait_for(self.ws.recv(), timeout=15.0)
                except asyncio.TimeoutError:
                    logger.error("Timeout waiting for TTS response")
                    if attempt < 1:
                        self.connected = False
                        self.ws = None
                        await asyncio.sleep(1.0)
                        continue
                    return None, time.time() - start_time

                # Parse response
                try:
                    response_data = json.loads(response)
                except json.JSONDecodeError as e:
                    logger.error(f"Failed to parse JSON response: {e}")
                    if attempt < 1:
                        continue
                    return None, time.time() - start_time

                # Check for errors
                if "error" in response_data:
                    err = response_data.get("error", "").strip()
                    if err:
                        logger.error(f"TTS error: {err}")
                    else:
                        logger.error("TTS returned empty error")
                    if attempt < 1:
                        continue
                    return None, time.time() - start_time

                # Process audio
                if "audio_b64" in response_data:
                    audio_b64 = response_data["audio_b64"]
                    
                    try:
                        # Decode audio
                        audio_bytes = base64.b64decode(audio_b64)
                        
                        # Check audio size
                        if len(audio_bytes) < 100:
                            logger.error(f"Audio too small: {len(audio_bytes)} bytes")
                            if attempt < 1:
                                continue
                            return None, time.time() - start_time
                        
                        # Get sample rate from response or default
                        sr = response_data.get("sr", 24000)  # XTTS default is 24kHz
                        logger.info(f"Audio sample rate: {sr}Hz")
                        
                        # Convert to float32 for playback
                        # XTTS outputs 16-bit PCM
                        audio_array = np.frombuffer(audio_bytes, dtype=np.int16).astype(np.float32) / 32768.0
                        
                        # Track latency
                        elapsed_time = time.time() - start_time
                        self.metrics["first_chunk_latency"] = elapsed_time
                        logger.info(f"Audio received in {elapsed_time:.3f}s, size: {len(audio_bytes)/1024:.1f}KB, {sr}Hz")
                        
                        # Resample if needed (XTTS outputs 24kHz, we need 16kHz for playback)
                        target_sr = 16000
                        if sr != target_sr:
                            logger.info(f"Resampling from {sr}Hz to {target_sr}Hz")
                            try:
                                import scipy.signal
                                # Calculate resampling ratio
                                ratio = target_sr / sr
                                target_length = int(len(audio_array) * ratio)
                                audio_array = scipy.signal.resample(audio_array, target_length)
                                logger.info(f"Resampled to {len(audio_array)} samples at {target_sr}Hz")
                            except ImportError:
                                logger.warning("scipy not installed, cannot resample. Audio may play too fast.")
                            except Exception as e:
                                logger.error(f"Resampling error: {e}")
                        
                        # Split into chunks for streaming playback
                        chunk_duration = 0.5  # 0.5 second chunks
                        chunk_size = int(target_sr * chunk_duration)
                        
                        # Add small fade in/out to each chunk to avoid clicks
                        fade_samples = min(100, chunk_size // 10)
                        
                        if len(audio_array) > chunk_size:
                            for i in range(0, len(audio_array), chunk_size):
                                chunk = audio_array[i:i + chunk_size]
                                
                                # Apply fade in/out
                                if len(chunk) > fade_samples * 2:
                                    # Fade in
                                    fade_in = np.linspace(0, 1, fade_samples)
                                    chunk[:fade_samples] *= fade_in
                                    
                                    # Fade out
                                    fade_out = np.linspace(1, 0, fade_samples)
                                    chunk[-fade_samples:] *= fade_out
                                
                                await audio_queue.put(chunk)
                                self.metrics["chunks_generated"] += 1
                        else:
                            # Single chunk, still apply fade
                            if len(audio_array) > fade_samples * 2:
                                fade_in = np.linspace(0, 1, fade_samples)
                                audio_array[:fade_samples] *= fade_in
                                fade_out = np.linspace(1, 0, fade_samples)
                                audio_array[-fade_samples:] *= fade_out
                            
                            await audio_queue.put(audio_array)
                            self.metrics["chunks_generated"] += 1
                        
                        # Signal end
                        await audio_queue.put(None)
                        
                        # Update metrics
                        self.metrics["total_chars"] += len(text_clean)
                        self.metrics["synthesis_latency"].append(elapsed_time)
                        
                        logger.info(f"TTS successful: {elapsed_time:.3f}s, {len(audio_bytes)/1024:.1f}KB audio, {self.metrics['chunks_generated']} chunks")
                        
                        return audio_queue, elapsed_time
                        
                    except Exception as e:
                        logger.error(f"Error processing audio: {e}")
                        if attempt < 1:
                            continue
                        return None, time.time() - start_time
                
                else:
                    logger.error(f"Unexpected response format: {list(response_data.keys())}")
                    if attempt < 1:
                        continue
                    return None, time.time() - start_time

            except websockets.exceptions.ConnectionClosed as e:
                logger.error(f"⚠️ TTS connection closed: {e}")
                self.connected = False
                self.ws = None
                self.metrics["errors"] += 1
                if attempt < 1:
                    await asyncio.sleep(2.0)
                    continue
                return None, time.time() - start_time

            except Exception as e:
                logger.error(f"⚠️ TTS error on attempt {attempt + 1}: {e}")
                self.metrics["errors"] += 1
                if attempt < 1:
                    await asyncio.sleep(2.0)
                    continue
                return None, time.time() - start_time

        return None, time.time() - start_time

    async def close(self):
        """Close the WebSocket connection"""
        async with self.lock:
            if self.ws:
                try:
                    await self.ws.close()
                    self.connected = False
                    logger.info("🔌 TTS WebSocket closed")
                    
                    # Log final metrics
                    if self.metrics["synthesis_latency"]:
                        avg_latency = sum(self.metrics["synthesis_latency"]) / len(self.metrics["synthesis_latency"])
                        logger.info(f"TTS Final Metrics: "
                                  f"avg_latency={avg_latency:.3f}s, "
                                  f"total_chunks={self.metrics['chunks_generated']}, "
                                  f"total_chars={self.metrics['total_chars']}, "
                                  f"avg_first_chunk={self.metrics['first_chunk_latency']:.3f}s, "
                                  f"total_errors={self.metrics['errors']}")
                              
                except Exception as e:
                    logger.error(f"Error closing TTS WebSocket: {e}")
                finally:
                    self.ws = None