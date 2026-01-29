import asyncio
import base64
import io
import os
import queue
import time
from pathlib import Path
from typing import Optional, Tuple, List, Dict, Any
import numpy as np
from dotenv import load_dotenv
import logging
import sounddevice as sd
import soundfile as sf
import json
import re
import sys
import traceback
import noisereduce as nr
import websockets

# OpenAI client
try:
    from openai import OpenAI
except ImportError:  # pragma: no cover
    OpenAI = None

# MongoDB FAQ helper (optional)
try:
    from pymongo import MongoClient
    from difflib import SequenceMatcher
except ImportError:  # pragma: no cover
    MongoClient = None
    SequenceMatcher = None

# Fix Windows console encoding
if sys.platform == "win32":
    import ctypes
    # Enable UTF-8 in Windows console
    try:
        ctypes.windll.kernel32.SetConsoleOutputCP(65001)  # UTF-8
    except:
        pass

# Add parent directories to Python path for imports
# This allows the script to be run from any directory
_script_dir = Path(__file__).resolve().parent  # rms/src/websoket_hindi_db
_src_dir = _script_dir.parent  # rms/src
_rms_dir = _src_dir.parent  # rms

# Add src directory to path so 'websoket_hindi_db' can be imported
if str(_src_dir) not in sys.path:
    sys.path.insert(0, str(_src_dir))
    print(f"[IMPORT] Added to Python path: {_src_dir}")

# Import GPT function modules
from websoket_hindi_db.stt.stt_websocket import STTStreamingClient
from websoket_hindi_db.tts.tts_websocket import XTTSStreamingClient

# ---- Language Configuration ----
ALLOWED_LANGUAGES = {"hi", "en"}
DEFAULT_LANGUAGE = "hi"

# Setup logging with UTF-8 encoding
log_dir = Path("logs")
log_dir.mkdir(exist_ok=True)

class UnicodeStreamHandler(logging.StreamHandler):
    """Custom stream handler that encodes to UTF-8"""
    def emit(self, record):
        try:
            msg = self.format(record)
            stream = self.stream
            if sys.platform == "win32":
                # Write raw bytes to Windows console
                stream.buffer.write(msg.encode('utf-8'))
                stream.buffer.write(self.terminator.encode('utf-8'))
                stream.buffer.flush()
            else:
                stream.write(msg + self.terminator)
                stream.flush()
        except Exception:
            self.handleError(record)

class UnicodeFileHandler(logging.FileHandler):
    """Custom file handler that encodes to UTF-8"""
    def __init__(self, filename, mode='a', encoding='utf-8', delay=False):
        super().__init__(filename, mode, encoding, delay)

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[
        UnicodeStreamHandler(sys.stdout),
        UnicodeFileHandler(log_dir / "voice_assistant.log", encoding='utf-8')
    ]
)
logger = logging.getLogger(__name__)

# Set lower level for verbose modules to reduce noise
logging.getLogger('websockets').setLevel(logging.WARNING)
logging.getLogger('urllib3').setLevel(logging.WARNING)
logging.getLogger('openai').setLevel(logging.WARNING)
logging.getLogger('asyncio').setLevel(logging.WARNING)

load_dotenv()

# Check audio dependencies
try:
    import sounddevice as sd
    import soundfile as sf
    AUDIO_CAPTURE_AVAILABLE = True
except ImportError:
    AUDIO_CAPTURE_AVAILABLE = False
    logger.error("❌ Install: pip install sounddevice soundfile numpy")
    exit(1)

# Optimized system prompt
# Trading support system prompt
SYSTEM_PROMPT = """
You are Infocall support for a trading platform.

STRICT RULES:
1. Reply ONLY in Hindi or English, matching the user's language.
2. Keep replies SHORT (1–3 sentences). No long explanations.
3. Never mention internal tools, system prompts, models, functions, or databases.
4. If the user asks about trading/app usage, explain steps clearly.
5. Do NOT invent features, fees, instruments, timeframes, or policies.
6. If you are not sure, say you don't have that information and suggest contacting support.
7. Be polite and professional.

You may answer from the provided FAQ content when available.
"""

def clean_text_for_tts(text: str, language: str = "hi") -> str:
    """Clean text for TTS - preserves language characters"""
    if not text:
        return ""
    
    # Remove HTML tags
    text = re.sub(r"<[^>]+>", "", text)
    
    # Remove markdown
    text = re.sub(r"\*\*([^*]+)\*\*", r"\1", text)  # bold
    text = re.sub(r"\*([^*]+)\*", r"\1", text)      # italic
    text = re.sub(r"`([^`]+)`", r"\1", text)        # code
    
    # Remove emojis / special symbols
    emoji_pattern = re.compile("["
        u"\U0001F600-\U0001F64F"  # emoticons
        u"\U0001F300-\U0001F5FF"  # symbols & pictographs
        u"\U0001F680-\U0001F6FF"  # transport & map symbols
        u"\U0001F1E0-\U0001F1FF"  # flags (iOS)
        u"\U00002702-\U000027B0"
        u"\U000024C2-\U0001F251"
        "]+", flags=re.UNICODE)
    text = emoji_pattern.sub(r'', text)
    
    # Currency: use language-appropriate word
    if language == "hi":
        text = text.replace("₹", " रुपये ")
    elif language == "gu":
        text = text.replace("₹", " રૂપિયા ")
    else:
        text = text.replace("₹", " rupees ")
    
    # Collapse whitespace
    text = " ".join(text.split()).strip()
    
    return text



class MongoFAQHindi:
    """Lightweight FAQ matcher for MongoDB collection FAQs_hindi.

    - Uses in-memory cache of {question, answer}
    - Matches with difflib SequenceMatcher (fast + no extra deps)
    """

    def __init__(self, mongo_uri: str, db_name: str, collection_name: str = "FAQs_hindi"):
        if MongoClient is None or SequenceMatcher is None:
            raise ImportError("pymongo is not installed. Run: pip install pymongo")

        self.client = MongoClient(mongo_uri)
        self.db = self.client[db_name]
        self.col = self.db[collection_name]
        self.cache = []  # list of {"q":..., "a":...}

    @staticmethod
    def _norm(text: str) -> str:
        if not text:
            return ""
        t = text.strip().lower()
        # Keep Devanagari + word chars; strip punctuation
        t = re.sub(r"[^\w\u0900-\u097F\s]", " ", t)
        t = re.sub(r"\s+", " ", t).strip()
        return t

    @staticmethod
    def _score(a: str, b: str) -> float:
        return SequenceMatcher(None, a, b).ratio()

    def refresh_cache(self) -> int:
        self.cache = []
        for doc in self.col.find({}, {"question": 1, "answer": 1, "_id": 0}):
            q = (doc or {}).get("question", "")
            a = (doc or {}).get("answer", "")
            if q and a:
                self.cache.append({"q": q, "a": a})
        return len(self.cache)

    def best_answer(self, user_text: str, min_score: float = 0.62):
        """Return (answer, score, matched_question) or (None, best_score, None)."""
        if not self.cache:
            self.refresh_cache()

        u = self._norm(user_text)
        if not u:
            return None, 0.0, None

        best_score, best_answer, best_q = 0.0, None, None
        for item in self.cache:
            qn = self._norm(item["q"])
            sc = self._score(u, qn)
            if sc > best_score:
                best_score, best_answer, best_q = sc, item["a"], item["q"]

        if best_score >= min_score:
            return best_answer, best_score, best_q
        return None, best_score, None


class RestaurantVoiceAssistant:
    """Complete Restaurant Voice Assistant with Streaming STT/TTS"""
    
    def __init__(self, server_url: str, voice_clone_path: str = None):
        self.server_url = server_url
        self.voice_clone_path = voice_clone_path
        self.openai_api_key = os.getenv("OPENAI_API_KEY")
        if not self.openai_api_key:
            print("❌ OPENAI_API_KEY is missing in .env")
            sys.exit(1)
        
        # Initialize STREAMING STT and TTS clients
        self.xtts_client = XTTSStreamingClient(server_url, voice_clone_path)
        self.stt_client = STTStreamingClient(server_url)

        if OpenAI is None:
            logger.error("❌ openai package missing. Install: pip install openai")
            sys.exit(1)

        self.oai_client = OpenAI(api_key=self.openai_api_key)
        self.oai_model = os.getenv("OPENAI_MODEL", "gpt-4o-mini")

        # Initialize OpenAI client (LLM runs inside this file)

        # MongoDB FAQ (Hindi) - answer from FAQs_hindi first, then fallback to RAG/GPT
        self.mongo_uri = os.getenv("MONGO_URI")
        self.mongo_db = os.getenv("MONGO_DB", "pro_analysis")
        self.mongo_faq_collection = os.getenv("MONGO_FAQ_COLLECTION", "FAQs_hindi")

        self.faq_db = None
        if self.mongo_uri:
            try:
                self.faq_db = MongoFAQHindi(self.mongo_uri, self.mongo_db, self.mongo_faq_collection)
                count = self.faq_db.refresh_cache()
                logger.info(f"Mongo FAQ loaded: {count} items from {self.mongo_db}.{self.mongo_faq_collection}")
            except Exception as e:
                logger.error(f"Mongo FAQ init failed: {e}")
                self.faq_db = None
        else:
            logger.warning("MONGO_URI not set; Mongo FAQ disabled")
        
        # Audio settings
        self.samplerate = 16000
        self.input_channels = 1
        self.output_channels = 1
        
        # Streaming setup
        self.is_recording = False
        self.is_playing = False
        self.input_stream = None
        self.output_stream = None
        
        # VAD settings
        self.vad_threshold = 0.02  # Lower threshold for better speech detection
        self.silence_duration = 1.0  # 1 second of silence to stop
        self.chunk_duration_ms = 30  # ms
        self.chunk_samples = int(self.samplerate * self.chunk_duration_ms / 1000)
        
        # Audio buffers
        self.audio_buffer = []
        self.playback_queue = asyncio.Queue()
        self.audio_chunk_queue = asyncio.Queue()  # For chunks from TTS
        
        # Session tracking
        self.session_count = 0
        self.next_prompt_id = 1000
        
        # Conversation context
        self.first_interaction = True
        self.session_language = DEFAULT_LANGUAGE
        self.language_locked = False
        
        # Performance tracking
        self.performance_stats = {
            "stt_chunks_processed": 0,
            "tts_chunks_played": 0,
            "vad_triggers": 0,
            "interruptions": 0,
            "speech_detected": False
        }
        
        # For VAD
        self.last_speech_time = 0
        self.silence_start_time = None

        # Noise reduction settings
        self.noise_reduction_enabled = True
        self.noise_profile = None
        self.noise_profile_duration = 1.0  # Seconds to record noise profile
        
        # Interruptible playback settings
        self.interruption_enabled = True
        self.interruption_threshold = 0.05  # Volume threshold for interruption
        self.is_being_interrupted = False
        
        # Improved VAD settings
        self.vad_threshold = 0.03  # Adjusted for better speech detection
        self.min_speech_duration = 0.3  # Minimum speech duration to consider
        self.silence_duration = 1.0  # 1 second of silence to stop
        
        # Audio buffers for noise cancellation
        self.noise_samples = []
        self.noise_profile_ready = False
        
    def get_next_prompt_id(self) -> int:
        """Get next prompt ID for session"""
        current_id = self.next_prompt_id
        self.next_prompt_id += 1
        return current_id
    
    def display_latency_summary(
        self,
        prompt_id: int,
        stt_time: float,
        rag_time: float,
        gpt_time: float,
        tts_time: float,
        total_time: float,
        function_called: str = None,
        intent: str = None,
        streaming_metrics: Dict[str, Any] = None
    ):
        """Display enhanced latency summary with streaming metrics"""
        brain_time = rag_time + gpt_time
        calculated_total = stt_time + brain_time + tts_time
        
        summary = f"[Latency Summary Prompt ID: {prompt_id}]"
        print(summary)
        logger.info(summary)
        
        print(f"  Language: {self.session_language} | Intent: {intent or 'unknown'} | Function: {function_called or 'none'}")
        print(f"  STT: {stt_time:.3f}s | Brain: {brain_time:.3f}s (RAG={rag_time:.3f}s, GPT={gpt_time:.3f}s) | TTS: {tts_time:.3f}s")
        print(f"  TOTAL: {calculated_total:.3f}s")
        
        if streaming_metrics:
            print(f"  STREAMING METRICS:")
            print(f"    STT chunks: {streaming_metrics.get('stt_chunks', 0)}")
            print(f"    TTS chunks: {streaming_metrics.get('tts_chunks', 0)}")
            print(f"    VAD triggers: {streaming_metrics.get('vad_triggers', 0)}")
            print(f"    First word latency: {streaming_metrics.get('first_word_latency', 0):.3f}s")
    
    async def record_noise_profile(self):
        """Record noise profile for noise cancellation"""
        if not self.noise_reduction_enabled:
            return
            
        print("🎵 Recording noise profile (1 second)... Please stay silent.")
        
        samplerate = self.samplerate
        duration = self.noise_profile_duration
        
        # Record noise
        noise_recording = sd.rec(int(duration * samplerate), 
                                samplerate=samplerate, 
                                channels=1, 
                                dtype='float32')
        sd.wait()
        
        # Store noise profile
        self.noise_profile = noise_recording.flatten()
        self.noise_profile_ready = True
        
        print("✅ Noise profile recorded")
    
    def apply_noise_reduction(self, audio_data: np.ndarray) -> np.ndarray:
        """Apply noise reduction to audio data"""
        if not self.noise_reduction_enabled or not self.noise_profile_ready:
            return audio_data
            
        try:
            # Apply noise reduction
            reduced_noise = nr.reduce_noise(
                y=audio_data,
                sr=self.samplerate,
                y_noise=self.noise_profile,
                prop_decrease=0.95,  # Reduce 95% of noise
                stationary=True
            )
            return reduced_noise
        except Exception as e:
            logger.warning(f"Noise reduction failed: {e}")
            return audio_data
        
    def _audio_input_callback(self, indata, frames, time_info, status):
        """Streaming audio input callback with VAD"""
        if status:
            logger.warning(f"Audio input status: {status}")
            
        if not self.is_recording or indata is None:
            return
            
        try:
            # Process audio chunk
            chunk = indata.copy()
            
            # Calculate volume for VAD
            volume = np.mean(np.abs(chunk))
            
            # Simple VAD
            is_speech = volume > self.vad_threshold
            
            # Log VAD status occasionally
            if np.random.random() < 0.01:  # Log 1% of chunks
                logger.debug(f"VAD: volume={volume:.6f}, speech={is_speech}")
            
            # Store chunk with metadata
            chunk_data = {
                'timestamp': time.time(),
                'audio': chunk.copy(),
                'is_speech': is_speech,
                'volume': volume
            }
            
            # Add to buffer
            self.audio_buffer.append(chunk_data)
            
            # Update VAD state
            if is_speech:
                self.performance_stats["vad_triggers"] += 1
                self.performance_stats["speech_detected"] = True
                self.last_speech_time = time.time()
                self.silence_start_time = None
            else:
                if self.silence_start_time is None:
                    self.silence_start_time = time.time()
                
        except Exception as e:
            logger.error(f"Error in audio input callback: {e}")
    
    def _audio_output_callback(self, outdata, frames, time_info, status):
        """Streaming audio output callback - SIMPLIFIED to prevent repetition"""
        if status:
            logger.debug(f"Audio output status: {status}")
            
        try:
            # Always initialize with silence
            outdata.fill(0)
            
            if not self.is_playing:
                return
                
            # Get audio chunk from queue
            try:
                audio_data = self.playback_queue.get_nowait()
                
                if audio_data is not None:
                    # Ensure correct shape
                    if len(audio_data.shape) == 1:
                        audio_data = audio_data.reshape(-1, 1)
                    
                    # Copy only available data
                    available = min(frames, len(audio_data))
                    outdata[:available] = audio_data[:available]
                    
                    # DO NOT put leftover back - this causes repetition
                    # If there's leftover, it will be in the next chunk
                    
                    self.performance_stats["tts_chunks_played"] += 1
                    
            except asyncio.QueueEmpty:
                # No data available, silence will play (outdata is already zeros)
                pass
                
        except Exception as e:
            logger.error(f"Error in audio output callback: {e}")
            outdata.fill(0)

    async def record_with_vad(self) -> Tuple[Optional[List[Dict]], float]:
        """Record audio using VAD for real-time speech detection"""
        if not AUDIO_CAPTURE_AVAILABLE:
            raise ImportError("Install: pip install sounddevice soundfile numpy")
        
        logger.info("Starting VAD recording")
        
        # Reset state
        self.is_recording = True
        self.audio_buffer = []
        self.last_speech_time = time.time()
        self.silence_start_time = None
        self.performance_stats["speech_detected"] = False
        
        recording_start = time.time()
        
        try:
            # Setup audio input stream
            self.input_stream = sd.InputStream(
                samplerate=self.samplerate,
                channels=self.input_channels,
                dtype='float32',
                blocksize=self.chunk_samples,
                callback=self._audio_input_callback
            )
            
            self.input_stream.start()
            logger.info("Audio input stream started")
            
            # Record until silence threshold
            timeout = 10.0  # Maximum recording time
            
            while self.is_recording:
                current_time = time.time()
                recording_duration = current_time - recording_start
                
                # Check for silence after speech
                if self.performance_stats["speech_detected"] and self.silence_start_time:
                    silence_duration = current_time - self.silence_start_time
                    if silence_duration > self.silence_duration:
                        logger.info(f"Silence detected for {silence_duration:.1f}s after speech, stopping recording")
                        break
                
                # Check timeout
                if recording_duration > timeout:
                    logger.warning("Recording timeout reached")
                    break
                
                # Small sleep to prevent CPU hogging
                await asyncio.sleep(0.01)
            
            # Stop recording
            self.is_recording = False
            
            if self.input_stream:
                self.input_stream.stop()
                self.input_stream.close()
                self.input_stream = None
            
            recording_time = time.time() - recording_start
            
            # Analyze recording
            speech_chunks = [chunk for chunk in self.audio_buffer if chunk.get('is_speech', False)]
            total_chunks = len(self.audio_buffer)
            speech_ratio = len(speech_chunks) / max(total_chunks, 1)
            
            logger.info(f"Recording complete: {total_chunks} chunks, {speech_ratio:.1%} speech, {recording_time:.2f}s")
            
            if not self.performance_stats["speech_detected"]:
                logger.warning("No speech detected in recording")
                return None, recording_time
            
            return self.audio_buffer, recording_time
            
        except Exception as e:
            logger.error(f"Error in VAD recording: {e}")
            self.is_recording = False
            if self.input_stream:
                try:
                    self.input_stream.stop()
                    self.input_stream.close()
                except:
                    pass
                self.input_stream = None
            return None, time.time() - recording_start
    
    async def process_audio_buffer(self, audio_buffer: List[Dict]) -> Optional[bytes]:
        """Convert audio buffer to WAV bytes"""
        try:
            if not audio_buffer:
                return None
            
            # Extract and concatenate audio chunks
            audio_arrays = []
            for chunk_data in audio_buffer:
                audio_chunk = chunk_data['audio']
                if isinstance(audio_chunk, np.ndarray):
                    audio_arrays.append(audio_chunk.flatten())
            
            if not audio_arrays:
                return None
            
            # Concatenate all audio
            audio_array = np.concatenate(audio_arrays)
            
            # Convert to WAV format
            wav_io = io.BytesIO()
            sf.write(wav_io, audio_array, self.samplerate, format='WAV', subtype='PCM_16')
            
            logger.info(f"Audio processed: {len(audio_array)} samples, {len(wav_io.getvalue())} bytes")
            return wav_io.getvalue()
            
        except Exception as e:
            logger.error(f"Error processing audio buffer: {e}")
            return None
    
    # ---------------- Language Detection ----------------
    
    @staticmethod
    def _is_devanagari(ch: str) -> bool:
        """Basic Devanagari block check."""
        o = ord(ch)
        return 0x0900 <= o <= 0x097F
    
    @staticmethod
    def _is_gujarati(ch: str) -> bool:
        """Basic Gujarati block check."""
        o = ord(ch)
        return 0x0A80 <= o <= 0x0AFF

    def detect_and_lock_language_from_text(self, text: str) -> None:
        """Lock language after first successful transcription (Hindi/English only)."""
        if self.language_locked:
            return

        if not text or not text.strip():
            return

        # Count scripts
        dev_count = sum(1 for c in text if self._is_devanagari(c))
        eng_count = sum(1 for c in text if c.isalpha() and c.isascii())

        # Pick language
        self.session_language = "hi" if dev_count >= eng_count else "en"

        if self.session_language not in ALLOWED_LANGUAGES:
            self.session_language = DEFAULT_LANGUAGE

        self.language_locked = True
        logger.info(f"Language locked to: {self.session_language}")
        print(f"🔒 Language locked to: {self.session_language}")

    # ----------------------------------------------------------
    
    async def stt_streaming_transcribe(self, audio_chunks: List[Dict], prompt_id: int) -> Tuple[Optional[str], float]:
        """Streaming STT: Convert speech to text using streaming"""
        logger.info(f"Starting streaming STT with {len(audio_chunks)} chunks")
        
        # Combine all audio for transcription
        audio_bytes = await self.process_audio_buffer(audio_chunks)
        if not audio_bytes:
            return None, 0.0
        
        # Convert to base64
        audio_b64 = base64.b64encode(audio_bytes).decode('ascii')
        
        # Use streaming STT
        return await self.stt_client.streaming_transcribe(audio_b64, prompt_id, self.session_language)
    
    async def tts_streaming_speak(self, text: str, prompt_id: int) -> Tuple[Optional[asyncio.Queue], float]:
        """Streaming TTS: Convert text to speech with chunked streaming"""
        # Clean text
        cleaned_text = clean_text_for_tts(text, self.session_language)
        
        if not cleaned_text:
            logger.warning("Text cleaning resulted in empty string")
            return None, 0.0
        
        # Print cleaned text for debugging
        print(f"TTS input: {cleaned_text[:50]}...")
        logger.info(f"TTS input length: {len(cleaned_text)} chars")
        
        # Get streaming TTS
        return await self.xtts_client.streaming_tts(cleaned_text, prompt_id, self.session_language)

    async def play_streaming_audio(self, audio_queue: asyncio.Queue) -> bool:
        """Simple audio playback - just play the full audio at once"""
        try:
            logger.info("Starting audio playback")
            
            # Collect all audio chunks
            audio_chunks = []
            print("🔄 Loading audio...")
            
            while True:
                try:
                    chunk = await asyncio.wait_for(audio_queue.get(), timeout=5.0)
                    
                    if chunk is None:  # End signal
                        break
                        
                    audio_chunks.append(chunk)
                    
                except asyncio.TimeoutError:
                    logger.warning("Timeout waiting for audio")
                    break
                except Exception as e:
                    logger.error(f"Error collecting audio: {e}")
                    break
            
            if not audio_chunks:
                logger.error("No audio received")
                print("❌ No audio received from TTS")
                return False
            
            # Combine all chunks
            full_audio = np.concatenate(audio_chunks)
            
            # Ensure it's 1D
            if len(full_audio.shape) > 1:
                full_audio = full_audio.flatten()
            
            # Calculate duration
            duration = len(full_audio) / self.samplerate
            print(f"🎵 Playing {duration:.1f}s of audio...")
            
            # Play audio directly (simplest approach)
            try:
                sd.play(full_audio, self.samplerate)
                
                # Show playback progress
                start_time = time.time()
                while time.time() - start_time < duration + 0.5:  # Add 0.5s buffer
                    elapsed = time.time() - start_time
                    progress = min(elapsed / duration, 1.0)
                    print(f"   [{'.' * int(progress * 20):20}] {elapsed:.1f}/{duration:.1f}s", end='\r')
                    await asyncio.sleep(0.1)
                
                print()  # New line after progress bar
                sd.wait()  # Ensure playback completes
                
                logger.info(f"✅ Audio playback completed: {duration:.2f}s")
                print(f"✅ Audio playback completed")
                return True
                
            except Exception as e:
                logger.error(f"Audio playback error: {e}")
                print(f"❌ Playback error: {e}")
                return False
            
        except Exception as e:
            logger.error(f"Audio playback error: {e}")
            print(f"❌ Audio error: {e}")
            return False
            
    async def streaming_tts(self, text: str, prompt_id: int, language: str = "hi") -> Tuple[Optional[asyncio.Queue], float]:
        """Streaming TTS with better error handling and retry logic"""
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
        
        # Try different configurations
        configurations = [
            {"use_voice_cloning": True if model_id == "xtts" and self.voice_reference_loaded else False},
            {"use_voice_cloning": False},  # Fallback: no voice cloning
        ]
        
        for config_idx, config in enumerate(configurations):
            use_voice_cloning = config["use_voice_cloning"]
            
            for attempt in range(2):  # 2 attempts per configuration
                try:
                    if not await self.ensure_connection():
                        logger.error("❌ TTS connection failed")
                        continue

                    # Prepare request
                    request = {
                        "model_id": model_id,
                        "prompt": text_clean,
                        "prompt_id": prompt_id,
                        "language": language,
                        "task": "tts",
                        "stream": False,
                    }

                    # Add voice cloning if enabled
                    if use_voice_cloning and model_id == "xtts" and self.voice_reference_loaded:
                        request["voice_cloning"] = True
                        request["voice_reference"] = self.voice_reference_b64
                        logger.info(f"🎤 Voice cloning enabled (attempt {attempt + 1})")
                    else:
                        if config_idx > 0:  # Only log if we're retrying without voice cloning
                            logger.info("🎤 Trying without voice cloning")

                    logger.debug(f"Sending TTS request (config {config_idx}, attempt {attempt})")
                    
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
                        continue

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
                        continue

                    # Parse response
                    try:
                        response_data = json.loads(response)
                    except json.JSONDecodeError as e:
                        logger.error(f"Failed to parse JSON response: {e}")
                        if attempt < 1:
                            continue
                        continue

                    # Check for errors
                    if "error" in response_data:
                        err = response_data.get("error", "").strip()
                        if err:
                            logger.error(f"TTS error: {err}")
                            print(f"❌ TTS Error: {err}")
                        else:
                            logger.error("TTS returned empty error")
                            print("⚠️ TTS returned empty error")
                        
                        # If we get an error with voice cloning, try next config
                        if use_voice_cloning:
                            logger.info("Voice cloning failed, will try without it")
                            break  # Break to next configuration
                        
                        if attempt < 1:
                            continue
                        continue

                    # Check for audio
                    if "audio_b64" not in response_data:
                        logger.error(f"No audio in response: {list(response_data.keys())}")
                        if attempt < 1:
                            continue
                        continue

                    # Process audio
                    audio_b64 = response_data["audio_b64"]
                    
                    try:
                        # Decode audio
                        audio_bytes = base64.b64decode(audio_b64)
                        
                        # Check audio size
                        if len(audio_bytes) < 100:
                            logger.error(f"Audio too small: {len(audio_bytes)} bytes")
                            if attempt < 1:
                                continue
                            continue
                        
                        # Get sample rate
                        sr = response_data.get("sr", 24000)
                        logger.info(f"Audio: {len(audio_bytes)/1024:.1f}KB, {sr}Hz")
                        
                        # Convert to float32
                        audio_array = np.frombuffer(audio_bytes, dtype=np.int16).astype(np.float32) / 32768.0
                        
                        # Resample if needed
                        target_sr = 16000
                        if sr != target_sr:
                            logger.info(f"Resampling {sr}Hz → {target_sr}Hz")
                            try:
                                # Simple linear resampling
                                ratio = target_sr / sr
                                new_length = int(len(audio_array) * ratio)
                                
                                # Create indices for resampling
                                indices = np.linspace(0, len(audio_array) - 1, new_length)
                                indices = indices.astype(int)
                                indices = np.clip(indices, 0, len(audio_array) - 1)
                                
                                audio_array = audio_array[indices]
                                logger.info(f"Resampled to {len(audio_array)} samples")
                            except Exception as e:
                                logger.error(f"Resampling failed: {e}")
                                # Continue with original audio, might play fast/slow
                        
                        # Add to queue
                        await audio_queue.put(audio_array)
                        await audio_queue.put(None)  # End signal
                        
                        # Update metrics
                        elapsed_time = time.time() - start_time
                        self.metrics["first_chunk_latency"] = elapsed_time
                        self.metrics["chunks_generated"] += 1
                        self.metrics["total_chars"] += len(text_clean)
                        self.metrics["synthesis_latency"].append(elapsed_time)
                        
                        logger.info(f"✅ TTS successful: {elapsed_time:.3f}s")
                        
                        return audio_queue, elapsed_time
                        
                    except Exception as e:
                        logger.error(f"Error processing audio: {e}")
                        if attempt < 1:
                            continue
                        continue

                except websockets.exceptions.ConnectionClosed as e:
                    logger.error(f"⚠️ TTS connection closed: {e}")
                    self.connected = False
                    self.ws = None
                    self.metrics["errors"] += 1
                    if attempt < 1:
                        await asyncio.sleep(2.0)
                        continue
                    continue

                except Exception as e:
                    logger.error(f"⚠️ TTS error: {e}")
                    self.metrics["errors"] += 1
                    if attempt < 1:
                        await asyncio.sleep(2.0)
                        continue
                    continue
        
        # If all configurations failed
        elapsed_time = time.time() - start_time
        logger.error(f"❌ All TTS attempts failed: {elapsed_time:.3f}s")
        return None, elapsed_time

    async def get_gpt_response(self, user_input: str) -> Tuple[str, str, float]:
        """Get response from OpenAI (model: gpt-4o-mini)."""
        start = time.time()
        try:
            # Keep message short and controlled
            resp = self.oai_client.chat.completions.create(
                model=self.oai_model,
                messages=[
                    {"role": "system", "content": SYSTEM_PROMPT},
                    {"role": "user", "content": user_input},
                ],
                temperature=0.2,
                max_tokens=200,
            )
            out = (resp.choices[0].message.content or '').strip()
            elapsed = time.time() - start
            if not out:
                # Fallback
                out = "माफ कीजिए, मैं समझ नहीं पाई। कृपया फिर से पूछिए।" if self.session_language == "hi" else "Sorry, I couldn't understand. Please ask again."
            logger.info(f"GPT response length: {len(out)} chars")
            return out, None, elapsed
        except Exception as e:
            logger.error(f"GPT Error: {e}")
            elapsed = time.time() - start
            if self.session_language == "hi":
                return "क्षमा करें, तकनीकी समस्या है। कृपया फिर से प्रयास करें।", "error", elapsed
            else:
                return "Sorry, there is a technical issue. Please try again.", "error", elapsed

    async def single_conversation_session(self):
        """Complete session with streaming"""
        self.session_count += 1
        prompt_id = self.get_next_prompt_id()
        session_start_time = time.time()
        
        
        logger.info(f"Starting session {self.session_count} (Prompt ID: {prompt_id})")
        
        # Step 1: Record audio with VAD
        print("\n🎤 Listening... (Speak now, stops after 1s silence)")
        logger.info("Starting VAD recording")
        audio_chunks, recording_time = await self.record_with_vad()
        
        if not audio_chunks or not self.performance_stats["speech_detected"]:
            logger.warning("No speech detected in recording")
            print("No speech detected")
            
            # Give a quick beep to indicate it's listening
            if audio_chunks:
                print("I heard something but couldn't detect speech. Try speaking louder.")
            return
        
        logger.info(f"Recording captured: {len(audio_chunks)} chunks, {recording_time:.2f}s")
        
        # Step 2: Streaming STT
        stt_start_time = time.time()
        transcription, stt_time = await self.stt_streaming_transcribe(audio_chunks, prompt_id)
        stt_time = time.time() - stt_start_time if stt_time == 0.0 else stt_time
        
        if transcription:
            self.detect_and_lock_language_from_text(transcription)
            logger.info(f"Transcription length: {len(transcription)} chars")
            print(f"👤 User: {transcription}")
        else:
            logger.warning("Transcription failed or empty")
            print("⚠️ Couldn't understand that. Please try again.")
            
            # Fallback response
            if self.session_language == "hi":
                reply = "माफ कीजिए मैं समझ नहीं पाई कृपया फिर से बोलिए"
            else:
                reply = "Sorry I couldn't understand that. Please try again."
            
            # Streaming TTS for fallback
            tts_start_time = time.time()
            audio_queue, tts_time = await self.tts_streaming_speak(reply, prompt_id)
            tts_time = time.time() - tts_start_time if tts_time == 0.0 else tts_time
            
            if audio_queue:
                await self.play_streaming_audio(audio_queue)
            
            total_time = time.time() - session_start_time
            
            # Display summary
            self.display_latency_summary(
                prompt_id=prompt_id,
                stt_time=stt_time,
                rag_time=0.0,
                gpt_time=0.0,
                tts_time=tts_time,
                total_time=total_time,
                function_called="stt_error",
                intent="error",
                streaming_metrics={
                    "stt_chunks": len(audio_chunks),
                    "tts_chunks": self.performance_stats.get("tts_chunks_played", 0),
                    "vad_triggers": self.performance_stats.get("vad_triggers", 0),
                    "first_word_latency": 0.0
                }
            )
            return
        

        # Step 2.5: MongoDB FAQ match (Hindi) BEFORE RAG/GPT
        # Fast + deterministic for FAQs_hindi collection
        rag_time = 0.0
        gpt_time = 0.0
        function_called = None
        detected_intent = "unknown"
        reply = None

        if self.session_language == "hi" and self.faq_db:
            try:
                ans, sc, matched_q = self.faq_db.best_answer(transcription)
                if ans:
                    reply = ans
                    function_called = "mongo_faq_match"
                    detected_intent = "faq"
                    logger.info(f"FAQ matched (score={sc:.2f}): {matched_q}")
                else:
                    logger.info(f"No FAQ match (best score={sc:.2f})")
            except Exception as e:
                logger.error(f"FAQ lookup failed: {e}")
        # If FAQ didn't answer, fallback to GPT
        if reply is None:
            reply, function_called, gpt_time = await self.get_gpt_response(transcription)
            detected_intent = detected_intent if detected_intent != 'unknown' else 'gpt'
        logger.info(f"Response length: {len(reply)} chars")
        print(f"🤖 Assistant: {reply}")
        
        # Step 5: Streaming TTS
        tts_start_time = time.time()
        audio_queue, tts_time = await self.tts_streaming_speak(reply, prompt_id)
        tts_time = time.time() - tts_start_time if tts_time == 0.0 else tts_time
        
        if audio_queue:
            await self.play_streaming_audio(audio_queue)
        else:
            logger.error("Could not generate speech for response")
            print("⚠️ Could not generate speech for response")
        
        # Calculate total time
        total_time = time.time() - session_start_time
        
        # Display enhanced latency summary
        self.display_latency_summary(
            prompt_id=prompt_id,
            stt_time=stt_time,
            rag_time=rag_time,
            gpt_time=gpt_time,
            tts_time=tts_time,
            total_time=total_time,
            function_called=function_called,
            intent=detected_intent,
            streaming_metrics={
                "stt_chunks": len(audio_chunks),
                "tts_chunks": self.performance_stats.get("tts_chunks_played", 0),
                "vad_triggers": self.performance_stats.get("vad_triggers", 0),
                "first_word_latency": 0.0
            }
        )
        
        # Reset performance stats for next session
        self.performance_stats = {
            "stt_chunks_processed": 0,
            "tts_chunks_played": 0,
            "vad_triggers": 0,
            "interruptions": 0,
            "speech_detected": False
        }
    
    async def run_voice_assistant(self):
        """Main voice assistant loop"""
        print("\n" + "="*60)
        print("🍽️  RESTAURANT VOICE ASSISTANT v8.0 (STREAMING)")
        print("="*60)
        print("✅ Features: Streaming STT/TTS | VAD | Real-time Processing")
        print("✅ Optimizations: Zero Disk I/O | Chunked Processing | Early Playback")
        print("📊 Logging: Detailed logs in logs/voice_assistant.log")
        print("🛑 Press Ctrl+C to exit")
        print("="*60)
        
        try:
            # Initial connections
            print("🔗 Establishing all persistent streaming connections...")
            logger.info("Establishing streaming connections")
            
            xtts_connected = await self.xtts_client.connect()
            stt_connected = await self.stt_client.connect()
            
            if xtts_connected:
                print("✅ Streaming XTTS connection established")
                logger.info("Streaming XTTS connection established")
            else:
                print("⚠️ Failed to establish XTTS connection")
                logger.error("Failed to establish XTTS connection")
            
            if stt_connected:
                print("✅ Streaming STT connection established")
                logger.info("Streaming STT connection established")
            else:
                print("⚠️ Failed to establish STT connection")
                logger.error("Failed to establish STT connection")
            
            # Initial greeting
            if self.session_language == "hi":
                initial_greeting = "नमस्ते, आपका स्वागत है। मैं आपकी क्या सहायता कर सकता हूं?"
            else:
                initial_greeting = "Hello, welcome. How can I help you today?"
            
            print(f"🤖 {initial_greeting}")
            logger.info(f"Initial greeting length: {len(initial_greeting)} chars")
            
            # Play initial greeting with streaming TTS
            prompt_id = self.get_next_prompt_id()
            tts_start = time.time()
            audio_queue, tts_time = await self.tts_streaming_speak(initial_greeting, prompt_id)
            tts_time = time.time() - tts_start if tts_time == 0.0 else tts_time
            
            if audio_queue:
                await self.play_streaming_audio(audio_queue)
                self.display_latency_summary(
                    prompt_id=prompt_id,
                    stt_time=0.0,
                    rag_time=0.0,
                    gpt_time=0.0,
                    tts_time=tts_time,
                    total_time=tts_time,
                    function_called="greeting",
                    intent="greeting",
                    streaming_metrics={
                        "tts_chunks": self.performance_stats.get("tts_chunks_played", 0),
                        "first_word_latency": 0.0
                    }
                )
            else:
                print("⚠️ Could not speak initial greeting")
            
            # Main conversation loop
            while True:
                await self.single_conversation_session()
                
        except KeyboardInterrupt:
            print("\n👋 Restaurant assistant stopped")
            logger.info("Assistant stopped by user")
                
        except Exception as e:
            print(f"\n❌ Assistant error: {e}")
            logger.exception("Fatal error in voice assistant")
        finally:
            await self.cleanup()
    
    async def cleanup(self):
        """Cleanup resources"""
        self.is_recording = False
        self.is_playing = False
        
        # Stop audio streams
        if self.input_stream:
            try:
                self.input_stream.stop()
                self.input_stream.close()
            except:
                pass
            self.input_stream = None
            
        if self.output_stream:
            try:
                self.output_stream.stop()
                self.output_stream.close()
            except:
                pass
            self.output_stream = None
        
        # Close WebSocket connections
        if hasattr(self, 'xtts_client'):
            await self.xtts_client.close()
        if hasattr(self, 'stt_client'):
            await self.stt_client.close()
        
        print("✅ All streaming connections closed")
        logger.info("All connections closed")

async def main():
    """Main function"""
    server_url = os.getenv("SERVER_URL")
    if not server_url:
        print("❌ SERVER_URL environment variable is required")
        print("Create a .env file with: SERVER_URL=ws://your-server-url")
        return
    
    voice_clone_path = os.getenv("VOICE_CLONE_PATH")
    if voice_clone_path:
        if Path(voice_clone_path).exists():
            print(f"🔊 Found voice reference file: {voice_clone_path}")
        else:
            print(f"⚠️  Voice reference file not found: {voice_clone_path}")
            voice_clone_path = None
    else:
        print("ℹ️  Voice cloning not enabled")

    if not os.getenv("OPENAI_API_KEY"):
        print("❌ OPENAI_API_KEY environment variable is required")
        return

    assistant = RestaurantVoiceAssistant(server_url, voice_clone_path)
    try:
        await assistant.run_voice_assistant()
    except KeyboardInterrupt:
        print("\n👋 Restaurant assistant stopped")
    except Exception as e:
        print(f"\n❌ Assistant error: {e}")
        traceback.print_exc()
    finally:
        await assistant.cleanup()

try:
    from aiortc import RTCPeerConnection, RTCSessionDescription, RTCIceCandidate
    from aiortc.contrib.media import MediaStreamTrack
    from av import AudioFrame
    AIORTC_AVAILABLE = True
except Exception:
    AIORTC_AVAILABLE = False

def _resample_linear(x: np.ndarray, src_sr: int, dst_sr: int) -> np.ndarray:
    """Very simple linear resampler for mono audio."""
    if x is None or len(x) == 0 or src_sr == dst_sr:
        return x
    ratio = float(dst_sr) / float(src_sr)
    n = int(len(x) * ratio)
    if n <= 0:
        return x
    idx = np.linspace(0, len(x) - 1, n).astype(np.float32)
    idx0 = np.floor(idx).astype(np.int64)
    idx1 = np.minimum(idx0 + 1, len(x) - 1)
    frac = idx - idx0
    return (x[idx0] * (1.0 - frac) + x[idx1] * frac).astype(np.float32)

class OutgoingAudioTrack(MediaStreamTrack):
    """
    WebRTC outgoing audio track: bot -> client.
    We push PCM float32 mono into a queue; aiortc pulls frames via recv().
    """
    kind = "audio"

    def __init__(self, target_sr: int = 48000):
        super().__init__()
        self.target_sr = target_sr
        self.q: asyncio.Queue = asyncio.Queue()
        self._silence = np.zeros(int(self.target_sr * 0.02), dtype=np.float32)  # 20ms silence

    async def push_pcm(self, pcm_float32: np.ndarray, pcm_sr: int):
        if pcm_float32 is None or len(pcm_float32) == 0:
            return
        pcm = pcm_float32.astype(np.float32).flatten()
        if pcm_sr != self.target_sr:
            pcm = _resample_linear(pcm, pcm_sr, self.target_sr)
        await self.q.put(pcm)

    async def recv(self) -> AudioFrame:
        # WebRTC typically likes 20ms frames.
        frame_samples = int(self.target_sr * 0.02)

        try:
            pcm = await asyncio.wait_for(self.q.get(), timeout=0.2)
        except asyncio.TimeoutError:
            pcm = self._silence

        if pcm is None or len(pcm) == 0:
            pcm = self._silence

        if len(pcm) < frame_samples:
            # pad
            pcm = np.pad(pcm, (0, frame_samples - len(pcm)))

        chunk = pcm[:frame_samples]
        # keep remainder for next frame
        rem = pcm[frame_samples:]
        if len(rem) > 0:
            # put remainder back to the front by re-queuing (simple)
            await self.q.put(rem)

        # float32 [-1..1] -> int16
        int16 = (np.clip(chunk, -1, 1) * 32767).astype(np.int16)

        af = AudioFrame(format="s16", layout="mono", samples=len(int16))
        af.sample_rate = self.target_sr
        af.planes[0].update(int16.tobytes())
        return af

class WebRTCBotSession:
    """
    Per call_id session:
    - receives client audio track (WebRTC)
    - chunks + VAD -> STT -> GPT -> TTS
    - sends bot audio through OutgoingAudioTrack
    """
    def __init__(self, assistant: "RestaurantVoiceAssistant", call_id: str):
        self.assistant = assistant
        self.call_id = call_id
        self.pc = RTCPeerConnection()
        self.out_track = OutgoingAudioTrack(target_sr=int(os.getenv("BOT_WEBRTC_SR", "48000")))
        self.pc.addTrack(self.out_track)
        
        # Track connection state
        @self.pc.on("connectionstatechange")
        async def _on_connectionstatechange():
            state = self.pc.connectionState
            logger.info(f"[BOT SESSION] Connection state changed: {state} for call_id: {call_id}")
            print(f"[BOT SESSION] 🔗 Connection state: {state} for call: {call_id}")
            
            # Signal when connection is ready
            if state == "connected":
                logger.info(f"[BOT SESSION] ✅ Connection established! Setting ready event")
                print(f"[BOT SESSION] ✅ Connection established!")
                self._connection_ready.set()
            elif state in ["failed", "disconnected", "closed"]:
                logger.warning(f"[BOT SESSION] ⚠️ Connection {state}, clearing ready event")
                self._connection_ready.clear()
        
        # Check if already connected (might happen before event handler is set)
        if self.pc.connectionState == "connected":
            logger.info(f"[BOT SESSION] Already connected, setting ready event")
            self._connection_ready.set()

        # incoming audio handling
        self.in_sr_guess = 48000
        self.vad_threshold = float(getattr(self.assistant, "vad_threshold", 0.03))
        self.silence_duration = float(getattr(self.assistant, "silence_duration", 1.0))
        self.min_speech_duration = float(getattr(self.assistant, "min_speech_duration", 0.3))

        self._buffer = []
        self._speech_started_at = None
        self._last_speech_ts = None
        self._running = True
        self._connection_ready = asyncio.Event()  # Event to signal when connection is ready

        # When aiortc gathers ICE, send back using callback set by server
        self.on_ice_candidate = None  # async func(candidate_dict)

        @self.pc.on("icecandidate")
        async def _on_icecandidate(event):
            try:
                cand = event
                if cand is None:
                    return
                # aiortc event gives RTCIceCandidate sometimes as object
            except Exception:
                return

    async def close(self):
        self._running = False
        try:
            await self.pc.close()
        except Exception:
            pass

    def _frame_to_float32(self, frame: AudioFrame) -> Tuple[np.ndarray, int]:
        """
        Convert AudioFrame to float32 mono.
        """
        # frame.to_ndarray() returns shape (channels, samples) or (samples,)
        arr = frame.to_ndarray()
        sr = int(getattr(frame, "sample_rate", 48000) or 48000)
        if isinstance(arr, np.ndarray):
            if arr.ndim == 2:
                # (channels, samples) -> mono
                arr = arr[0]
            # Usually int16
            if arr.dtype == np.int16:
                f = arr.astype(np.float32) / 32768.0
            else:
                f = arr.astype(np.float32)
            return f.flatten(), sr
        return np.zeros(0, dtype=np.float32), sr

    def _is_speech(self, pcm: np.ndarray) -> bool:
        if pcm is None or len(pcm) == 0:
            return False
        vol = float(np.mean(np.abs(pcm)))
        return vol > self.vad_threshold

    async def _speak_text_into_call(self, text: str):
        """
        Uses your existing tts_streaming_speak() to get chunks,
        then pushes into out_track.
        """
        # Check connection state before speaking
        if self.pc.connectionState != "connected":
            logger.warning(f"[BOT SESSION] Connection not connected (state: {self.pc.connectionState}), waiting...")
            print(f"[BOT SESSION] ⏳ Connection not ready, waiting... (state: {self.pc.connectionState})")
            try:
                await asyncio.wait_for(self._connection_ready.wait(), timeout=5.0)
            except asyncio.TimeoutError:
                logger.error(f"[BOT SESSION] ❌ Connection not ready after timeout (state: {self.pc.connectionState})")
                print(f"[BOT SESSION] ❌ Connection timeout! Audio may not be heard.")
                # Continue anyway - might still work
        
        logger.info(f"[BOT SESSION] _speak_text_into_call: '{text[:50]}...' (connection: {self.pc.connectionState})")
        print(f"[BOT SESSION] 🗣️  Speaking: '{text[:50]}...' (connection: {self.pc.connectionState})")
        
        prompt_id = self.assistant.get_next_prompt_id()
        audio_queue, _ = await self.assistant.tts_streaming_speak(text, prompt_id)
        if not audio_queue:
            logger.warning(f"[BOT SESSION] No audio queue returned from TTS")
            print(f"[BOT SESSION] ⚠️  No audio from TTS")
            return

        # IMPORTANT: your XTTSStreamingClient likely outputs float32 at some SR.
        # If your client returns SR, use it. If not, assume env BOT_TTS_SR (default 24000).
        tts_sr = int(os.getenv("BOT_TTS_SR", "24000"))

        chunks = []
        while True:
            ch = await audio_queue.get()
            if ch is None:
                break
            if isinstance(ch, np.ndarray):
                chunks.append(ch.astype(np.float32).flatten())
        if not chunks:
            logger.warning(f"[BOT SESSION] No audio chunks from TTS")
            print(f"[BOT SESSION] ⚠️  No audio chunks")
            return
        pcm = np.concatenate(chunks)
        
        logger.info(f"[BOT SESSION] Pushing {len(pcm)} samples to out_track (SR: {tts_sr}, connection: {self.pc.connectionState})")
        print(f"[BOT SESSION] 📤 Pushing {len(pcm)} audio samples... (connection: {self.pc.connectionState})")
        await self.out_track.push_pcm(pcm, tts_sr)
        logger.info(f"[BOT SESSION] ✅ Audio pushed to out_track")
        print(f"[BOT SESSION] ✅ Audio pushed (connection: {self.pc.connectionState})")

    async def _transcribe_and_reply(self, audio_chunks: List[Dict]):
        """
        Reuse your STT + GPT + FAQ logic with minimal glue.
        """
        prompt_id = self.assistant.get_next_prompt_id()

        # Your stt_streaming_transcribe expects list of dicts with 'audio' ndarray float32
        transcription, _ = await self.assistant.stt_streaming_transcribe(audio_chunks, prompt_id)

        if not transcription:
            # quick fallback voice
            fallback = "माफ़ कीजिए, कृपया फिर से बोलिए।" if self.assistant.session_language == "hi" else "Sorry, please say that again."
            await self._speak_text_into_call(fallback)
            return

        # lock language once
        try:
            self.assistant.detect_and_lock_language_from_text(transcription)
        except Exception:
            pass

        # FAQ first (same as your logic)
        reply = None
        function_called = None

        if self.assistant.session_language == "hi" and self.assistant.faq_db:
            try:
                ans, sc, matched_q = self.assistant.faq_db.best_answer(transcription)
                if ans:
                    reply = ans
                    function_called = "mongo_faq_match"
            except Exception:
                pass

        if reply is None:
            reply, function_called, _ = await self.assistant.get_gpt_response(transcription)

        # Speak reply into the call
        await self._speak_text_into_call(reply)

    async def handle_incoming_track(self, track):
        """
        Reads audio frames continuously, does VAD segmentation,
        then calls STT->GPT->TTS.
        """
        logger.info(f"[BOT SESSION] Starting to handle incoming track for call_id: {self.call_id}")
        print(f"[BOT SESSION] 🎤 Starting audio handling for call: {self.call_id}")
        print(f"[BOT SESSION] 🔗 Connection state: {self.pc.connectionState}")
        
        # Wait for connection to be established before speaking
        # This is critical - audio won't be heard if connection isn't ready
        logger.info(f"[BOT SESSION] Waiting for connection to be established...")
        print(f"[BOT SESSION] ⏳ Waiting for connection to be established...")
        
        # Give connection a moment to start
        await asyncio.sleep(1.0)
        
        # Check current state
        current_state = self.pc.connectionState
        logger.info(f"[BOT SESSION] Current connection state: {current_state}")
        print(f"[BOT SESSION] Current connection state: {current_state}")
        
        # If already connected, proceed immediately
        if current_state == "connected":
            logger.info(f"[BOT SESSION] ✅ Already connected!")
            print(f"[BOT SESSION] ✅ Already connected!")
            self._connection_ready.set()
        else:
            # Wait for connection to be established
            try:
                # Wait up to 8 seconds for connection to be ready
                await asyncio.wait_for(self._connection_ready.wait(), timeout=8.0)
                logger.info(f"[BOT SESSION] ✅ Connection is ready! (state: {self.pc.connectionState})")
                print(f"[BOT SESSION] ✅ Connection is ready! (state: {self.pc.connectionState})")
            except asyncio.TimeoutError:
                current_state = self.pc.connectionState
                logger.warning(f"[BOT SESSION] ⚠️ Connection not ready after 8s (state: {current_state})")
                print(f"[BOT SESSION] ⚠️ Connection timeout (state: {current_state})")
                
                # If connecting or new, still try (ICE might be working)
                if current_state in ["connecting", "new"]:
                    logger.info(f"[BOT SESSION] ⚠️ Proceeding anyway - connection might still work (state: {current_state})")
                    print(f"[BOT SESSION] ⚠️ Proceeding anyway - ICE might be working")
                else:
                    logger.error(f"[BOT SESSION] ❌ Connection failed or closed (state: {current_state})")
                    print(f"[BOT SESSION] ❌ Connection failed! (state: {current_state})")
        
        # First greeting after transfer (your requested line)
        # You can customize per your flow:
        greet = os.getenv("BOT_TRANSFER_GREETING", "Master is busy right now. I will help you. Please tell me your issue.")
        # If you want Hindi greeting by default:
        if self.assistant.session_language == "hi":
            greet = os.getenv("BOT_TRANSFER_GREETING_HI", "मास्टर अभी व्यस्त हैं। मैं आपकी मदद करूँगा। कृपया अपनी समस्या बताइए।")
        
        logger.info(f"[BOT SESSION] Speaking greeting (connection state: {self.pc.connectionState})")
        print(f"[BOT SESSION] 🔊 Speaking greeting... (connection: {self.pc.connectionState})")
        await self._speak_text_into_call(greet)
        logger.info(f"[BOT SESSION] Greeting sent")
        print(f"[BOT SESSION] ✅ Greeting sent")

        # Buffering logic
        chunk_target_sr = int(os.getenv("BOT_STT_SR", "16000"))
        now = time.time()
        self._last_speech_ts = now

        while self._running:
            frame = await track.recv()
            pcm, sr = self._frame_to_float32(frame)

            if sr != chunk_target_sr:
                pcm_16k = _resample_linear(pcm, sr, chunk_target_sr)
            else:
                pcm_16k = pcm

            speech = self._is_speech(pcm_16k)
            ts = time.time()

            # store chunk dict like your audio_buffer elements
            self._buffer.append({
                "timestamp": ts,
                "audio": pcm_16k.reshape(-1, 1),  # your code uses ndarray
                "is_speech": speech,
                "volume": float(np.mean(np.abs(pcm_16k))) if len(pcm_16k) else 0.0
            })

            if speech:
                if self._speech_started_at is None:
                    self._speech_started_at = ts
                self._last_speech_ts = ts
            else:
                # if speech happened and silence long enough -> finalize utterance
                if self._speech_started_at is not None:
                    silence = ts - (self._last_speech_ts or ts)
                    speech_dur = (self._last_speech_ts or ts) - self._speech_started_at

                    if silence >= self.silence_duration and speech_dur >= self.min_speech_duration:
                        # finalize and reset
                        audio_chunks = self._buffer
                        self._buffer = []
                        self._speech_started_at = None
                        self._last_speech_ts = ts

                        try:
                            await self._transcribe_and_reply(audio_chunks)
                        except Exception as e:
                            logger.error(f"[BOT CALL ERROR] {e}")
                            # speak small error
                            err = "तकनीकी समस्या है, कृपया फिर से बोलिए।" if self.assistant.session_language == "hi" else "Technical issue. Please say again."
                            await self._speak_text_into_call(err)

async def webrtc_bot_signaling_server():
    """
    WebSocket signaling server for backend <-> bot.
    Backend sends:
      {"type":"offer","call_id":"...","sdp":"..."}
      {"type":"ice","call_id":"...","candidate":{...}}
      {"type":"end","call_id":"..."}
    Bot replies:
      {"type":"answer","call_id":"...","sdp":"..."}
      {"type":"ice","call_id":"...","candidate":{...}}   (optional if you implement trickle)
      {"type":"ended","call_id":"..."}
    """
    if not AIORTC_AVAILABLE:
        raise RuntimeError("aiortc/av not installed. Run: pip install aiortc av")

    host = os.getenv("BOT_SIGNAL_HOST", "0.0.0.0")
    port = int(os.getenv("BOT_SIGNAL_PORT", "8765"))

    server_url = os.getenv("SERVER_URL")
    if not server_url:
        raise RuntimeError("SERVER_URL is required in .env for STT/TTS websocket clients")

    voice_clone_path = os.getenv("VOICE_CLONE_PATH")
    assistant = RestaurantVoiceAssistant(server_url, voice_clone_path)

    # Establish persistent STT/TTS connections (reuse your logic)
    await assistant.xtts_client.connect()
    await assistant.stt_client.connect()

    sessions: Dict[str, WebRTCBotSession] = {}

    async def handler(ws):
        """Handle WebSocket connection from Flask server"""
        logger.info(f"[BOT SERVER] New connection from {ws.remote_address}")
        print(f"[BOT SERVER] 🔌 New connection from {ws.remote_address}")
        
        try:
            # Handle messages - Flask server sends one message and waits for response
            async for raw in ws:
                try:
                    msg = json.loads(raw)
                    logger.info(f"[BOT SERVER] Received message: {msg.get('type')}, call_id: {msg.get('call_id')}")
                    print(f"[BOT SERVER] 📥 Received: {msg.get('type')}, call_id: {msg.get('call_id')}")
                except json.JSONDecodeError as e:
                    logger.error(f"[BOT SERVER] Failed to parse JSON: {e}, raw: {raw[:100]}")
                    continue
                except Exception as e:
                    logger.error(f"[BOT SERVER] Error processing message: {e}")
                    continue

                mtype = (msg.get("type") or "").lower()
                call_id = (msg.get("call_id") or "").strip()
                if not call_id:
                    logger.warning(f"[BOT SERVER] Missing call_id in message: {msg}")
                    continue

                if mtype == "offer":
                    logger.info(f"[BOT SERVER] Processing offer for call_id: {call_id}")
                    print(f"[BOT SERVER] 🎯 Processing offer for call: {call_id}")
                    
                    # create session
                    sess = sessions.get(call_id)
                    if sess:
                        try:
                            logger.info(f"[BOT SERVER] Closing existing session for call_id: {call_id}")
                            await sess.close()
                        except Exception as e:
                            logger.error(f"[BOT SERVER] Error closing existing session: {e}")

                    try:
                        sess = WebRTCBotSession(assistant, call_id)
                        sessions[call_id] = sess
                        logger.info(f"[BOT SERVER] Created new WebRTC session for call_id: {call_id}")

                        # attach incoming track handler
                        @sess.pc.on("track")
                        def on_track(track):
                            if track.kind == "audio":
                                logger.info(f"[BOT SERVER] Audio track received for call_id: {call_id}")
                                print(f"[BOT SERVER] 🎤 Audio track received for call: {call_id}")
                                asyncio.create_task(sess.handle_incoming_track(track))

                        offer_sdp_raw = msg.get("sdp")
                        if not offer_sdp_raw:
                            logger.error(f"[BOT SERVER] Missing SDP in offer for call_id: {call_id}")
                            await ws.send(json.dumps({
                                "type": "error",
                                "call_id": call_id,
                                "error": "sdp_required"
                            }))
                            continue

                        # Handle SDP as either string or dict {"sdp": "...", "type": "offer"}
                        if isinstance(offer_sdp_raw, dict):
                            offer_sdp = offer_sdp_raw.get("sdp") or ""
                            logger.info(f"[BOT SERVER] Extracted SDP from dict, length: {len(offer_sdp)}")
                        else:
                            offer_sdp = str(offer_sdp_raw)
                            
                        if not offer_sdp:
                            logger.error(f"[BOT SERVER] Empty SDP string for call_id: {call_id}")
                            await ws.send(json.dumps({
                                "type": "error",
                                "call_id": call_id,
                                "error": "sdp_empty"
                            }))
                            continue

                        logger.info(f"[BOT SERVER] Setting remote description... (SDP length: {len(offer_sdp)})")
                        print(f"[BOT SERVER] 📝 SDP preview: {offer_sdp[:100]}...")
                        await sess.pc.setRemoteDescription(RTCSessionDescription(sdp=offer_sdp, type="offer"))
                        logger.info(f"[BOT SERVER] Creating answer...")
                        logger.info(f"[BOT SERVER] Connection state before answer: {sess.pc.connectionState}")
                        answer = await sess.pc.createAnswer()
                        await sess.pc.setLocalDescription(answer)
                        logger.info(f"[BOT SERVER] Answer created, connection state: {sess.pc.connectionState}")
                        logger.info(f"[BOT SERVER] Answer created, sending to Flask server")

                        answer_payload = {
                            "type": "answer",
                            "call_id": call_id,
                            "sdp": sess.pc.localDescription.sdp
                        }
                        await ws.send(json.dumps(answer_payload))
                        logger.info(f"[BOT SERVER] ✅ Answer sent for call_id: {call_id}")
                        print(f"[BOT SERVER] ✅ Answer sent for call: {call_id}")
                        
                    except Exception as e:
                        logger.error(f"[BOT SERVER] Error processing offer: {e}", exc_info=True)
                        print(f"[BOT SERVER] ❌ Error processing offer: {e}")
                        try:
                            await ws.send(json.dumps({
                                "type": "error",
                                "call_id": call_id,
                                "error": str(e)
                            }))
                        except Exception:
                            pass
                    continue

                if mtype == "ice":
                    logger.info(f"[BOT SERVER] ICE candidate received for call_id: {call_id}")
                    print(f"[BOT SERVER] 🧊 ICE candidate received for call: {call_id}")
                    
                    # Get the session for this call
                    sess = sessions.get(call_id)
                    if not sess:
                        logger.warning(f"[BOT SERVER] No session found for call_id: {call_id} when processing ICE")
                        print(f"[BOT SERVER] ⚠️  No session found for call: {call_id}")
                        # Send acknowledgment anyway so Flask doesn't timeout
                        try:
                            await ws.send(json.dumps({"type": "ice_ack", "call_id": call_id}))
                        except Exception:
                            pass
                        continue
                    
                    # Extract candidate from message
                    candidate_data = msg.get("candidate")
                    if not candidate_data:
                        logger.warning(f"[BOT SERVER] Missing candidate data in ICE message")
                        try:
                            await ws.send(json.dumps({"type": "ice_ack", "call_id": call_id}))
                        except Exception:
                            pass
                        continue
                    
                    # Handle candidate - can be dict or string
                    if isinstance(candidate_data, dict):
                        candidate_str = candidate_data.get("candidate") or ""
                    else:
                        candidate_str = str(candidate_data)
                    
                    if not candidate_str:
                        logger.warning(f"[BOT SERVER] Empty candidate string")
                        try:
                            await ws.send(json.dumps({"type": "ice_ack", "call_id": call_id}))
                        except Exception:
                            pass
                        continue
                    
                    try:
                        # Parse candidate string to create RTCIceCandidate
                        # Format: "candidate:3763323628 1 udp 2122129151 192.168.1.6 60134 typ host generation 0 ufrag SZRa network-id 1"
                        parts = candidate_str.split()
                        if len(parts) < 8:
                            logger.warning(f"[BOT SERVER] Invalid candidate format: {candidate_str}")
                            await ws.send(json.dumps({"type": "ice_ack", "call_id": call_id}))
                            continue
                        
                        # Extract candidate fields
                        foundation = parts[0].split(":")[1] if ":" in parts[0] else ""
                        component = int(parts[1]) if parts[1].isdigit() else 1
                        protocol = parts[2].lower()
                        priority = int(parts[3]) if parts[3].isdigit() else 0
                        ip = parts[4]
                        port = int(parts[5]) if parts[5].isdigit() else 0
                        typ = parts[7] if len(parts) > 7 else "host"
                        
                        # Get sdpMid and sdpMLineIndex from candidate_data if available
                        sdp_mid = "0"
                        sdp_mline_index = 0
                        if isinstance(candidate_data, dict):
                            sdp_mid = str(candidate_data.get("sdpMid", "0"))
                            sdp_mline_index = int(candidate_data.get("sdpMLineIndex", 0))
                        
                        # Create RTCIceCandidate
                        ice_candidate = RTCIceCandidate(
                            component=component,
                            foundation=foundation,
                            ip=ip,
                            port=port,
                            priority=priority,
                            protocol=protocol,
                            type=typ,
                            sdpMLineIndex=sdp_mline_index,
                            sdpMid=sdp_mid
                        )
                        
                        await sess.pc.addIceCandidate(ice_candidate)
                        logger.info(f"[BOT SERVER] ✅ ICE candidate added: {ip}:{port} ({typ})")
                        print(f"[BOT SERVER] ✅ ICE candidate added: {ip}:{port}")
                        
                        # Send acknowledgment
                        await ws.send(json.dumps({"type": "ice_ack", "call_id": call_id}))
                        logger.debug(f"[BOT SERVER] ICE acknowledgment sent")
                        
                    except Exception as e:
                        logger.error(f"[BOT SERVER] Error adding ICE candidate: {e}", exc_info=True)
                        print(f"[BOT SERVER] ❌ Error adding ICE candidate: {e}")
                        # Try to send acknowledgment anyway
                        try:
                            await ws.send(json.dumps({"type": "ice_ack", "call_id": call_id}))
                        except Exception:
                            pass
                    continue

                if mtype == "end":
                    logger.info(f"[BOT SERVER] Ending call_id: {call_id}")
                    print(f"[BOT SERVER] 🔚 Ending call: {call_id}")
                    sess = sessions.pop(call_id, None)
                    if sess:
                        try:
                            await sess.close()
                        except Exception as e:
                            logger.error(f"[BOT SERVER] Error closing session: {e}")
                    try:
                        await ws.send(json.dumps({"type": "ended", "call_id": call_id}))
                    except Exception:
                        pass
                    continue
                    
        except websockets.exceptions.ConnectionClosed:
            logger.info(f"[BOT SERVER] Connection closed normally")
            print(f"[BOT SERVER] 🔌 Connection closed")
        except Exception as e:
            logger.error(f"[BOT SERVER] Connection handler error: {e}", exc_info=True)
            print(f"[BOT SERVER] ❌ Connection error: {e}")

    print(f"✅ BOT signaling server listening ws://{host}:{port}")
    print(f"💡 Flask server should connect to: ws://127.0.0.1:{port}")
    logger.info(f"BOT signaling server starting on ws://{host}:{port}")
    
    # Increase ping interval and timeout to handle short-lived connections
    async with websockets.serve(
        handler, 
        host, 
        port, 
        ping_interval=30,  # Send ping every 30 seconds
        ping_timeout=10,   # Wait 10 seconds for pong
        close_timeout=10  # Wait 10 seconds when closing
    ):
        logger.info(f"BOT signaling server is running")
        print(f"🚀 BOT signaling server is running and ready for connections")
        await asyncio.Future()  # run forever

if __name__ == "__main__":
    mode = (os.getenv("BOT_MODE") or "local").lower()

    if mode == "webrtc":
        asyncio.run(webrtc_bot_signaling_server())
    else:
        asyncio.run(main())
