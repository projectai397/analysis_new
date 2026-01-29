import asyncio
import json
import re
import time
from typing import Optional, Tuple

import websockets
import logging

logger = logging.getLogger(__name__)

class STTPersistentClient:
    """Persistent WebSocket client for STT (Whisper) with auto-reconnect"""

    def __init__(self, server_url: str):
        self.server_url = server_url
        self.ws = None
        self.lock = asyncio.Lock()
        self.connected = False
        self.reconnect_attempts = 0
        self.max_reconnect_attempts = 5
        self.reconnect_delay = 1.0

    async def connect(self) -> bool:
        """Establish WebSocket connection or reconnect if needed"""
        async with self.lock:
            if self.connected and self.ws:
                try:
                    if self.ws.state == websockets.protocol.State.OPEN:
                        return True
                except Exception:
                    self.connected = False
                    self.ws = None

            try:
                print("🔗 Connecting to STT WebSocket...")
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
                print("✅ STT WebSocket connected")
                return True

            except Exception as e:
                self.connected = False
                self.ws = None
                self.reconnect_attempts += 1

                if self.reconnect_attempts <= self.max_reconnect_attempts:
                    print(
                        f"⚠️ STT WebSocket connection failed "
                        f"(attempt {self.reconnect_attempts}/{self.max_reconnect_attempts}): {e}"
                    )
                    await asyncio.sleep(self.reconnect_delay * self.reconnect_attempts)
                    return await self.connect()

                print("❌ Failed to connect to STT WebSocket after max attempts")
                return False

    async def ensure_connection(self) -> bool:
        """Ensure we have a valid connection"""
        if not self.connected or not self.ws:
            return await self.connect()

        try:
            if self.ws.state != websockets.protocol.State.OPEN:
                print(f"⚠️ STT WebSocket state={self.ws.state}, reconnecting...")
                self.connected = False
                self.ws = None
                return await self.connect()
            return True
        except Exception as e:
            print(f"⚠️ STT WebSocket check failed: {e}, reconnecting...")
            self.connected = False
            self.ws = None
            return await self.connect()

    async def transcribe(self, audio_b64: str, prompt_id: int, language: str = "hi") -> Tuple[Optional[str], float]:
        """Transcribe audio using persistent WebSocket """
        start_time = time.time()

        def _safe_cleanup(text: str) -> str:
            # remove control chars, keep all languages
            text = re.sub(r"[\x00-\x08\x0B\x0C\x0E-\x1F\x7F]", " ", text)
            text = re.sub(r"\s+", " ", text).strip()
            return text
    
        for attempt in range(3):
            try:
                if not await self.ensure_connection():
                    print("❌ STT connection failed, cannot send request")
                    return None, time.time() - start_time

                # request = {
                #     "model_id": "whisper",
                #     "prompt": audio_b64,
                #     "prompt_id": prompt_id,
                #     "language": language,
                #      "task": "transcribe",
                    
                # }
                request = {
                    "model_id": "indic_asr",
                    "prompt": audio_b64,
                    "prompt_id": prompt_id,
                    "language": language,
                    "decoder": "ctc",
                    "task": "transcribe"
                }

                await asyncio.wait_for(self.ws.send(json.dumps(request)), timeout=5.0)

                response = await asyncio.wait_for(self.ws.recv(), timeout=20.0)
                response_data = json.loads(response)
                elapsed_time = time.time() - start_time

                if "error" in response_data:
                    print(f"⚠️ STT error: {response_data['error']}")
                    if attempt < 2:
                        await asyncio.sleep(1.0)
                        continue
                    return None, elapsed_time

                if "text" in response_data:
                    transcription = response_data["text"]
                    print(f"📝 You said: {transcription}")

                    # keep basic English characters + Hindi Devanagari + punctuation
                    transcription = re.sub(r'[^\u0900-\u097F\u0041-\u005A\u0061-\u007A\u0030-\u0039\s.,!?\'"-]', "", transcription).strip()
                    if not transcription and attempt < 2:
                        print("⚠️ Transcription empty after cleaning, retrying...")
                        await asyncio.sleep(1.0)
                        continue

                    return transcription, elapsed_time

                return None, elapsed_time

            except websockets.exceptions.ConnectionClosed as e:
                print(f"⚠️ STT connection closed: {e}, reconnecting...")
                self.connected = False
                self.ws = None
                if attempt < 2:
                    await asyncio.sleep(1.0)
                    continue
                return None, time.time() - start_time

            except asyncio.TimeoutError:
                print(f"⚠️ STT timeout on attempt {attempt + 1}")
                if attempt < 2:
                    await asyncio.sleep(1.0)
                    continue
                return None, time.time() - start_time

            except Exception as e:
                print(f"⚠️ STT error on attempt {attempt + 1}: {e}")
                if attempt < 2:
                    await asyncio.sleep(1.0)
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
                    print("🔌 STT WebSocket closed")
                except Exception:
                    pass
                finally:
                    self.ws = None

class STTStreamingClient:
    """Streaming WebSocket client for STT with real-time processing"""

    def __init__(self, server_url: str):
        self.server_url = server_url
        self.ws = None
        self.lock = asyncio.Lock()
        self.connected = False
        self.reconnect_attempts = 0
        self.max_reconnect_attempts = 5
        self.reconnect_delay = 1.0
        
        # Streaming metrics
        self.metrics = {
            "chunks_processed": 0,
            "total_audio_ms": 0,
            "transcription_latency": [],
            "errors": 0
        }

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
                logger.info("🔗 Connecting to STT WebSocket...")
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
                logger.info("✅ STT WebSocket connected")
                return True

            except Exception as e:
                self.connected = False
                self.ws = None
                self.reconnect_attempts += 1
                self.metrics["errors"] += 1

                if self.reconnect_attempts <= self.max_reconnect_attempts:
                    logger.warning(
                        f"STT WebSocket connection failed "
                        f"(attempt {self.reconnect_attempts}/{self.max_reconnect_attempts}): {e}"
                    )
                    await asyncio.sleep(self.reconnect_delay * self.reconnect_attempts)
                    return await self.connect()

                logger.error("❌ Failed to connect to STT WebSocket after max attempts")
                return False

    async def ensure_connection(self) -> bool:
        """Ensure we have a valid connection"""
        if not self.connected or not self.ws:
            return await self.connect()

        try:
            if self.ws.state != websockets.protocol.State.OPEN:
                logger.warning(f"STT WebSocket state={self.ws.state}, reconnecting...")
                self.connected = False
                self.ws = None
                return await self.connect()
            return True
        except Exception as e:
            logger.warning(f"STT WebSocket check failed: {e}, reconnecting...")
            self.connected = False
            self.ws = None
            return await self.connect()

    def _clean_transcription(self, text: str) -> str:
        """Clean transcription while preserving language characters"""
        if not text:
            return ""
        
        # Remove control characters
        text = re.sub(r"[\x00-\x08\x0B\x0C\x0E-\x1F\x7F]", " ", text)
        
        # Collapse whitespace
        text = re.sub(r"\s+", " ", text).strip()
        
        # Preserve Hindi, Gujarati, and English characters
        # Remove only unwanted special characters, keep language scripts
        allowed_pattern = r'[^\u0900-\u097F\u0A80-\u0AFF\u0041-\u005A\u0061-\u007A\u0030-\u0039\s.,!?\'"-]'
        text = re.sub(allowed_pattern, "", text)
        
        return text

    async def streaming_transcribe(self, audio_b64: str, prompt_id: int, language: str = "hi") -> Tuple[Optional[str], float]:
        """Streaming transcription with metrics"""
        start_time = time.time()
        logger.info(f"Starting streaming transcription (Prompt ID: {prompt_id})")

        for attempt in range(3):
            try:
                if not await self.ensure_connection():
                    logger.error("❌ STT connection failed, cannot send request")
                    return None, time.time() - start_time


                request = {
                    "model_id": "whisper",
                    "prompt": audio_b64,
                    "prompt_id": prompt_id,
                    "language": language,
                    "task": "transcribe",
                    "streaming": True,  # Indicate streaming mode
                    "chunk_size": len(audio_b64)  # Total size for metrics
                    
                }

                # Prepare streaming request
                # request = {
                #     "model_id": "indic_asr",
                #     "prompt": audio_b64,
                #     "prompt_id": prompt_id,
                #     "language": language,
                #     "decoder": "ctc",
                #     "task": "transcribe",
                #     "streaming": True,  # Indicate streaming mode
                #     "chunk_size": len(audio_b64)  # Total size for metrics
                # }

                logger.debug(f"Sending STT request: {prompt_id}, language: {language}")
                
                # Send request
                send_start = time.time()
                await asyncio.wait_for(self.ws.send(json.dumps(request)), timeout=5.0)
                send_time = time.time() - send_start
                logger.debug(f"Request sent in {send_time:.3f}s")

                # Receive response
                receive_start = time.time()
                response = await asyncio.wait_for(self.ws.recv(), timeout=20.0)
                receive_time = time.time() - receive_start
                
                response_data = json.loads(response)
                elapsed_time = time.time() - start_time
                
                # Update metrics
                self.metrics["chunks_processed"] += 1
                self.metrics["total_audio_ms"] += len(audio_b64) / 1024 * 8  # Approximate ms
                self.metrics["transcription_latency"].append(elapsed_time)

                logger.debug(f"Response received in {receive_time:.3f}s, total: {elapsed_time:.3f}s")

                if "error" in response_data:
                    error_msg = response_data.get("error", "Unknown error")
                    logger.error(f"⚠️ STT error: {error_msg}")
                    if attempt < 2:
                        await asyncio.sleep(1.0)
                        continue
                    return None, elapsed_time

                if "text" in response_data:
                    transcription = response_data["text"]
                    cleaned_transcription = self._clean_transcription(transcription)
                    
                    if cleaned_transcription:
                        logger.info(f"📝 Transcription: {cleaned_transcription[:100]}...")
                        logger.debug(f"Raw transcription: {transcription[:100]}...")
                        
                        # Log metrics
                        logger.info(f"STT Metrics: latency={elapsed_time:.3f}s, "
                                  f"chunks={self.metrics['chunks_processed']}, "
                                  f"errors={self.metrics['errors']}")
                        
                        return cleaned_transcription, elapsed_time
                    else:
                        logger.warning("⚠️ Transcription empty after cleaning")
                        if attempt < 2:
                            await asyncio.sleep(1.0)
                            continue

                return None, elapsed_time

            except websockets.exceptions.ConnectionClosed as e:
                logger.error(f"⚠️ STT connection closed: {e}, reconnecting...")
                self.connected = False
                self.ws = None
                self.metrics["errors"] += 1
                if attempt < 2:
                    await asyncio.sleep(1.0)
                    continue
                return None, time.time() - start_time

            except asyncio.TimeoutError:
                logger.error(f"⚠️ STT timeout on attempt {attempt + 1}")
                self.metrics["errors"] += 1
                if attempt < 2:
                    await asyncio.sleep(1.0)
                    continue
                return None, time.time() - start_time

            except Exception as e:
                logger.error(f"⚠️ STT error on attempt {attempt + 1}: {e}")
                self.metrics["errors"] += 1
                if attempt < 2:
                    await asyncio.sleep(1.0)
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
                    logger.info("🔌 STT WebSocket closed")
                    
                    # Log final metrics
                    avg_latency = sum(self.metrics["transcription_latency"]) / max(len(self.metrics["transcription_latency"]), 1)
                    logger.info(f"STT Final Metrics: "
                              f"avg_latency={avg_latency:.3f}s, "
                              f"total_chunks={self.metrics['chunks_processed']}, "
                              f"total_errors={self.metrics['errors']}")
                              
                except Exception as e:
                    logger.error(f"Error closing STT WebSocket: {e}")
                finally:
                    self.ws = None