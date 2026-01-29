import asyncio
import json
import time
from typing import Optional, Tuple

import websockets


class LLMPersistentClient:
    """Persistent WebSocket client for LLM with auto-reconnect"""

    def __init__(self, server_url: str, token: str):
        self.server_url = server_url
        self.token = token
        self.ws = None
        self.lock = asyncio.Lock()
        self.connected = False
        self.reconnect_attempts = 0
        self.max_reconnect_attempts = 5
        self.reconnect_delay = 1.0

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
                print("🔗 Connecting to LLM WebSocket...")
                self.ws = await websockets.connect(
                    self.server_url,
                    ping_interval=10,
                    ping_timeout=20,
                    close_timeout=30,
                    max_size=10 * 1024 * 1024,
                    compression=None,
                )
                self.connected = True
                self.reconnect_attempts = 0
                print("✅ LLM WebSocket connected")
                return True

            except Exception as e:
                self.connected = False
                self.ws = None
                self.reconnect_attempts += 1

                if self.reconnect_attempts <= self.max_reconnect_attempts:
                    print(
                        f"⚠️ LLM WebSocket connection failed "
                        f"(attempt {self.reconnect_attempts}/{self.max_reconnect_attempts}): {e}"
                    )
                    await asyncio.sleep(self.reconnect_delay * self.reconnect_attempts)
                    return await self.connect()

                print("❌ Failed to connect to LLM WebSocket after max attempts")
                return False

    async def ensure_connection(self) -> bool:
        if not self.connected or not self.ws:
            return await self.connect()

        try:
            if self.ws.state != websockets.protocol.State.OPEN:
                print(f"⚠️ LLM WebSocket state={self.ws.state}, reconnecting...")
                self.connected = False
                self.ws = None
                return await self.connect()
            return True
        except Exception as e:
            print(f"⚠️ LLM WebSocket check failed: {e}, reconnecting...")
            self.connected = False
            self.ws = None
            return await self.connect()

    async def generate(self, prompt: str, prompt_id: int) -> Tuple[Optional[str], float]:
        start_time = time.time()

        for attempt in range(3):
            try:
                if not await self.ensure_connection():
                    print("❌ LLM connection failed")
                    return None, time.time() - start_time

                request = {
                    "model_id": "llama",
                    "prompt": prompt,
                    "prompt_id": prompt_id,
                }

                await asyncio.wait_for(self.ws.send(json.dumps(request)), timeout=5.0)
                response = await asyncio.wait_for(self.ws.recv(), timeout=30.0)

                response_data = json.loads(response)
                elapsed_time = time.time() - start_time

                if "error" in response_data:
                    print(f"⚠️ LLM error: {response_data['error']}")
                    if attempt < 2:
                        await asyncio.sleep(1.0)
                        continue
                    return None, elapsed_time

                if "text" in response_data:
                    return response_data["text"], elapsed_time

                return None, elapsed_time

            except websockets.exceptions.ConnectionClosed as e:
                print(f"⚠️ LLM connection closed: {e}, reconnecting...")
                self.connected = False
                self.ws = None
                if attempt < 2:
                    await asyncio.sleep(1.0)
                    continue
                return None, time.time() - start_time

            except asyncio.TimeoutError:
                print(f"⚠️ LLM timeout on attempt {attempt + 1}")
                if attempt < 2:
                    await asyncio.sleep(1.0)
                    continue
                return None, time.time() - start_time

            except Exception as e:
                print(f"⚠️ LLM error on attempt {attempt + 1}: {e}")
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
                    print("🔌 LLM WebSocket closed")
                except Exception:
                    pass
                finally:
                    self.ws = None


class RestaurantLLM:
    """
    Thin wrapper around LLMPersistentClient to enforce your system prompt behavior.
    """

    def __init__(self, server_url: str, token: str, system_prompt: str):
        self.llm_client = LLMPersistentClient(server_url, token)
        self.system_prompt = system_prompt

    async def generate_response(self, user_text: str, prompt_id: int, language: str = "hi") -> Tuple[Optional[str], float]:
        # Hard force output language
        lang_rule = (
            "Respond ONLY in Hindi. Do not mix Hindi and English in the same sentence."
            if language == "hi"
            else "Respond ONLY in English. Do not mix Hindi and English in the same sentence."
        )

        full_prompt = (
            f"{self.system_prompt}\n\n"
            f"{lang_rule}\n\n"
            f"User: {user_text}\n"
            f"Assistant:"
        )
        response, llm_time = await self.llm_client.generate(full_prompt, prompt_id)

        if not response:
            return None, llm_time

        raw = response.strip()

        # Remove common labels and preceding context
        for label in ["User:", "Assistant:", "System:", "Bot:", "Response:", "user:", "assistant:"]:
            if label in raw:
                parts = raw.split(label)
                raw = parts[-1].strip()

        # Take only the first sentence / line to keep speech short
        first_line = raw.splitlines()[0].strip()
        if "।" in first_line:
            first_line = first_line.split("।")[0] + "।"
        elif "." in first_line:
            first_line = first_line.split(".")[0] + "."

        cleaned = first_line.strip(": \n\t")
        return cleaned or None, llm_time

    async def translate_text(
        self,
        text: str,
        prompt_id: int,
        target_language: str = "hi",
    ) -> Tuple[Optional[str], float]:
        """
        Lightweight translation helper that does NOT include the long system prompt.
        Used to convert deterministic English RAG/template replies to Hindi (or English).
        """
        if not text:
            return None, 0.0

        lang_name = "Hindi" if target_language == "hi" else "English"
        prompt = (
            f"Translate this into natural {lang_name} in ONE very short sentence, "
            f"without adding anything extra:\n\n{text}"
        )

        response, llm_time = await self.llm_client.generate(prompt, prompt_id)
        if not response:
            return None, llm_time

        raw = response.strip()
        # If the model just echoes the instruction, treat as failure
        if raw.lower().startswith("translate this"):
            return None, llm_time

        # Keep the first sentence/line
        first_line = raw.splitlines()[0].strip()
        if "।" in first_line:
            first_line = first_line.split("।")[0] + "।"
        elif "." in first_line:
            first_line = first_line.split(".")[0] + "."

        return first_line.strip() or None, llm_time

    async def close(self):
        await self.llm_client.close()
