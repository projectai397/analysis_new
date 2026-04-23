import os
import sys
import asyncio
import json
import base64
import io
import logging
import uuid
import numpy as np
from pathlib import Path
from dotenv import load_dotenv
from websockets.server import serve

import torch
import soundfile as sf
from TTS.api import TTS

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s",
)
logger = logging.getLogger("TTSWorker")

TEMP_VOICE_DIR = Path("temp_voices")
TEMP_VOICE_DIR.mkdir(exist_ok=True)

load_dotenv()
DEVICE = "cuda" if torch.cuda.is_available() else "cpu"
if len(sys.argv) > 1:
    WORKER_PORT = int(sys.argv[1])
else:
    WORKER_PORT = int(os.getenv("TTS_WORKER_PORT", "8766"))

logger.info("Loading XTTS v2...")
xtts_api = TTS("tts_models/multilingual/multi-dataset/xtts_v2").to(DEVICE)
DEFAULT_SPEAKER = xtts_api.speakers[0] if xtts_api.speakers else None


def wav_to_b64(wav, sr: int):
    if hasattr(wav, "dtype") and str(wav.dtype) == "float16":
        wav = wav.astype("float32")
    if hasattr(wav, "dtype") and "float" in str(wav.dtype):
        wav = np.clip(wav, -1.0, 1.0)
    out_io = io.BytesIO()
    sf.write(out_io, wav, sr, format="WAV")
    return base64.b64encode(out_io.getvalue()).decode("utf-8")


async def handle_xtts(text: str, voice_ref_b64: str | None = None, language: str = "en"):
    temp_ref_path = None
    try:
        if voice_ref_b64:
            temp_ref_path = TEMP_VOICE_DIR / f"ref_{uuid.uuid4()}.wav"
            ref_bytes = base64.b64decode(voice_ref_b64)
            with open(temp_ref_path, "wb") as f:
                f.write(ref_bytes)
            wav = xtts_api.tts(text=text, speaker_wav=str(temp_ref_path), language=language)
        else:
            wav = xtts_api.tts(text=text, speaker=DEFAULT_SPEAKER, language=language)
        return wav_to_b64(wav, sr=24000)
    finally:
        if temp_ref_path and temp_ref_path.exists():
            try:
                os.remove(temp_ref_path)
            except Exception as e:
                logger.warning(f"Could not delete temp file: {e}")


IDLE_TIMEOUT_SEC = 300


async def handler(websocket, path):
    client_ip = websocket.remote_address[0]
    logger.info(f"TTS worker connected: {client_ip}")

    while True:
        idle_waiter = asyncio.create_task(asyncio.sleep(IDLE_TIMEOUT_SEC))
        recv_waiter = asyncio.create_task(websocket.recv())
        done, pending = await asyncio.wait(
            [idle_waiter, recv_waiter], return_when=asyncio.FIRST_COMPLETED
        )
        for t in pending:
            t.cancel()
            try:
                await t
            except asyncio.CancelledError:
                pass

        if idle_waiter in done:
            logger.info(f"Idle timeout ({IDLE_TIMEOUT_SEC}s), disconnecting: {client_ip}")
            break

        try:
            message = recv_waiter.result()
        except Exception:
            break

        is_ping = False
        if isinstance(message, str) and message.strip().lower() == "ping":
            is_ping = True
        else:
            try:
                data_test = json.loads(message)
                if isinstance(data_test, dict) and data_test.get("type") == "ping":
                    is_ping = True
                elif isinstance(data_test, dict) and data_test.get("model_id") == "ping":
                    is_ping = True
            except (json.JSONDecodeError, TypeError):
                pass

        if is_ping:
            await websocket.send(json.dumps({"type": "pong"}, ensure_ascii=False))
            continue

        prompt_id = None
        try:
            data = json.loads(message)
            prompt_id = data.get("prompt_id")
            prompt = data.get("prompt")
            voice_reference = data.get("voice_reference")
            is_cloning = bool(data.get("voice_cloning", False))
            language = data.get("xtts_language", data.get("language", "en"))

            if not isinstance(prompt, str) or not prompt:
                raise ValueError("prompt must be non-empty text")

            logger.info(f"TTS worker port {WORKER_PORT}: processing request prompt_id={prompt_id}")
            ref_to_use = voice_reference if is_cloning else None
            result = await handle_xtts(prompt, ref_to_use, language=language)
            logger.info(f"TTS worker port {WORKER_PORT}: done prompt_id={prompt_id}")
            resp = {"status": "success", "audio_b64": result, "prompt_id": prompt_id, "sr": 24000}
            await websocket.send(json.dumps(resp, ensure_ascii=False))

        except Exception as e:
            logger.error(f"Error: {str(e)}", exc_info=True)
            await websocket.send(
                json.dumps({"error": str(e), "prompt_id": prompt_id}, ensure_ascii=False)
            )


async def main():
    async with serve(handler, "0.0.0.0", WORKER_PORT, max_size=40 * 1024 * 1024):
        logger.info(f"TTS worker instance listening on port {WORKER_PORT}")
        await asyncio.Future()


if __name__ == "__main__":
    asyncio.run(main())
