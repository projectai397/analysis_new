import os
import re
import asyncio
import json
import time
import torch
import warnings
import base64
import io
import logging
import soundfile as sf
import librosa
import numpy as np
from pathlib import Path
from dotenv import load_dotenv
import websockets
from websockets.server import serve
from websockets.exceptions import ConnectionClosed
from urllib.parse import urlparse, parse_qs
from itertools import cycle

# --- 1. CRITICAL IMPORTS ---
from transformers import (
    AutoModel,
    AutoTokenizer,
    VitsModel,
    WhisperProcessor,
    WhisperForConditionalGeneration,
)

# --- 2. SETUP LOGGING & DIRECTORIES ---
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s",
)
logger = logging.getLogger("ModelServer")
logging.getLogger("websockets").setLevel(logging.WARNING)

load_dotenv()
warnings.filterwarnings("ignore")

# --- 3. CONFIGURATION ---
STATIC_TOKEN = os.getenv("STATIC_TOKEN")
HF_TOKEN = os.getenv("HF_TOKEN")  # optional (needed if any repo is gated)
DEVICE = "cuda" if torch.cuda.is_available() else "cpu"
DTYPE = torch.float16 if DEVICE == "cuda" else torch.float32

logger.info(f"Initializing models on: {DEVICE} (dtype={DTYPE})")

# -----------------------
# 0) Whisper LARGE V3 TURBO (STT)
# -----------------------
logger.info("Loading Whisper Large v3 Turbo (STT)...")
whisper_id = "openai/whisper-large-v3-turbo"
whisper_proc = WhisperProcessor.from_pretrained(whisper_id, token=HF_TOKEN)

# Use fp16 on CUDA for speed; keep fp32 on CPU for correctness
whisper_dtype = torch.float16 if DEVICE == "cuda" else torch.float32
whisper_model = WhisperForConditionalGeneration.from_pretrained(
    whisper_id, torch_dtype=whisper_dtype, token=HF_TOKEN
).to(DEVICE)
whisper_model.eval()

# -----------------------
# 1) IndicConformer 600M multilingual (ASR for Gujarati/Hindi)
# -----------------------
logger.info("Loading IndicConformer 600M multilingual (ASR)...")
indic_asr_id = "ai4bharat/indic-conformer-600m-multilingual"
indic_asr_model = AutoModel.from_pretrained(
    indic_asr_id,
    trust_remote_code=True,
    token=HF_TOKEN,
).to(DEVICE)
indic_asr_model.eval()

# -----------------------
# 2) TTS workers (XTTS) - round-robin proxy
# -----------------------
_tts_worker_urls = [u.strip() for u in os.getenv("TTS_WORKER_URLS", "ws://127.0.0.1:8766,ws://127.0.0.1:8767,ws://127.0.0.1:8768").split(",") if u.strip()]
_tts_worker_cycle = cycle(_tts_worker_urls) if _tts_worker_urls else None
TTS_PROXY_TIMEOUT = float(os.getenv("TTS_PROXY_TIMEOUT", "120"))
TTS_MIN_CHARS_TO_SPLIT = int(os.getenv("TTS_MIN_CHARS_TO_SPLIT", "90"))
TTS_TARGET_CHUNK_CHARS = int(os.getenv("TTS_TARGET_CHUNK_CHARS", "35"))
TTS_MAX_CHUNKS = int(os.getenv("TTS_MAX_CHUNKS", "6"))
_tts_semaphore = asyncio.Semaphore(len(_tts_worker_urls)) if _tts_worker_urls else None

if _tts_worker_urls:
    logger.info(f"TTS workers configured: {len(_tts_worker_urls)} instance(s) -> {_tts_worker_urls}")
else:
    logger.warning("No TTS workers configured (TTS_WORKER_URLS); xtts requests will fail")

# -----------------------
# 3) MMS Gujarati TTS (single voice, no cloning)
# -----------------------
logger.info("Loading MMS Gujarati TTS...")
mms_tts_id = "facebook/mms-tts-guj"
mms_tok = AutoTokenizer.from_pretrained(mms_tts_id, token=HF_TOKEN)
mms_model = VitsModel.from_pretrained(
    mms_tts_id, torch_dtype=torch.float32, token=HF_TOKEN
).to(DEVICE)
mms_model.eval()
MMS_SR = int(getattr(mms_model.config, "sampling_rate", 16000))

# --- 4. HELPERS ---
def decode_audio_b64_to_float(audio_b64: str, target_sr: int = 16000):
    audio_bytes = base64.b64decode(audio_b64)
    data, sr = sf.read(io.BytesIO(audio_bytes), always_2d=False)

    if hasattr(data, "ndim") and data.ndim > 1:
        data = data.mean(axis=1)

    if sr != target_sr:
        data = librosa.resample(data, orig_sr=sr, target_sr=target_sr)

    if str(data.dtype) != "float32":
        data = data.astype("float32")

    return data, target_sr


def wav_to_b64(wav, sr: int):
    if hasattr(wav, "dtype") and str(wav.dtype) == "float16":
        wav = wav.astype("float32")
    if hasattr(wav, "dtype") and "float" in str(wav.dtype):
        wav = np.clip(wav, -1.0, 1.0)
    out_io = io.BytesIO()
    sf.write(out_io, wav, sr, format="WAV")
    return base64.b64encode(out_io.getvalue()).decode("utf-8")


def safe_prompt_id(data: dict):
    try:
        return data.get("prompt_id")
    except Exception:
        return None


def split_text_for_tts(text: str, n: int = 3, target_chars: int | None = None) -> list[str]:
    text = text.strip()
    if not text or n <= 1:
        return [text] if text else []
    target = target_chars or max(1, len(text) // n)
    parts = re.split(r"(?<=[.!?])\s+", text)
    parts = [p.strip() for p in parts if p.strip()]
    if len(parts) >= n:
        chunk_size = (len(parts) + n - 1) // n
        return [" ".join(parts[i : i + chunk_size]) for i in range(0, len(parts), chunk_size)][:n]
    desired_by_len = (len(text) + target - 1) // target
    if len(parts) > 1 and desired_by_len < n:
        return [" ".join(parts[i::n]) for i in range(n) if parts[i::n]]
    size = max(1, (len(text) + n - 1) // n)
    words = text.split()
    if len(words) <= 1:
        return [text[i : i + size].strip() for i in range(0, len(text), size) if text[i : i + size].strip()][:n]
    chunks, current, current_len = [], [], 0
    for w in words:
        current.append(w)
        current_len += len(w) + (1 if current else 0)
        if current_len >= size and len(chunks) < n - 1:
            chunks.append(" ".join(current))
            current, current_len = [], 0
    if current:
        chunks.append(" ".join(current))
    return chunks[:n]


def concat_audio_b64(b64_list: list[str], sr: int = 24000) -> str:
    if not b64_list:
        raise ValueError("concat_audio_b64: empty list")
    if len(b64_list) == 1:
        return b64_list[0]
    chunks = []
    for b64 in b64_list:
        data, _ = sf.read(io.BytesIO(base64.b64decode(b64)), always_2d=False)
        if hasattr(data, "ndim") and data.ndim > 1:
            data = data.mean(axis=1)
        chunks.append(data.astype(np.float32))
    out = np.concatenate(chunks)
    buf = io.BytesIO()
    sf.write(buf, out, sr, format="WAV")
    return base64.b64encode(buf.getvalue()).decode("utf-8")


# --- 5. INFERENCE HANDLERS ---
async def handle_whisper_large_v3_turbo(audio_b64: str, language: str | None = None):
    """
    Whisper Large v3 Turbo STT.
    If language is provided (e.g., "en", "hi", "gu"), Whisper will be forced to transcribe in that language.
    """
    t0 = time.perf_counter()
    logger.info("Whisper Large v3 Turbo: decoding audio...")
    audio_np, _ = decode_audio_b64_to_float(audio_b64, target_sr=16000)

    inputs = whisper_proc(audio_np, sampling_rate=16000, return_tensors="pt")

    # Move to device first
    inputs = {k: v.to(DEVICE) for k, v in inputs.items()}

    # CRITICAL: match Whisper model dtype (fp16 on CUDA, fp32 on CPU)
    inputs["input_features"] = inputs["input_features"].to(dtype=whisper_dtype)

    gen_kwargs = {}
    if language:
        try:
            forced_ids = whisper_proc.get_decoder_prompt_ids(language=language, task="transcribe")
            gen_kwargs["forced_decoder_ids"] = forced_ids
        except Exception as e:
            logger.warning(f"Whisper: could not set forced language '{language}': {e}")

    with torch.no_grad():
        predicted_ids = whisper_model.generate(inputs["input_features"], **gen_kwargs)

    result = whisper_proc.batch_decode(predicted_ids, skip_special_tokens=True)[0]
    elapsed = time.perf_counter() - t0
    logger.info(f"Whisper Large v3 Turbo: STT completed in {elapsed:.2f}s")
    return result


async def handle_indic_conformer_stt(audio_b64: str, language: str = "hi"):
    # FORCE CTC ONLY
    if language not in ("hi", "gu"):
        raise ValueError("indic_asr language must be 'hi' (Hindi) or 'gu' (Gujarati)")

    audio_np, _ = decode_audio_b64_to_float(audio_b64, target_sr=16000)
    wav = torch.from_numpy(audio_np).unsqueeze(0).to(DEVICE, dtype=torch.float32)

    with torch.no_grad():
        result = indic_asr_model(wav, language, "ctc")

    if isinstance(result, str):
        return result
    if isinstance(result, (list, tuple)) and result and isinstance(result[0], str):
        return result[0]
    return str(result)


async def proxy_xtts_to_worker(payload: dict) -> dict:
    if not _tts_worker_cycle:
        return {"error": "No TTS workers configured (TTS_WORKER_URLS)", "prompt_id": payload.get("prompt_id")}
    worker_url = next(_tts_worker_cycle)
    prompt_id = payload.get("prompt_id")
    t0 = asyncio.get_event_loop().time()
    logger.info(f"XTTS: proxying to TTS worker {worker_url} prompt_id={prompt_id}")
    try:
        async with websockets.connect(worker_url, max_size=40 * 1024 * 1024, close_timeout=2) as ws:
            await ws.send(json.dumps(payload, ensure_ascii=False))
            resp = await asyncio.wait_for(ws.recv(), timeout=TTS_PROXY_TIMEOUT)
            out = json.loads(resp)
            elapsed = asyncio.get_event_loop().time() - t0
            if out.get("status") == "success":
                logger.info(f"XTTS: worker {worker_url} completed prompt_id={prompt_id} in {elapsed:.2f}s")
            else:
                logger.warning(f"XTTS: worker {worker_url} returned error prompt_id={prompt_id} in {elapsed:.2f}s")
            return out
    except asyncio.TimeoutError:
        elapsed = asyncio.get_event_loop().time() - t0
        logger.warning(f"XTTS: worker {worker_url} timeout prompt_id={prompt_id} after {elapsed:.2f}s")
        return {"error": "TTS worker timeout", "prompt_id": payload.get("prompt_id")}
    except Exception as e:
        elapsed = asyncio.get_event_loop().time() - t0
        logger.warning(f"TTS worker {worker_url} failed after {elapsed:.2f}s: {e}")
        return {"error": f"TTS worker error: {e}", "prompt_id": payload.get("prompt_id")}


async def handle_mms_tts_guj(text: str):
    logger.info("MMS Gujarati TTS: generating audio...")
    inputs = mms_tok(text, return_tensors="pt")
    inputs = {k: v.to(DEVICE) for k, v in inputs.items()}

    with torch.no_grad():
        out = mms_model(**inputs)

    wav = out.waveform[0].detach().to(torch.float32).cpu().numpy()
    return wav_to_b64(wav, sr=MMS_SR)


IDLE_TIMEOUT_SEC = 300

# --- 6. SERVER ROUTER ---
async def router(websocket, path):
    client_ip = websocket.remote_address[0]
    query_params = parse_qs(urlparse(path).query)
    token = query_params.get("token", [None])[0]

    if token != STATIC_TOKEN:
        try:
            await websocket.send(json.dumps({"error": "Unauthorized"}, ensure_ascii=False))
        except ConnectionClosed:
            pass
        return

    logger.info(f"Connected: {client_ip}")

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
            logger.info(f"Ping from {client_ip}")
            try:
                await websocket.send(json.dumps({"type": "pong"}, ensure_ascii=False))
            except ConnectionClosed:
                break
            continue

        prompt_id_for_error = None
        try:
            data = json.loads(message)

            prompt_id_for_error = safe_prompt_id(data)
            prompt_id = data.get("prompt_id")
            model_id = data.get("model_id")
            prompt = data.get("prompt")

            # XTTS voice cloning fields
            voice_reference = data.get("voice_reference")
            is_cloning = bool(data.get("voice_cloning", False))
            xtts_language = data.get("xtts_language", data.get("language", "en"))

            # IndicConformer fields (CTC forced)
            asr_language = data.get("asr_language", data.get("language", "hi"))

            # Whisper fields
            whisper_language = data.get("whisper_language") or data.get("language")  # optional

            if model_id == "whisper":
                if not isinstance(prompt, str) or not prompt:
                    raise ValueError("whisper expects base64 WAV bytes string in 'prompt'")
                result = await handle_whisper_large_v3_turbo(prompt, language=whisper_language)
                resp = {"status": "success", "text": result, "prompt_id": prompt_id}

            elif model_id in ("indic_asr", "indic_conformer"):
                if not isinstance(prompt, str) or not prompt:
                    raise ValueError("indic_asr expects base64 WAV bytes string in 'prompt'")
                if asr_language not in ("hi", "gu"):
                    raise ValueError("indic_asr language must be 'hi' or 'gu'")
                result = await handle_indic_conformer_stt(prompt, language=asr_language)
                resp = {
                    "status": "success",
                    "text": result,
                    "prompt_id": prompt_id,
                    "language": asr_language,
                    "decoder": "ctc",
                }

            elif model_id == "xtts":
                if not isinstance(prompt, str) or not prompt:
                    raise ValueError("xtts expects non-empty text string in 'prompt'")
                stream_chunks = bool(data.get("stream_chunks", False))
                if len(prompt) >= TTS_MIN_CHARS_TO_SPLIT and _tts_worker_urls:
                    num_chunks = min(TTS_MAX_CHUNKS, max(2, (len(prompt) + TTS_TARGET_CHUNK_CHARS - 1) // TTS_TARGET_CHUNK_CHARS))
                    chunks = split_text_for_tts(prompt, num_chunks, TTS_TARGET_CHUNK_CHARS)
                    if len(chunks) >= 2:
                        t_split_start = asyncio.get_event_loop().time()
                        payloads = [
                            {
                                "model_id": "xtts",
                                "prompt": part,
                                "voice_reference": voice_reference if is_cloning else None,
                                "voice_cloning": is_cloning,
                                "xtts_language": xtts_language,
                                "prompt_id": prompt_id,
                            }
                            for part in chunks
                        ]
                        async def run_chunk_with_sem(p: dict):
                            if _tts_semaphore:
                                async with _tts_semaphore:
                                    return await proxy_xtts_to_worker(p)
                            return await proxy_xtts_to_worker(p)

                        if stream_chunks:
                            async def run_chunk(i: int, p: dict):
                                return (i, await run_chunk_with_sem(p))

                            tasks = [asyncio.create_task(run_chunk(i, p)) for i, p in enumerate(payloads)]
                            results_by_idx = {}
                            next_idx = 0
                            stream_closed = False
                            for coro in asyncio.as_completed(tasks):
                                if stream_closed:
                                    break
                                idx, chunk_resp = await coro
                                results_by_idx[idx] = chunk_resp
                                while next_idx in results_by_idx:
                                    r = results_by_idx[next_idx]
                                    if r.get("status") == "success":
                                        try:
                                            await websocket.send(json.dumps({
                                                "status": "chunk",
                                                "chunk_index": next_idx,
                                                "total_chunks": len(payloads),
                                                "audio_b64": r.get("audio_b64", ""),
                                                "prompt_id": prompt_id,
                                                "sr": r.get("sr", 24000),
                                            }, ensure_ascii=False))
                                        except ConnectionClosed:
                                            stream_closed = True
                                            break
                                    next_idx += 1
                            if not stream_closed:
                                try:
                                    await websocket.send(json.dumps({
                                        "status": "success",
                                        "prompt_id": prompt_id,
                                        "done": True,
                                        "total_chunks": len(payloads),
                                    }, ensure_ascii=False))
                                except ConnectionClosed:
                                    pass
                            elapsed = asyncio.get_event_loop().time() - t_split_start
                            logger.info(f"XTTS split into {len(payloads)} chunks (stream): complete in {elapsed:.2f}s prompt_id={prompt_id}")
                            continue
                        else:
                            results = await asyncio.gather(*[run_chunk_with_sem(p) for p in payloads])
                            err = next((r for r in results if r.get("error")), None)
                            if err:
                                resp = err
                            else:
                                audios = [r["audio_b64"] for r in results if r.get("status") == "success" and r.get("audio_b64")]
                                if len(audios) != len(results):
                                    resp = {"error": "One or more TTS chunks failed", "prompt_id": prompt_id}
                                else:
                                    resp = {
                                        "status": "success",
                                        "audio_b64": concat_audio_b64(audios, sr=24000),
                                        "prompt_id": prompt_id,
                                        "sr": 24000,
                                    }
                            elapsed = asyncio.get_event_loop().time() - t_split_start
                            logger.info(f"XTTS split into {len(payloads)} chunks: complete in {elapsed:.2f}s prompt_id={prompt_id}")
                    else:
                        payload = {
                            "model_id": "xtts",
                            "prompt": prompt,
                            "voice_reference": voice_reference if is_cloning else None,
                            "voice_cloning": is_cloning,
                            "xtts_language": xtts_language,
                            "prompt_id": prompt_id,
                        }
                        resp = await proxy_xtts_to_worker(payload)
                else:
                    payload = {
                        "model_id": "xtts",
                        "prompt": prompt,
                        "voice_reference": voice_reference if is_cloning else None,
                        "voice_cloning": is_cloning,
                        "xtts_language": xtts_language,
                        "prompt_id": prompt_id,
                    }
                    resp = await proxy_xtts_to_worker(payload)

            elif model_id in ("mms_tts_guj", "mms-tts-guj"):
                if not isinstance(prompt, str) or not prompt:
                    raise ValueError("mms_tts_guj expects non-empty text string in 'prompt'")
                if data.get("voice_cloning") or data.get("voice_reference"):
                    resp = {"error": "mms_tts_guj does not support voice cloning", "prompt_id": prompt_id}
                else:
                    result = await handle_mms_tts_guj(prompt)
                    resp = {"status": "success", "audio_b64": result, "prompt_id": prompt_id, "sr": MMS_SR}

            else:
                resp = {"error": f"Unknown model_id: {model_id}", "prompt_id": prompt_id}

            try:
                await websocket.send(json.dumps(resp, ensure_ascii=False))
            except ConnectionClosed:
                break

        except Exception as e:
            logger.error(f"Error: {str(e)}", exc_info=True)
            try:
                await websocket.send(
                    json.dumps({"error": str(e), "prompt_id": prompt_id_for_error}, ensure_ascii=False)
                )
            except ConnectionClosed:
                break


async def main():
    async with serve(router, "0.0.0.0", 8765, max_size=40 * 1024 * 1024):
        logger.info("WebSocket Server listening on port 8765")
        await asyncio.Future()


if __name__ == "__main__":
    asyncio.run(main())
