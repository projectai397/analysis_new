# Option A: Single process for WebSocket (call signaling)

So that **call.accepted** and other call events reach the right client in **live**, the app must run with **one process** handling `/ws`. Socket maps (`USER_SOCKETS`, `MASTER_SOCKETS`, `ACTIVE_CALLS`) are in-memory and not shared across processes.

## How this app is run

- **Development / default production**: This app uses **Waitress**, which is **single-process** by default. No change needed; WebSocket call signaling works.
- **Live with Gunicorn/uWSGI**: You must use **one worker only**. Otherwise the user and master can be on different workers and `call.accepted` will not be delivered to the user.

## Commands for live (Option A)

### 1. Keep using Waitress (recommended)

```bash
# From project root, same as now – already single process
python app.py
# or
waitress-serve --host=0.0.0.0 --port=8013 --call app:create_app
```

### 2. If you use Gunicorn

Use **exactly one worker**:

```bash
gunicorn -w 1 -k sync -b 0.0.0.0:8013 "app:create_app()"
```

Do **not** use `-w 2` or more for the same app that serves `/ws` if you want call signaling to work.

### 3. Docker / multiple replicas

- Either run **one container/replica** for this service, or  
- Put `/ws` behind a **dedicated single-instance** service and keep the rest of the API scaled.

## Summary

| Environment | Requirement |
|-------------|-------------|
| Local      | Single process (default) – works. |
| Live       | **Single process** for the app that serves `/ws` (Waitress default or Gunicorn `-w 1`). |

After switching live to a single process, `call.accepted` should reach the user when the master accepts.
