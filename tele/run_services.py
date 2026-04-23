import multiprocessing
import signal
import sys
import threading
import time


def run_app() -> None:
    import app as app_module

    # Run scheduler in background inside the app process.
    schedule_thread = threading.Thread(target=app_module.run_schedule, daemon=True)
    schedule_thread.start()
    app_module.app.run(host="0.0.0.0", port=8015)


def run_bot() -> None:
    import bot as bot_module

    bot_module.main()


def terminate_processes(processes: list[multiprocessing.Process]) -> None:
    for process in processes:
        if process.is_alive():
            process.terminate()
    for process in processes:
        process.join(timeout=5)


def main() -> None:
    multiprocessing.set_start_method("spawn", force=True)

    app_process = multiprocessing.Process(target=run_app, name="app")
    bot_process = multiprocessing.Process(target=run_bot, name="bot")
    processes = [app_process, bot_process]

    for process in processes:
        process.start()

    def shutdown_handler(signum, frame):  # type: ignore[unused-argument]
        terminate_processes(processes)
        sys.exit(0)

    signal.signal(signal.SIGINT, shutdown_handler)
    signal.signal(signal.SIGTERM, shutdown_handler)

    try:
        while True:
            dead = [p for p in processes if not p.is_alive()]
            if dead:
                terminate_processes(processes)
                raise SystemExit(f"Process stopped unexpectedly: {dead[0].name}")
            time.sleep(1)
    except KeyboardInterrupt:
        terminate_processes(processes)


if __name__ == "__main__":
    main()
