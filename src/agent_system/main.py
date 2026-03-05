from __future__ import annotations

import argparse

from .api import run_server
from .app import create_app
from .scheduler import Scheduler
from .worker import run_workers


def main() -> None:
    parser = argparse.ArgumentParser(description="Bitrix+Omie AI Commercial Agent")
    parser.add_argument("command", choices=["serve", "run-jobs", "workers", "seed"], help="command to run")
    parser.add_argument("--host", default="127.0.0.1")
    parser.add_argument("--port", type=int, default=8080)
    args = parser.parse_args()

    if args.command == "serve":
        run_server(args.host, args.port)
        return

    app = create_app()

    if args.command == "run-jobs":
        Scheduler(app).run_once_all()
        print("Enqueued stock sync, outreach, follow-up, telemetry, and daily report.")
        return

    if args.command == "workers":
        run_workers()
        return

    if args.command == "seed":
        app.bootstrap_demo_data()
        print("Seed complete.")


if __name__ == "__main__":
    main()
