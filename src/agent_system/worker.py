from __future__ import annotations

import argparse
import threading
from typing import Sequence

from .app import create_app
from .infra.jobs import run_job
from .infra.observability import get_logger
from .infra.queue import CAMPAIGN_QUEUE, CONVERSATION_QUEUE, ERP_QUEUE, REPORT_QUEUE, QueueBroker


class Worker:
    def __init__(self, queue_name: str, broker: QueueBroker) -> None:
        self.app = create_app()
        self.queue_name = queue_name
        self.broker = broker
        self.logger = get_logger(f"worker.{queue_name}")
        self.running = True

    def run_forever(self) -> None:
        while self.running:
            job = self.broker.dequeue(self.queue_name, timeout_seconds=2)
            if not job:
                continue
            event_id = job.payload.get("event_id")
            if event_id and not self.app.db.claim_event(event_id):
                self.logger.info("event_not_claimed", extra={"extra": {"event_id": event_id}})
                continue
            try:
                result = run_job(self.app, job.queue, job.job_type, job.payload)
                if event_id:
                    self.app.db.mark_event_done(event_id)
                self.logger.info(
                    "job_processed",
                    extra={"extra": {"queue": job.queue, "job_type": job.job_type, "result": result}},
                )
            except Exception as exc:  # noqa: BLE001
                if event_id:
                    self.app.db.mark_event_error(event_id, str(exc))
                    state = self.app.db.event_by_id(event_id)
                    if state and state["status"] == "RETRY":
                        self.broker.enqueue(job.queue, job.job_type, job.payload)
                self.logger.error(
                    "job_failed",
                    extra={"extra": {"queue": job.queue, "job_type": job.job_type, "error": str(exc)}},
                )


def run_workers(queues: Sequence[str] | None = None) -> None:
    queue_names = list(queues or [CAMPAIGN_QUEUE, CONVERSATION_QUEUE, ERP_QUEUE, REPORT_QUEUE])
    broker = QueueBroker()
    threads = []
    for queue_name in queue_names:
        worker = Worker(queue_name, broker)
        thread = threading.Thread(target=worker.run_forever, daemon=False)
        thread.start()
        threads.append(thread)
    for thread in threads:
        thread.join()


def main() -> None:
    parser = argparse.ArgumentParser(description="Run asynchronous worker(s)")
    parser.add_argument("--queue", action="append", choices=[CAMPAIGN_QUEUE, CONVERSATION_QUEUE, ERP_QUEUE, REPORT_QUEUE])
    args = parser.parse_args()
    run_workers(args.queue)


if __name__ == "__main__":
    main()
