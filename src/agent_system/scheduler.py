from __future__ import annotations

import time
from dataclasses import dataclass
from datetime import date, datetime
from zoneinfo import ZoneInfo

from .app import AgentApp, create_app
from .config import SETTINGS
from .infra.observability import get_logger
from .infra.queue import CAMPAIGN_QUEUE, ERP_QUEUE, REPORT_QUEUE


@dataclass
class Job:
    name: str
    schedule_key: str


class Scheduler:
    def __init__(self, app: AgentApp) -> None:
        self.app = app
        self.logger = get_logger("scheduler")
        self._running = False
        self.tz = ZoneInfo(SETTINGS.tz)
        self.last_tick: dict[str, str] = {}
        self.jobs = [
            Job("stock_sync", "every_15m"),
            Job("daily_outreach", "daily_0900"),
            Job("telemetry", "daily_1000"),
            Job("follow_up", "daily_1400"),
            Job("daily_report", "daily_1800"),
        ]

    def run_once_all(self) -> None:
        self._enqueue_stock_sync()
        self._enqueue_outreach()
        self._enqueue_followup()
        self._enqueue_telemetry()
        self._enqueue_daily_report()

    def run_forever(self, poll_seconds: int = 20) -> None:
        self._running = True
        while self._running:
            now = datetime.now(self.tz)
            self._maybe_schedule(now)
            time.sleep(poll_seconds)

    def stop(self) -> None:
        self._running = False

    def _maybe_schedule(self, now: datetime) -> None:
        if self._should_skip_for_calendar(now.date()):
            return
        if now.minute % SETTINGS.stock_sync_interval_minutes == 0:
            self._run_once_per_key(now, "stock_sync", self._enqueue_stock_sync, "%Y%m%d%H%M")

        self._run_daily_if_match(now, "daily_outreach", SETTINGS.weekday_outreach_hour, 0, self._enqueue_outreach, weekday_only=True)
        self._run_daily_if_match(now, "telemetry", SETTINGS.telemetry_campaign_hour, 0, self._enqueue_telemetry, weekday_only=True)
        self._run_daily_if_match(now, "follow_up", SETTINGS.weekday_followup_hour, 0, self._enqueue_followup, weekday_only=True)
        self._run_daily_if_match(now, "daily_report", SETTINGS.daily_report_hour, 0, self._enqueue_daily_report, weekday_only=False)

    def _run_daily_if_match(
        self,
        now: datetime,
        key: str,
        hour: int,
        minute: int,
        callback: callable,
        *,
        weekday_only: bool,
    ) -> None:
        if now.hour == hour and now.minute == minute:
            if weekday_only and SETTINGS.scheduler_skip_weekends and now.weekday() >= 5:
                return
            self._run_once_per_key(now, key, callback, "%Y%m%d")

    def _run_once_per_key(self, now: datetime, key: str, callback: callable, fmt: str) -> None:
        token = now.strftime(fmt)
        if self.last_tick.get(key) == token:
            return
        callback()
        self.last_tick[key] = token

    def _enqueue_stock_sync(self) -> None:
        event_id = f"evt-stock-{datetime.now(self.tz).strftime('%Y%m%d%H%M')}"
        self.app.db.store_event(event_id=event_id, source_system="scheduler", payload={"job": "stock_sync"}, status="PENDING")
        self.app.queue.enqueue(ERP_QUEUE, "stock_sync", {"event_id": event_id})
        self.logger.info("job_enqueued", extra={"extra": {"job": "stock_sync", "event_id": event_id}})

    def _enqueue_outreach(self) -> None:
        event_id = f"evt-outreach-{datetime.now(self.tz).strftime('%Y%m%d')}"
        self.app.db.store_event(event_id=event_id, source_system="scheduler", payload={"job": "daily_outreach"}, status="PENDING")
        self.app.queue.enqueue(
            CAMPAIGN_QUEUE,
            "outreach_campaign",
            {"event_id": event_id, "campaign_name": "daily_outreach", "product_category": "parts"},
        )
        self.logger.info("job_enqueued", extra={"extra": {"job": "daily_outreach", "event_id": event_id}})

    def _enqueue_followup(self) -> None:
        event_id = f"evt-followup-{datetime.now(self.tz).strftime('%Y%m%d')}"
        self.app.db.store_event(event_id=event_id, source_system="scheduler", payload={"job": "follow_up"}, status="PENDING")
        self.app.queue.enqueue(CAMPAIGN_QUEUE, "follow_up", {"event_id": event_id})
        self.logger.info("job_enqueued", extra={"extra": {"job": "follow_up", "event_id": event_id}})

    def _enqueue_telemetry(self) -> None:
        event_id = f"evt-telemetry-{datetime.now(self.tz).strftime('%Y%m%d')}"
        self.app.db.store_event(event_id=event_id, source_system="scheduler", payload={"job": "telemetry"}, status="PENDING")
        self.app.queue.enqueue(CAMPAIGN_QUEUE, "telemetry_activation", {"event_id": event_id})
        self.logger.info("job_enqueued", extra={"extra": {"job": "telemetry", "event_id": event_id}})

    def _enqueue_daily_report(self) -> None:
        event_id = f"evt-report-{datetime.now(self.tz).strftime('%Y%m%d')}"
        self.app.db.store_event(event_id=event_id, source_system="scheduler", payload={"job": "daily_report"}, status="PENDING")
        self.app.queue.enqueue(REPORT_QUEUE, "daily_report", {"event_id": event_id})
        self.logger.info("job_enqueued", extra={"extra": {"job": "daily_report", "event_id": event_id}})

    def _should_skip_for_calendar(self, target: date) -> bool:
        if not SETTINGS.scheduler_skip_holidays:
            return False
        holidays = {item.strip() for item in SETTINGS.scheduler_holidays_csv.split(",") if item.strip()}
        return target.isoformat() in holidays


def run_scheduler() -> None:
    app = create_app()
    scheduler = Scheduler(app)
    scheduler.run_forever()
