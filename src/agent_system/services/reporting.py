from __future__ import annotations

from dataclasses import dataclass
from datetime import date, datetime
from typing import Any
from zoneinfo import ZoneInfo

from ..config import SETTINGS
from ..db import Database
from ..integrations.bitrix import BitrixClient


@dataclass
class ReportingService:
    db: Database
    bitrix: BitrixClient

    def generate_daily(self, for_date: date | None = None) -> dict[str, Any]:
        target_date = for_date or datetime.now(ZoneInfo(SETTINGS.tz)).date()
        prefix = target_date.isoformat()
        campaign_entries = [e for e in self.db.campaign_entries() if str(e["message_sent_at"]).startswith(prefix)]
        replies = [e for e in campaign_entries if e["response_at"]]
        deals = self.db.deals_by_date_prefix(prefix)
        won = [d for d in deals if d["status"] == "WON"]
        telemetry = [t for t in self.db.telemetry_logs() if str(t["contacted_at"]).startswith(prefix)]
        telemetry_targets = self.db.telemetry_targets()
        report = {
            "date": prefix,
            "contacts_made": len(campaign_entries),
            "replies": len(replies),
            "deals_created": len(deals),
            "deals_won": len(won),
            "telemetry_activations": len(telemetry),
            "telemetry_complete_data": len([t for t in telemetry_targets if t["status"] == "COMPLETE"]),
            "telemetry_activated": len([t for t in telemetry_targets if t["status"] == "ACTIVATED"]),
            "crm_evidence": [f"telemetry_target:{t['id']}:{t['status']}" for t in telemetry_targets[:20]],
        }
        self.db.save_daily_report(prefix, report)
        key = f"crm:post_report:{prefix}"
        self.db.idempotency_acquire(key, "bitrix.post_daily_report")
        self.bitrix.post_daily_report(report, idempotency_key=key)
        return report
