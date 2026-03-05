from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime
from typing import Any
from zoneinfo import ZoneInfo

from ..config import SETTINGS
from ..db import Database, utc_now_iso
from ..infra.security import GlobalOutboundLimiter
from ..integrations.bitrix import BitrixClient


@dataclass
class TelemetryService:
    db: Database
    bitrix: BitrixClient

    def __post_init__(self) -> None:
        self.rate_limiter = GlobalOutboundLimiter()

    def run_activation_campaign(self) -> dict[str, int]:
        self._ensure_targets()
        targets = self.db.telemetry_targets()
        contacted = 0
        for target in targets:
            phone = target["phone"]
            if self.db.is_opted_out(phone):
                continue
            allowed, _ = self.rate_limiter.allow_send(
                phone=phone,
                campaign_name="telemetry_activation",
                daily_cap=SETTINGS.outbound_daily_cap,
                weekly_campaign_cap=SETTINGS.outbound_campaign_weekly_cap,
            )
            if not allowed:
                continue
            required_fields = target.get("required_fields_json") or {}
            required = ", ".join(required_fields.keys()) or SETTINGS.telemetry_required_fields
            message = (
                "Para ativar a telemetria, responda neste formato campo=valor. "
                f"Campos obrigatorios: {required}. Responda STOP para opt-out."
            )
            key = f"crm:telemetry:{target['id']}:{phone}"
            if not self.db.idempotency_acquire(key, "bitrix.send_whatsapp"):
                continue
            self.bitrix.send_whatsapp(phone, message, campaign_id="telemetry_activation", idempotency_key=key)
            self.db.update_telemetry_target_progress(
                target_id=target["id"],
                collected_fields=target.get("collected_fields_json") or {},
                status="PENDING",
            )
            self.db.log_telemetry_activation(
                {
                    "machine_id": str(target.get("machine_id") or "unknown"),
                    "customer_id": target["customer_id"],
                    "contacted_at": utc_now_iso(),
                    "status": "CONTACTED",
                    "response": None,
                }
            )
            self.bitrix.create_activity(target["customer_id"], f"Telemetry outreach sent to {phone}")
            contacted += 1
        return {"contacted": contacted}

    def telemetry_daily_progress(self) -> dict[str, Any]:
        today = datetime.now(ZoneInfo(SETTINGS.tz)).date().isoformat()
        logs = [item for item in self.db.telemetry_logs() if str(item["contacted_at"]).startswith(today)]
        targets = self.db.telemetry_targets()
        pending = len([t for t in targets if t["status"] in ("NEW", "PENDING")])
        completed = len([t for t in targets if t["status"] == "COMPLETE"])
        activated = len([t for t in targets if t["status"] == "ACTIVATED"])
        payload = {
            "date": today,
            "contacts_attempted": len(logs),
            "responses": len([l for l in logs if l.get("response")]),
            "complete_data": completed,
            "activated": activated,
            "pending": pending,
            "crm_evidence": [f"telemetry_target:{t['id']}:{t['status']}" for t in targets[:20]],
        }
        self.bitrix.create_activity(SETTINGS.bitrix_report_control_deal_id or "telemetry", f"Telemetry daily progress {payload}")
        return payload

    def _ensure_targets(self) -> None:
        existing = self.db.telemetry_targets()
        if existing:
            return
        required = {field.strip(): "" for field in SETTINGS.telemetry_required_fields.split(",") if field.strip()}
        for machine in self.db.machines_without_telemetry():
            self.db.upsert_telemetry_target(
                customer_id=machine["client_id"],
                phone=machine["phone"],
                machine_id=machine["id"],
                client_id=machine["client_id"],
                required_fields=required,
                status="NEW",
            )
