from __future__ import annotations

from pathlib import Path
import sys

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "src"))

from agent_system.channels.openlines_channel import OpenLinesChannel
from agent_system.services.campaign import CampaignService


class _Db:
    def __init__(self) -> None:
        self.last_status = None

    def enabled_offer_rules(self):
        return [{"offer_template": "template"}]

    def create_campaign(self, **kwargs):
        _ = kwargs

    def active_machine_ownerships(self):
        return [{"client_id": "C1", "machine_id": "M1", "phone": "+552222", "state": "MT", "model": "CASE"}]

    def upsert_campaign_target(self, **kwargs):
        _ = kwargs

    def campaign_targets(self, *, campaign_id: str, statuses=("NEW", "PENDING", "RETRY")):
        _ = campaign_id, statuses
        return [{"id": 2, "phone": "+552222", "client_id": "C1", "machine_id": "M1"}]

    def is_opted_out(self, phone: str):
        _ = phone
        return False

    def idempotency_acquire(self, key: str, operation: str):
        _ = key, operation
        return True

    def update_campaign_target_attempt(self, *, target_id: int, status: str, result: str):
        _ = target_id, result
        self.last_status = status

    def log_campaign(self, item):
        _ = item

    def mark_contacted(self, phone: str):
        _ = phone

    def upsert_offer_rule(self, rule):
        _ = rule


class _Bitrix:
    def __init__(self) -> None:
        self.tasks: list[dict] = []

    def find_crm_entity_by_phone(self, phone: str, *, event_id=None):
        _ = phone, event_id
        return {"crm_entity_type": "CONTACT", "crm_entity_id": "123"}

    def resolve_openlines_dialog(self, *, crm_entity_type: str, crm_entity_id: str, event_id=None):
        _ = crm_entity_type, crm_entity_id, event_id
        return "chat123"

    def send_openlines_message(self, crm_entity_type: str, crm_entity_id: str, message: str, **kwargs):
        _ = crm_entity_type, crm_entity_id, message, kwargs
        raise RuntimeError("bitrix down")

    def create_task(self, title: str, summary: str, context: dict, idempotency_key=None):
        _ = title, summary, idempotency_key
        self.tasks.append(context)
        return {"ok": True}

    def create_activity(self, customer_id: str, note: str, idempotency_key=None):
        _ = customer_id, note, idempotency_key
        return {"ok": True}


class _Llm:
    def campaign_message(self, *, customer_name: str, machine_type: str | None, category: str | None):
        _ = customer_name, machine_type, category
        return "msg"


def test_channel_failure_handoff() -> None:
    db = _Db()
    bitrix = _Bitrix()
    channel = OpenLinesChannel(bitrix)  # type: ignore[arg-type]
    service = CampaignService(db=db, bitrix=bitrix, llm_router=_Llm(), channel=channel)  # type: ignore[arg-type]
    result = service.run_outreach(campaign_name="daily", region="MT")
    assert result["sent"] == 0
    assert db.last_status == "HANDOFF_REQUIRED"
    assert bitrix.tasks
    packet = bitrix.tasks[0]
    assert "customer_phone" in packet
    assert "crm_entity_id" in packet
    assert "campaign_name" in packet
    assert "message_attempted" in packet
    assert "reason" in packet
    assert "timestamp" in packet
