from __future__ import annotations

import json
from dataclasses import dataclass
from typing import Any

from ..db import Database, utc_now_iso
from ..integrations.bitrix import BitrixClient


HANDOFF_KEYWORDS = {"negotiation", "discount", "complaint", "financing", "problem", "angry"}


@dataclass
class HandoffService:
    db: Database
    bitrix: BitrixClient

    def should_handoff(self, message: str) -> bool:
        normalized = message.lower()
        return any(word in normalized for word in HANDOFF_KEYWORDS)

    def create_handoff(
        self,
        *,
        customer_id: str | None,
        customer_name: str,
        phone: str,
        machine_model: str,
        requested_product: str,
        summary: str,
        transcript: list[dict[str, Any]],
        stock_snapshot_time: str | None = None,
        next_steps: list[str] | None = None,
        reason: str = "human_request",
    ) -> dict[str, Any]:
        packet = {
            "customer_name": customer_name,
            "customer_id": customer_id,
            "phone": phone,
            "machine_model": machine_model,
            "requested_product": requested_product,
            "summary": summary,
            "last_messages_summary": [item.get("message", "") for item in transcript[-6:]],
            "stock_snapshot_time": stock_snapshot_time,
            "next_recommended_steps": next_steps or ["Confirm client intent", "Validate stock", "Proceed with proposal"],
            "reason": reason,
        }
        task_title = f"Handoff: {customer_name} ({phone})"
        task_summary = f"{summary} | Product: {requested_product} | Model: {machine_model}"
        self.db.create_sales_task(
            {
                "customer_id": customer_id,
                "phone": phone,
                "title": task_title,
                "summary": task_summary,
                "context_json": json.dumps(packet),
                "status": "OPEN",
                "created_at": utc_now_iso(),
            }
        )
        key = f"crm:create_task:{phone}:{hash(task_summary)}"
        self.db.idempotency_acquire(key, "bitrix.create_task")
        self.bitrix.create_task(task_title, task_summary, packet, idempotency_key=key)
        self.db.set_conversation_handoff_state(conversation_id=phone, customer_id=customer_id, packet=packet)
        return {"handoff": True, "title": task_title, "summary": task_summary, "context": packet}
