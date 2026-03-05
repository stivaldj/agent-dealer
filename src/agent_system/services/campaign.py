from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime
from typing import Any
from uuid import uuid4
from zoneinfo import ZoneInfo

from ..channels.openlines_channel import InboundContext, OpenLinesChannel
from ..config import SETTINGS
from ..db import Database, utc_now_iso
from ..infra.llm_router import LLMRouter
from ..infra.security import sanitize_message
from ..integrations.bitrix import BitrixClient


@dataclass
class CampaignService:
    db: Database
    bitrix: BitrixClient
    llm_router: LLMRouter
    channel: OpenLinesChannel

    def run_outreach(
        self,
        *,
        campaign_name: str,
        machine_type: str | None = None,
        region: str | None = None,
        product_category: str | None = None,
        wave: str = "initial",
    ) -> dict[str, Any]:
        campaign_id = f"CMP-{uuid4().hex[:8].upper()}"
        rules = self.db.enabled_offer_rules()
        if not rules:
            self.db.upsert_offer_rule(
                {
                    "id": "default-parts",
                    "rule_type": "machine",
                    "predicate": {"machine_model_contains": machine_type or ""},
                    "offer_template": "Temos ofertas para {machine_model}. Responda com SKU e quantidade.",
                    "sku_list": ["ABC123"],
                    "priority": 100,
                    "enabled": True,
                }
            )
            rules = self.db.enabled_offer_rules()

        self.db.create_campaign(
            campaign_id=campaign_id,
            name=campaign_name,
            campaign_type="OUTREACH",
            status="RUNNING",
            schedule=f"wave:{wave}",
            segment={"machine_type": machine_type, "region": region, "category": product_category},
            template=rules[0]["offer_template"],
        )

        selected_targets = self._build_targets(campaign_id=campaign_id, rules=rules, machine_type=machine_type, region=region)
        pending = self.db.campaign_targets(campaign_id=campaign_id)
        sent = 0
        skipped_optout = 0
        skipped_frequency = 0

        for target in pending:
            phone = target["phone"]
            if self.db.is_opted_out(phone):
                skipped_optout += 1
                self.db.update_campaign_target_attempt(target_id=target["id"], status="SKIPPED", result="opt_out")
                continue
            message = self.llm_router.campaign_message(
                customer_name=str(target["client_id"]),
                machine_type=machine_type,
                category=product_category,
            )
            if SETTINGS.outbound_include_opt_out_text and "STOP" not in message.upper():
                message = f"{message} Responda STOP para não receber novas mensagens."
            message = sanitize_message(message)
            dedupe_key = f"crm:campaign:{campaign_id}:{phone}:{wave}"
            if not self.db.idempotency_acquire(dedupe_key, "bitrix.send_whatsapp"):
                self.db.update_campaign_target_attempt(target_id=target["id"], status="SKIPPED", result="duplicate")
                continue
            resolved = self.bitrix.find_crm_entity_by_phone(phone)
            if not resolved:
                self.bitrix.create_task(
                    title=f"Manual campaign start required - {phone}",
                    summary=f"Campaign {campaign_id} could not send because no active OpenLines chat was found.",
                    context={"campaign_id": campaign_id, "phone": phone, "target_id": target["id"]},
                )
                self.db.update_campaign_target_attempt(target_id=target["id"], status="NEEDS_MANUAL_START", result="missing_chat")
                skipped_frequency += 1
                continue
            context = InboundContext(
                phone=phone,
                message="",
                crm_entity_type=resolved["crm_entity_type"],
                crm_entity_id=resolved["crm_entity_id"],
            )
            context.dialog_id = self.channel.resolve_openlines_dialog(
                crm_entity_type=context.crm_entity_type or "",
                crm_entity_id=context.crm_entity_id or "",
            )
            if not context.dialog_id:
                self.bitrix.create_task(
                    title=f"Manual campaign start required - {phone}",
                    summary=f"Campaign {campaign_id} requires manual start (no OpenLines dialog).",
                    context={"campaign_id": campaign_id, "phone": phone, "target_id": target["id"]},
                )
                self.db.update_campaign_target_attempt(target_id=target["id"], status="NEEDS_MANUAL_START", result="no_dialog")
                skipped_frequency += 1
                continue
            send_result = self.channel.send_message(context, message, campaign_name=campaign_name)
            if not send_result.ok:
                handoff_packet = {
                    "customer_phone": phone,
                    "crm_entity_id": context.crm_entity_id,
                    "campaign_name": campaign_name,
                    "message_attempted": message,
                    "reason": send_result.reason or "send_failed",
                    "timestamp": utc_now_iso(),
                }
                self.bitrix.create_task(
                    title=f"Channel failure handoff - {phone}",
                    summary=f"Campaign {campaign_id} channel failure: {handoff_packet['reason']}",
                    context=handoff_packet,
                )
                self.db.update_campaign_target_attempt(target_id=target["id"], status="HANDOFF_REQUIRED", result=handoff_packet["reason"])
                skipped_frequency += 1
                continue
            self.bitrix.create_activity(target["client_id"], f"Campaign {campaign_id} sent to {phone} message_id={send_result.message_id}")
            self.db.log_campaign(
                {
                    "campaign_id": campaign_id,
                    "customer_id": target["client_id"],
                    "phone": phone,
                    "message_sent_at": utc_now_iso(),
                    "response_at": None,
                    "response": None,
                    "outcome": "sent",
                    "wave": wave,
                }
            )
            self.db.update_campaign_target_attempt(target_id=target["id"], status="SENT", result="sent")
            self.db.mark_contacted(phone)
            sent += 1

        return {
            "campaign_id": campaign_id,
            "campaign_name": campaign_name,
            "targets_created": selected_targets,
            "sent": sent,
            "skipped_optout": skipped_optout,
            "skipped_frequency": skipped_frequency,
        }

    def run_followup(self) -> dict[str, Any]:
        today = datetime.now(ZoneInfo(SETTINGS.tz)).date().isoformat()
        entries = [e for e in self.db.campaign_entries() if e["response_at"] is None and str(e["message_sent_at"]).startswith(today)]
        followups = 0
        for entry in entries:
            if self.db.is_opted_out(entry["phone"]):
                continue
            dedupe_key = f"crm:followup:{entry['campaign_id']}:{entry['phone']}:{today}"
            if not self.db.idempotency_acquire(dedupe_key, "bitrix.send_whatsapp"):
                continue
            self.bitrix.send_whatsapp(
                entry["phone"],
                "Checking in on the offer we sent earlier today. Responda STOP para não receber novas mensagens.",
                idempotency_key=dedupe_key,
            )
            followups += 1
        return {"followups_sent": followups}

    def _build_targets(
        self,
        *,
        campaign_id: str,
        rules: list[dict[str, Any]],
        machine_type: str | None,
        region: str | None,
    ) -> int:
        _ = rules
        count = 0
        for ownership in self.db.active_machine_ownerships():
            if machine_type and machine_type.lower() not in str(ownership.get("model", "")).lower():
                continue
            if region and region.lower() != str(ownership.get("state", "")).lower():
                continue
            self.db.upsert_campaign_target(
                campaign_id=campaign_id,
                client_id=ownership["client_id"],
                machine_id=ownership["machine_id"],
                phone=ownership["phone"],
                status="NEW",
            )
            count += 1
        return count
