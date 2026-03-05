from __future__ import annotations

from dataclasses import dataclass
from typing import Any

from ..config import SETTINGS
from ..infra.observability import get_logger
from ..infra.security import GlobalOutboundLimiter, sanitize_message
from ..integrations.bitrix import BitrixClient


@dataclass
class InboundContext:
    phone: str
    message: str
    crm_entity_type: str | None = None
    crm_entity_id: str | None = None
    dialog_id: str | None = None


@dataclass
class ChannelSendResult:
    ok: bool
    message_id: str | None
    reason: str | None = None


class OpenLinesChannel:
    def __init__(self, bitrix: BitrixClient) -> None:
        self.bitrix = bitrix
        self.logger = get_logger("channels.openlines")
        self.rate_limiter = GlobalOutboundLimiter()

    def resolve_inbound_context(self, payload: dict[str, Any], *, event_id: str | None = None) -> InboundContext:
        phone = str(payload.get("phone") or payload.get("PHONE") or "").strip()
        message = str(payload.get("message") or payload.get("MESSAGE") or "").strip()
        crm_entity_type = payload.get("crm_entity_type") or payload.get("CRM_ENTITY_TYPE")
        crm_entity_id = payload.get("crm_entity_id") or payload.get("CRM_ENTITY_ID")
        dialog_id = payload.get("dialog_id") or payload.get("DIALOG_ID")

        if not (crm_entity_type and crm_entity_id) and phone:
            try:
                resolved = self.bitrix.find_crm_entity_by_phone(phone, event_id=event_id)
            except Exception:  # noqa: BLE001
                resolved = None
            if resolved:
                crm_entity_type = resolved["crm_entity_type"]
                crm_entity_id = resolved["crm_entity_id"]

        if not dialog_id and crm_entity_type and crm_entity_id:
            try:
                dialog_id = self.bitrix.resolve_openlines_dialog(
                    crm_entity_type=str(crm_entity_type),
                    crm_entity_id=str(crm_entity_id),
                    event_id=event_id,
                )
            except Exception:  # noqa: BLE001
                dialog_id = None

        return InboundContext(
            phone=phone,
            message=message,
            crm_entity_type=str(crm_entity_type) if crm_entity_type else None,
            crm_entity_id=str(crm_entity_id) if crm_entity_id else None,
            dialog_id=str(dialog_id) if dialog_id else None,
        )

    def send_message(
        self,
        context: InboundContext,
        text: str,
        *,
        campaign_name: str | None = None,
        event_id: str | None = None,
    ) -> ChannelSendResult:
        message = sanitize_message(text)
        if SETTINGS.outbound_include_opt_out_text and campaign_name and "STOP" not in message.upper():
            message = f"{message} Responda STOP para não receber novas mensagens."

        allowed, reason = self.rate_limiter.allow_send(
            phone=context.phone,
            campaign_name=campaign_name or "conversation",
            daily_cap=SETTINGS.outbound_daily_cap,
            weekly_campaign_cap=SETTINGS.outbound_campaign_weekly_cap,
        )
        if not allowed:
            self.logger.info("openlines_blocked", extra={"extra": {"phone": context.phone, "reason": reason}})
            return ChannelSendResult(ok=False, message_id=None, reason=reason)

        if context.crm_entity_type and context.crm_entity_id:
            dialog_id = context.dialog_id or self.resolve_openlines_dialog(
                crm_entity_type=context.crm_entity_type,
                crm_entity_id=context.crm_entity_id,
                event_id=event_id,
            )
            if not dialog_id:
                return ChannelSendResult(ok=False, message_id=None, reason="no_dialog")
            try:
                result = self.bitrix.send_openlines_message(
                    crm_entity_type=context.crm_entity_type,
                    crm_entity_id=context.crm_entity_id,
                    message=message,
                    dialog_id=dialog_id,
                    event_id=event_id,
                )
            except Exception:  # noqa: BLE001
                return ChannelSendResult(ok=False, message_id=None, reason="send_failed")
            msg_id = str(result.get("result", {}).get("message_id") or result.get("result", {}).get("ID") or "")
            if not msg_id:
                return ChannelSendResult(ok=False, message_id=None, reason="send_failed")
            return ChannelSendResult(ok=True, message_id=msg_id)

        return ChannelSendResult(ok=False, message_id=None, reason="no_crm_entity")

    def resolve_openlines_dialog(
        self,
        *,
        crm_entity_type: str,
        crm_entity_id: str,
        event_id: str | None = None,
    ) -> str | None:
        try:
            return self.bitrix.resolve_openlines_dialog(
                crm_entity_type=crm_entity_type,
                crm_entity_id=crm_entity_id,
                event_id=event_id,
            )
        except Exception:  # noqa: BLE001
            return None
