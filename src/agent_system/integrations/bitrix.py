from __future__ import annotations

import json
import time
from dataclasses import dataclass
from http import HTTPStatus
from typing import Any
from urllib import error, request

from ..config import SETTINGS
from ..infra.observability import get_logger
from ..infra.security import sanitize_message


class IntegrationError(RuntimeError):
    pass


@dataclass
class BitrixClient:
    timeout_seconds: float = 20.0
    max_retries: int = 3

    def __post_init__(self) -> None:
        self.logger = get_logger("integrations.bitrix")
        self.webhook_base_url = SETTINGS.bitrix_webhook_base_url.rstrip("/")
        self.oauth_token = SETTINGS.bitrix_oauth_token
        self.send_mode = SETTINGS.bitrix_openlines_send_mode

    def fetch_deal(self, deal_id: str, *, event_id: str | None = None) -> dict[str, Any]:
        payload = self._call("crm.deal.get", {"id": deal_id}, event_id=event_id)
        return payload.get("result", {})

    def fetch_contact(self, contact_id: str, *, event_id: str | None = None) -> dict[str, Any]:
        payload = self._call("crm.contact.get", {"id": contact_id}, event_id=event_id)
        return payload.get("result", {})

    def fetch_company(self, company_id: str, *, event_id: str | None = None) -> dict[str, Any]:
        payload = self._call("crm.company.get", {"id": company_id}, event_id=event_id)
        return payload.get("result", {})

    def find_crm_entity_by_phone(self, phone: str, *, event_id: str | None = None) -> dict[str, Any] | None:
        payload = self._call(
            "crm.duplicate.findbycomm",
            {"type": "PHONE", "values": [phone], "entity_type": ["CONTACT", "COMPANY"]},
            event_id=event_id,
        )
        result = payload.get("result", {}) if isinstance(payload.get("result"), dict) else {}
        contacts = result.get("CONTACT", [])
        companies = result.get("COMPANY", [])
        if contacts:
            return {"crm_entity_type": "CONTACT", "crm_entity_id": str(contacts[0])}
        if companies:
            return {"crm_entity_type": "COMPANY", "crm_entity_id": str(companies[0])}
        return None

    def resolve_openlines_dialog(self, *, crm_entity_type: str, crm_entity_id: str, event_id: str | None = None) -> str | None:
        if SETTINGS.dry_run:
            return f"chat_{crm_entity_type.lower()}_{crm_entity_id}"
        payload = self._call(
            "imopenlines.dialog.get",
            {"CRM_ENTITY_TYPE": crm_entity_type, "CRM_ENTITY": crm_entity_id},
            event_id=event_id,
        )
        result = payload.get("result", {})
        if isinstance(result, dict):
            return result.get("DIALOG_ID")
        return None

    def upsert_activity(self, deal_id: str, message: str, activity_type: str = "COMMENT", *, event_id: str | None = None) -> dict[str, Any]:
        payload = {
            "fields": {
                "OWNER_TYPE_ID": 2,
                "OWNER_ID": str(deal_id),
                "TYPE_ID": 1 if activity_type == "CALL" else 4,
                "SUBJECT": "Automation Activity",
                "DESCRIPTION": sanitize_message(message),
                "DESCRIPTION_TYPE": 3,
                "RESPONSIBLE_ID": SETTINGS.bitrix_default_assignee_id or None,
            }
        }
        return self._call("crm.activity.add", payload, event_id=event_id)

    def update_deal_fields(self, deal_id: str, fields_dict: dict[str, Any], *, event_id: str | None = None) -> dict[str, Any]:
        payload = {"id": str(deal_id), "fields": fields_dict}
        return self._call("crm.deal.update", payload, event_id=event_id)

    def send_openlines_message(
        self,
        crm_entity_type: str,
        crm_entity_id: str,
        message: str,
        *,
        dialog_id: str | None = None,
        event_id: str | None = None,
    ) -> dict[str, Any]:
        message = sanitize_message(message)
        if self.send_mode == "bot_message_add":
            if not SETTINGS.bitrix_bot_id:
                raise IntegrationError("BITRIX_BOT_ID is required for bot_message_add mode")
            dialog = dialog_id or f"{crm_entity_type.lower()}|{crm_entity_id}"
            payload = {
                "BOT_ID": SETTINGS.bitrix_bot_id,
                "DIALOG_ID": dialog,
                "MESSAGE": message,
            }
            return self._call("imbot.message.add", payload, event_id=event_id)

        payload = {
            "CRM_ENTITY_TYPE": crm_entity_type,
            "CRM_ENTITY": str(crm_entity_id),
            "MESSAGE": message,
        }
        return self._call("imopenlines.crm.message.add", payload, event_id=event_id)

    def send_whatsapp(
        self,
        phone: str,
        message: str,
        campaign_id: str | None = None,
        idempotency_key: str | None = None,
        *,
        event_id: str | None = None,
    ) -> dict[str, Any]:
        body = f"{sanitize_message(message)}"
        if campaign_id:
            body = f"[{campaign_id}] {body}"
        if idempotency_key:
            body = f"{body}\nref:{idempotency_key[:32]}"
        payload = {
            "fields": {
                "SUBJECT": "WhatsApp Outbound",
                "DESCRIPTION": body,
                "DESCRIPTION_TYPE": 3,
                "COMMUNICATIONS": [{"VALUE": phone, "ENTITY_TYPE_ID": 3, "TYPE": "PHONE"}],
            }
        }
        return self._call("crm.activity.add", payload, event_id=event_id)

    def create_activity(self, customer_id: str, note: str, idempotency_key: str | None = None) -> dict[str, Any]:
        return self.upsert_activity(customer_id, f"{note}\nref={idempotency_key or 'none'}")

    def create_task(
        self,
        title: str,
        summary: str,
        context: dict[str, Any],
        idempotency_key: str | None = None,
    ) -> dict[str, Any]:
        payload = {
            "fields": {
                "TITLE": title,
                "DESCRIPTION": sanitize_message(f"{summary}\ncontext={json.dumps(context, ensure_ascii=True)}"),
                "RESPONSIBLE_ID": SETTINGS.bitrix_default_assignee_id,
            }
        }
        if idempotency_key:
            payload["fields"]["UF_CRM_TASK_DEDUP"] = idempotency_key
        return self._call("tasks.task.add", payload)

    def update_deal(self, deal_id: str, fields: dict[str, Any], idempotency_key: str | None = None) -> dict[str, Any]:
        clean = dict(fields)
        if idempotency_key:
            clean["UF_CRM_SYNC_KEY"] = idempotency_key
        return self.update_deal_fields(deal_id, clean)

    def post_daily_report(self, report: dict[str, Any], idempotency_key: str | None = None) -> None:
        report_message = json.dumps(report, ensure_ascii=True)
        mode = SETTINGS.bitrix_report_mode
        if mode == "openline":
            if not SETTINGS.bitrix_report_openline_entity_id:
                raise IntegrationError("BITRIX_REPORT_OPENLINE_ENTITY_ID is required for openline mode")
            self.send_openlines_message(
                SETTINGS.bitrix_report_openline_entity_type,
                SETTINGS.bitrix_report_openline_entity_id,
                report_message,
            )
            return
        if mode == "task":
            self.create_task("Daily Operations Report", report_message, report, idempotency_key=idempotency_key)
            return
        if not SETTINGS.bitrix_report_control_deal_id:
            raise IntegrationError("BITRIX_REPORT_CONTROL_DEAL_ID is required for activity mode")
        self.upsert_activity(
            SETTINGS.bitrix_report_control_deal_id,
            f"Daily report: {report_message}",
            activity_type="COMMENT",
        )

    def _call(self, method: str, payload: dict[str, Any], *, event_id: str | None = None) -> dict[str, Any]:
        if SETTINGS.dry_run:
            self.logger.info(
                "bitrix_dry_run",
                extra={"extra": {"event_id": event_id, "method": method, "payload": payload}},
            )
            return {"result": {"dry_run": True, "method": method, "payload": payload}}
        if not self.webhook_base_url and not self.oauth_token:
            raise IntegrationError("Bitrix credentials are not configured")
        url = self._method_url(method)
        headers = {"Content-Type": "application/json"}
        if self.oauth_token:
            headers["Authorization"] = f"Bearer {self.oauth_token}"
        body = json.dumps(payload).encode("utf-8")
        last_exc: Exception | None = None
        for attempt in range(self.max_retries + 1):
            started = time.monotonic()
            try:
                req = request.Request(url=url, data=body, headers=headers, method="POST")
                with request.urlopen(req, timeout=self.timeout_seconds) as resp:
                    raw = resp.read().decode("utf-8")
                    data = json.loads(raw) if raw else {}
                    duration_ms = int((time.monotonic() - started) * 1000)
                    self.logger.info(
                        "bitrix_request",
                        extra={
                            "extra": {
                                "event_id": event_id,
                                "method": method,
                                "status_code": resp.status,
                                "duration_ms": duration_ms,
                                "bitrix_error": data.get("error"),
                            }
                        },
                    )
                    if data.get("error"):
                        if self._is_retryable_error(data.get("error", "")) and attempt < self.max_retries:
                            time.sleep(2**attempt)
                            continue
                        raise IntegrationError(f"bitrix_error:{data['error']}")
                    return data
            except error.HTTPError as exc:
                duration_ms = int((time.monotonic() - started) * 1000)
                body_text = exc.read().decode("utf-8", errors="ignore")
                self.logger.error(
                    "bitrix_http_error",
                    extra={
                        "extra": {
                            "event_id": event_id,
                            "method": method,
                            "status_code": exc.code,
                            "duration_ms": duration_ms,
                            "bitrix_error": body_text[:300],
                        }
                    },
                )
                last_exc = exc
                if exc.code in (HTTPStatus.TOO_MANY_REQUESTS, HTTPStatus.BAD_GATEWAY, HTTPStatus.SERVICE_UNAVAILABLE) and attempt < self.max_retries:
                    time.sleep(2**attempt)
                    continue
                break
            except Exception as exc:  # noqa: BLE001
                last_exc = exc
                if attempt < self.max_retries:
                    time.sleep(2**attempt)
                    continue
                break
        raise IntegrationError(f"bitrix_request_failed:{method}") from last_exc

    def _method_url(self, method: str) -> str:
        if self.webhook_base_url:
            return f"{self.webhook_base_url}/{method}"
        if not SETTINGS.bitrix_rest_base_url:
            raise IntegrationError("BITRIX_REST_BASE_URL is required when using OAuth")
        return f"{SETTINGS.bitrix_rest_base_url.rstrip('/')}/{method}"

    @staticmethod
    def _is_retryable_error(error_code: str) -> bool:
        upper = str(error_code).upper()
        return "RATE_LIMIT" in upper or "TOO_MANY" in upper or "TIMEOUT" in upper
