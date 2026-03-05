from __future__ import annotations

import json
from http import HTTPStatus
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from urllib.parse import parse_qs, urlparse
from uuid import uuid4

from .app import create_app
from .config import SETTINGS
from .infra.observability import get_logger
from .infra.queue import CAMPAIGN_QUEUE, CONVERSATION_QUEUE, ERP_QUEUE, REPORT_QUEUE
from .infra.security import RateLimiter, validate_json_fields, validate_schema, verify_signature

APP = create_app()
LOGGER = get_logger("api")
RATE_LIMITER = RateLimiter(limit_per_minute=SETTINGS.rate_limit_per_minute)


class AgentRequestHandler(BaseHTTPRequestHandler):
    server_version = "AgentHTTP/1.0"

    def do_GET(self) -> None:  # noqa: N802
        parsed = urlparse(self.path)
        if parsed.path == "/health":
            self._send(HTTPStatus.OK, {"status": "ok"})
            return
        if parsed.path == "/stock":
            sku = parse_qs(parsed.query).get("sku", [None])[0]
            if not sku:
                self._send(HTTPStatus.BAD_REQUEST, {"error": "sku query param required"})
                return
            self._send(HTTPStatus.OK, APP.stock.query_stock(sku))
            return
        self._send(HTTPStatus.NOT_FOUND, {"error": "not_found"})

    def do_POST(self) -> None:  # noqa: N802
        if not RATE_LIMITER.allow(self.client_address[0]):
            self._send(HTTPStatus.TOO_MANY_REQUESTS, {"error": "rate_limited"})
            return

        parsed = urlparse(self.path)
        try:
            raw_body, body = self._json_body_with_raw()
        except ValueError as exc:
            code = HTTPStatus.REQUEST_ENTITY_TOO_LARGE if "too_large" in str(exc) else HTTPStatus.BAD_REQUEST
            self._send(code, {"error": str(exc)})
            return

        if parsed.path.startswith("/webhooks/"):
            signature = self.headers.get("X-Signature")
            if not verify_signature(SETTINGS.webhook_secret, raw_body, signature):
                self._send(HTTPStatus.UNAUTHORIZED, {"error": "invalid_signature"})
                return

        if parsed.path == "/webhooks/bitrix/message":
            payload = body.get("data") if isinstance(body.get("data"), dict) else body
            ok, error = validate_json_fields(payload, ["phone", "message"])
            if not ok:
                self._send(HTTPStatus.BAD_REQUEST, {"error": error})
                return
            ok_schema, schema_error = validate_schema(payload, {"phone": str, "message": str})
            if not ok_schema:
                self._send(HTTPStatus.BAD_REQUEST, {"error": schema_error})
                return
            event_id = body.get("event_id") or f"evt-msg-{uuid4().hex}"
            context = APP.channel.resolve_inbound_context(payload, event_id=event_id)
            normalized = {
                "phone": context.phone,
                "message": context.message,
                "crm_entity_type": context.crm_entity_type,
                "crm_entity_id": context.crm_entity_id,
                "dialog_id": context.dialog_id,
            }
            inserted = APP.db.store_event(event_id=event_id, source_system="bitrix", payload=payload, status="PENDING")
            if inserted:
                APP.queue.enqueue(CONVERSATION_QUEUE, "incoming_message", {**normalized, "event_id": event_id})
            self._send(
                HTTPStatus.ACCEPTED,
                {"status": "accepted", "event_id": event_id, "inserted": inserted, "normalized": normalized},
            )
            return

        if parsed.path == "/webhooks/bitrix/deal-won":
            ok, error = validate_json_fields(body, ["event_id", "deal_id"])
            if not ok:
                self._send(HTTPStatus.BAD_REQUEST, {"error": error})
                return
            ok_schema, schema_error = validate_schema(body, {"event_id": str, "deal_id": str})
            if not ok_schema:
                self._send(HTTPStatus.BAD_REQUEST, {"error": schema_error})
                return
            inserted = APP.db.store_event(
                event_id=body["event_id"],
                source_system="bitrix",
                payload={"deal_id": body["deal_id"]},
                status="PENDING",
            )
            if inserted:
                APP.queue.enqueue(ERP_QUEUE, "deal_won", body)
            self._send(HTTPStatus.ACCEPTED, {"status": "accepted", "event_id": body["event_id"], "inserted": inserted})
            return

        if parsed.path == "/campaigns/run":
            event_id = body.get("event_id") or f"evt-campaign-{uuid4().hex}"
            payload = {
                "event_id": event_id,
                "campaign_name": body.get("campaign_name", "daily_outreach"),
                "machine_type": body.get("machine_type"),
                "region": body.get("region"),
                "product_category": body.get("product_category"),
                "wave": body.get("wave", "initial"),
            }
            inserted = APP.db.store_event(event_id=event_id, source_system="api", payload=payload, status="PENDING")
            if inserted:
                APP.queue.enqueue(CAMPAIGN_QUEUE, "outreach_campaign", payload)
            self._send(HTTPStatus.ACCEPTED, {"status": "accepted", "event_id": event_id, "inserted": inserted})
            return

        if parsed.path == "/campaigns/telemetry":
            event_id = body.get("event_id") or f"evt-telemetry-{uuid4().hex}"
            payload = {"event_id": event_id}
            inserted = APP.db.store_event(event_id=event_id, source_system="api", payload=payload, status="PENDING")
            if inserted:
                APP.queue.enqueue(CAMPAIGN_QUEUE, "telemetry_activation", payload)
            self._send(HTTPStatus.ACCEPTED, {"status": "accepted", "event_id": event_id, "inserted": inserted})
            return

        if parsed.path == "/sync/stock":
            event_id = body.get("event_id") or f"evt-stock-{uuid4().hex}"
            payload = {"event_id": event_id}
            inserted = APP.db.store_event(event_id=event_id, source_system="api", payload=payload, status="PENDING")
            if inserted:
                APP.queue.enqueue(ERP_QUEUE, "stock_sync", payload)
            self._send(HTTPStatus.ACCEPTED, {"status": "accepted", "event_id": event_id, "inserted": inserted})
            return

        if parsed.path == "/reports/daily":
            event_id = body.get("event_id") or f"evt-report-{uuid4().hex}"
            payload = {"event_id": event_id}
            inserted = APP.db.store_event(event_id=event_id, source_system="api", payload=payload, status="PENDING")
            if inserted:
                APP.queue.enqueue(REPORT_QUEUE, "daily_report", payload)
            self._send(HTTPStatus.ACCEPTED, {"status": "accepted", "event_id": event_id, "inserted": inserted})
            return

        self._send(HTTPStatus.NOT_FOUND, {"error": "not_found"})

    def _json_body_with_raw(self) -> tuple[bytes, dict]:
        length = int(self.headers.get("Content-Length", "0"))
        if length <= 0:
            return b"{}", {}
        if length > SETTINGS.max_request_body_bytes:
            raise ValueError("request_body_too_large")
        raw = self.rfile.read(length)
        if not raw:
            return b"{}", {}
        return raw, json.loads(raw.decode("utf-8"))

    def _send(self, status: HTTPStatus, payload: dict) -> None:
        encoded = json.dumps(payload).encode("utf-8")
        self.send_response(status.value)
        self.send_header("Content-Type", "application/json")
        self.send_header("Content-Length", str(len(encoded)))
        self.end_headers()
        self.wfile.write(encoded)

    def log_message(self, format: str, *args: object) -> None:
        LOGGER.info("http_access", extra={"extra": {"path": self.path, "client": self.client_address[0]}})


def run_server(host: str = "127.0.0.1", port: int = 8080) -> None:
    server = ThreadingHTTPServer((host, port), AgentRequestHandler)
    LOGGER.info("api_started", extra={"extra": {"host": host, "port": port}})
    server.serve_forever()
