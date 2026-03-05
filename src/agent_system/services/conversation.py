from __future__ import annotations

from dataclasses import dataclass
from typing import Any
from uuid import uuid4

from ..channels.openlines_channel import InboundContext, OpenLinesChannel
from ..db import Database, utc_now_iso
from ..infra.llm_router import LLMRouter
from ..infra.security import sanitize_message
from ..integrations.bitrix import BitrixClient
from .handoff import HandoffService
from .stock import StockService


@dataclass
class ConversationService:
    db: Database
    bitrix: BitrixClient
    stock_service: StockService
    handoff_service: HandoffService
    llm_router: LLMRouter
    channel: OpenLinesChannel

    def handle_incoming(
        self,
        *,
        phone: str,
        message: str,
        crm_entity_type: str | None = None,
        crm_entity_id: str | None = None,
        dialog_id: str | None = None,
        event_id: str | None = None,
    ) -> dict[str, Any]:
        customer = self.db.customer_by_phone(phone)
        customer_id = customer["id"] if customer else None
        state = self.db.conversation_state(phone)
        if state and state.get("last_intent") == "HANDOFF_ACTIVE":
            locked_reply = "Seu atendimento está com o time humano. Aguarde nosso retorno por este mesmo chat."
            self._deliver_reply(
                phone=phone,
                reply=locked_reply,
                crm_entity_type=crm_entity_type,
                crm_entity_id=crm_entity_id,
                dialog_id=dialog_id,
                event_id=event_id,
            )
            return {"reply": locked_reply, "handoff_active": True}

        telemetry_status = self._try_collect_telemetry_fields(phone=phone, customer_id=customer_id, message=message)
        if telemetry_status:
            self._deliver_reply(
                phone=phone,
                reply=telemetry_status["reply"],
                crm_entity_type=crm_entity_type,
                crm_entity_id=crm_entity_id,
                dialog_id=dialog_id,
                event_id=event_id,
            )
            return telemetry_status

        intent = self._detect_intent(message)
        self.db.log_conversation(
            {
                "customer_id": customer_id,
                "phone": phone,
                "direction": "inbound",
                "message": message,
                "intent": intent,
                "handoff": 0,
                "created_at": utc_now_iso(),
            }
        )

        context_summary = self._build_context_summary(state=state, message=message, intent=intent)
        self.db.upsert_conversation_state(
            conversation_id=phone,
            customer_id=customer_id,
            last_intent=intent,
            context_summary=context_summary,
        )

        if message.strip().upper() == "STOP":
            self.db.set_opt_out(phone, "whatsapp_stop")
            reply = "You were opted out. Reply START to re-enable."
            self._log_outbound(customer_id, phone, reply, intent="opt_out")
            self._deliver_reply(
                phone=phone,
                reply=reply,
                crm_entity_type=crm_entity_type,
                crm_entity_id=crm_entity_id,
                dialog_id=dialog_id,
                event_id=event_id,
            )
            return {"reply": reply, "opt_out": True}

        if self.handoff_service.should_handoff(message):
            transcript = self.db.conversation_history(phone)
            payload = self.handoff_service.create_handoff(
                customer_id=customer_id,
                customer_name=customer["name"] if customer else "Unknown",
                phone=phone,
                machine_model="unknown",
                requested_product=self._extract_sku(message) or "unknown",
                summary="Customer needs human support due to sensitive context.",
                transcript=transcript,
            )
            self.db.log_conversation(
                {
                    "customer_id": customer_id,
                    "phone": phone,
                    "direction": "outbound",
                    "message": payload["summary"],
                    "intent": "handoff",
                    "handoff": 1,
                    "created_at": utc_now_iso(),
                }
            )
            handoff_reply = "Encaminhei seu atendimento para um especialista humano. Em breve retornamos."
            self._deliver_reply(
                phone=phone,
                reply=handoff_reply,
                crm_entity_type=crm_entity_type,
                crm_entity_id=crm_entity_id,
                dialog_id=dialog_id,
                event_id=event_id,
            )
            return payload

        if intent == "stock_query":
            sku = self._extract_sku(message)
            if not sku:
                reply = "Please share the SKU so I can check live stock."
                self._log_outbound(customer_id, phone, reply, intent)
                return {"reply": reply}
            stock = self.stock_service.query_stock(sku)
            if stock["total_quantity"] <= 0:
                alternatives = self.stock_service.suggest_alternatives(category="parts")
                top = ", ".join(item["sku"] for item in alternatives[:3]) or "none available"
                reply = f"{sku} is out of stock right now. Alternatives with stock: {top}."
                self._log_outbound(customer_id, phone, reply, "alternative_offer")
                self._deliver_reply(
                    phone=phone,
                    reply=reply,
                    crm_entity_type=crm_entity_type,
                    crm_entity_id=crm_entity_id,
                    dialog_id=dialog_id,
                    event_id=event_id,
                )
                return {"reply": reply, "alternatives": alternatives[:3]}
            reply = f"SKU {sku} has {stock['total_quantity']} units available."
            self._log_outbound(customer_id, phone, reply, intent)
            self._deliver_reply(
                phone=phone,
                reply=reply,
                crm_entity_type=crm_entity_type,
                crm_entity_id=crm_entity_id,
                dialog_id=dialog_id,
                event_id=event_id,
            )
            return {"reply": reply, "stock": stock}

        if intent == "purchase":
            sku = self._extract_sku(message)
            if not sku:
                reply = "Send the SKU you want and quantity, and I will create the deal."
                self._log_outbound(customer_id, phone, reply, intent)
                self._deliver_reply(
                    phone=phone,
                    reply=reply,
                    crm_entity_type=crm_entity_type,
                    crm_entity_id=crm_entity_id,
                    dialog_id=dialog_id,
                    event_id=event_id,
                )
                return {"reply": reply}
            if not self.stock_service.has_stock(sku, 1):
                reply = f"I cannot create a deal for {sku} because stock is unavailable."
                self._log_outbound(customer_id, phone, reply, "stock_block")
                self._deliver_reply(
                    phone=phone,
                    reply=reply,
                    crm_entity_type=crm_entity_type,
                    crm_entity_id=crm_entity_id,
                    dialog_id=dialog_id,
                    event_id=event_id,
                )
                return {"reply": reply, "deal_created": False}
            if not customer_id:
                reply = "I need your contact registration before creating a deal."
                self._log_outbound(customer_id, phone, reply, "need_registration")
                self._deliver_reply(
                    phone=phone,
                    reply=reply,
                    crm_entity_type=crm_entity_type,
                    crm_entity_id=crm_entity_id,
                    dialog_id=dialog_id,
                    event_id=event_id,
                )
                return {"reply": reply, "deal_created": False}
            deal_id = f"DEAL-{uuid4().hex[:10].upper()}"
            self.db.create_deal(
                {"id": deal_id, "customer_id": customer_id, "products": [{"sku": sku, "qty": 1}], "status": "NEW"}
            )
            key = f"crm:create_activity:{deal_id}"
            self.db.idempotency_acquire(key, "bitrix.create_activity")
            self.bitrix.create_activity(customer_id, f"Deal {deal_id} created from WhatsApp conversation.", idempotency_key=key)
            reply = f"Deal {deal_id} created for SKU {sku}. Please confirm billing data to close."
            self._log_outbound(customer_id, phone, reply, "deal_created")
            self._deliver_reply(
                phone=phone,
                reply=reply,
                crm_entity_type=crm_entity_type,
                crm_entity_id=crm_entity_id,
                dialog_id=dialog_id,
                event_id=event_id,
            )
            return {"reply": reply, "deal_created": True, "deal_id": deal_id}

        reply = self.llm_router.conversation_reply(
            intent=intent,
            customer_name=customer["name"] if customer else None,
            context_summary=context_summary,
            message=message,
            tools={
                "stock_lookup": lambda args: self._tool_stock_lookup(args),
                "crm_upsert_deal": lambda args: self._tool_create_deal(args, customer_id=customer_id),
                "handoff": lambda args: self._tool_request_handoff(args, phone=phone, customer=customer),
                "crm_log_activity": lambda args: self._tool_log_activity(args, customer_id=customer_id),
                "send_message": lambda args: self._tool_send_message(args, phone=phone),
                "schedule_followup": lambda args: self._tool_schedule_followup(args, customer_id=customer_id, phone=phone),
            },
        )
        if self.llm_router.last_conversation_failed:
            self.handoff_service.create_handoff(
                customer_id=customer_id,
                customer_name=customer["name"] if customer else "Unknown",
                phone=phone,
                machine_model="unknown",
                requested_product=self._extract_sku(message) or "unknown",
                summary="Falha no LLM. Escalonado para atendimento humano.",
                transcript=self.db.conversation_history(phone),
                reason="llm_failure",
            )
        self._log_outbound(customer_id, phone, reply, intent)
        self._deliver_reply(
            phone=phone,
            reply=reply,
            crm_entity_type=crm_entity_type,
            crm_entity_id=crm_entity_id,
            dialog_id=dialog_id,
            event_id=event_id,
        )
        return {"reply": reply}

    def _log_outbound(self, customer_id: str | None, phone: str, message: str, intent: str) -> None:
        self.db.log_conversation(
            {
                "customer_id": customer_id,
                "phone": phone,
                "direction": "outbound",
                "message": message,
                "intent": intent,
                "handoff": 0,
                "created_at": utc_now_iso(),
            }
        )

    def _deliver_reply(
        self,
        *,
        phone: str,
        reply: str,
        crm_entity_type: str | None,
        crm_entity_id: str | None,
        dialog_id: str | None,
        event_id: str | None,
    ) -> None:
        safe_reply = sanitize_message(reply)
        context = InboundContext(
            phone=phone,
            message="",
            crm_entity_type=crm_entity_type,
            crm_entity_id=crm_entity_id,
            dialog_id=dialog_id,
        )
        result = self.channel.send_message(context, safe_reply, event_id=event_id)
        if crm_entity_type and crm_entity_id and not result.ok:
            customer = self.db.customer_by_phone(phone)
            self.handoff_service.create_handoff(
                customer_id=customer["id"] if customer else None,
                customer_name=customer["name"] if customer else "Unknown",
                phone=phone,
                machine_model="unknown",
                requested_product="unknown",
                summary="OpenLines send failed; handoff required.",
                transcript=self.db.conversation_history(phone),
                reason=f"channel_send_failed:{result.reason or 'unknown'}",
            )

    def _tool_stock_lookup(self, args: dict[str, Any]) -> dict[str, Any]:
        sku = str(args.get("sku", "")).upper()
        if not sku:
            return {"status": "error", "error": "missing_sku"}
        stock = self.stock_service.query_stock(sku)
        return {
            "status": "ok",
            "sku": sku,
            "total_quantity": stock["total_quantity"],
            "updated_at": max((item["updated_at"] for item in stock["by_location"]), default=None),
        }

    def _tool_create_deal(self, args: dict[str, Any], *, customer_id: str | None) -> dict[str, Any]:
        if not customer_id:
            return {"status": "error", "error": "missing_customer"}
        sku = self._extract_sku(str(args.get("notes", "")))
        if not sku:
            return {"status": "error", "error": "missing_sku"}
        deal_id = f"DEAL-{uuid4().hex[:10].upper()}"
        self.db.create_deal({"id": deal_id, "customer_id": customer_id, "products": [{"sku": sku, "qty": 1}], "status": "NEW"})
        return {"status": "ok", "deal_id": deal_id}

    def _tool_request_handoff(self, args: dict[str, Any], *, phone: str, customer: dict[str, Any] | None) -> dict[str, Any]:
        reason = str(args.get("reason", "requested_by_llm"))
        payload = self.handoff_service.create_handoff(
            customer_id=customer["id"] if customer else None,
            customer_name=customer["name"] if customer else "Unknown",
            phone=phone,
            machine_model="unknown",
            requested_product="unknown",
            summary=f"Handoff solicitado pelo agente: {reason}",
            transcript=self.db.conversation_history(phone),
        )
        return {"status": "ok", "handoff": payload["handoff"], "reason": reason}

    def _tool_log_activity(self, args: dict[str, Any], *, customer_id: str | None) -> dict[str, Any]:
        message = sanitize_message(str(args.get("message", "")))
        if not message:
            return {"status": "error", "error": "empty_message"}
        target = customer_id or "unknown"
        self.bitrix.create_activity(target, message)
        return {"status": "ok"}

    def _tool_send_message(self, args: dict[str, Any], *, phone: str) -> dict[str, Any]:
        text = sanitize_message(str(args.get("text", "")))
        if not text:
            return {"status": "error", "error": "empty_text"}
        context = InboundContext(phone=phone, message="")
        self.channel.send_message(context, text)
        return {"status": "ok"}

    def _tool_schedule_followup(self, args: dict[str, Any], *, customer_id: str | None, phone: str) -> dict[str, Any]:
        target_id = str(args.get("target_id", phone))
        when = str(args.get("when", "tomorrow 09:00"))
        self.db.create_sales_task(
            {
                "customer_id": customer_id,
                "phone": phone,
                "title": f"Follow-up {target_id}",
                "summary": f"Follow-up agendado para {when}",
                "context_json": '{"source":"llm_tool"}',
                "status": "OPEN",
                "created_at": utc_now_iso(),
            }
        )
        return {"status": "ok", "target_id": target_id, "when": when}

    def _try_collect_telemetry_fields(self, *, phone: str, customer_id: str | None, message: str) -> dict[str, Any] | None:
        target = self.db.telemetry_target_by_phone(phone)
        if not target:
            return None
        required = target.get("required_fields_json") or {}
        collected = target.get("collected_fields_json") or {}
        for token in message.replace(";", ",").split(","):
            if "=" not in token:
                continue
            key, value = token.split("=", 1)
            key = key.strip()
            value = value.strip()
            if key:
                collected[key] = value
        missing = [k for k in required.keys() if not collected.get(k)]
        if missing:
            self.db.update_telemetry_target_progress(target_id=target["id"], collected_fields=collected, status="PENDING")
            self.bitrix.create_activity(customer_id or "unknown", f"Telemetry pendente: faltam {', '.join(missing)}")
            return {"reply": f"Obrigado. Ainda faltam os campos: {', '.join(missing)}.", "telemetry_status": "PENDING"}
        self.db.update_telemetry_target_progress(target_id=target["id"], collected_fields=collected, status="COMPLETE")
        self.bitrix.create_activity(customer_id or "unknown", "Telemetry data complete.")
        return {"reply": "Perfeito, recebemos todos os dados para ativação da telemetria.", "telemetry_status": "COMPLETE"}

    def _build_context_summary(self, *, state: dict[str, Any] | None, message: str, intent: str) -> str:
        history = []
        if state:
            history.append({"role": "system", "content": state.get("context_summary", "")})
        history.append({"role": "user", "content": message[:250]})
        previous = state["context_summary"] if state else ""
        return self.llm_router.summarize_context(history=history, previous_summary=previous)

    @staticmethod
    def _extract_sku(message: str) -> str | None:
        tokens = message.replace(",", " ").replace(".", " ").split()
        for token in tokens:
            upper = token.upper()
            if len(upper) >= 5 and any(char.isdigit() for char in upper):
                return upper
        return None

    @staticmethod
    def _detect_intent(message: str) -> str:
        lowered = message.lower()
        if "buy" in lowered or "purchase" in lowered or "want" in lowered:
            return "purchase"
        if "stock" in lowered or "available" in lowered or "sku" in lowered:
            return "stock_query"
        return "general"
