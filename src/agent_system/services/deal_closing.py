from __future__ import annotations

from dataclasses import dataclass
from typing import Any

from ..db import Database
from ..infra.security import stable_hash
from ..integrations.bitrix import BitrixClient
from ..integrations.omie import OmieClient


@dataclass
class DealClosingService:
    db: Database
    bitrix: BitrixClient
    omie: OmieClient

    def close_won_deal(self, *, event_id: str, deal_id: str) -> dict[str, Any]:
        inserted = self.db.store_event(event_id=event_id, source_system="bitrix", payload={"deal_id": deal_id}, status="PENDING")
        if not inserted and not self.db.claim_event(event_id):
            return {"status": "duplicate_ignored", "deal_id": deal_id, "event_id": event_id}

        if not self.db.claim_event(event_id):
            return {"status": "duplicate_ignored", "deal_id": deal_id, "event_id": event_id}

        try:
            remote_deal = self.bitrix.fetch_deal(deal_id, event_id=event_id)
        except Exception:  # noqa: BLE001
            remote_deal = {}
        deal = self.db.deal_by_id(deal_id)
        if not deal:
            self.db.mark_event_error(event_id, "deal_not_found")
            return {"status": "error", "error": "deal_not_found"}
        missing_fields = self._validate_required_fields(deal=deal, remote_deal=remote_deal)
        if missing_fields:
            self.bitrix.create_task(
                title=f"Deal {deal_id} pending close",
                summary=f"Campos obrigatórios ausentes: {', '.join(missing_fields)}",
                context={"deal_id": deal_id, "missing_fields": missing_fields},
            )
            self.bitrix.upsert_activity(deal_id, f"Pendente fechamento: faltam campos {', '.join(missing_fields)}")
            self.db.update_deal(deal_id, status="PENDING_CLOSE")
            self.db.mark_event_error(event_id, f"missing_required_fields:{','.join(missing_fields)}")
            return {"status": "error", "error": "missing_required_fields", "fields": missing_fields}

        try:
            payload_hash = stable_hash({"deal_id": deal_id, "products": deal["products"], "customer_id": deal["customer_id"]})
            order_key = self.db.idempotency_build_key(
                source_system="bitrix",
                entity_type="deal",
                entity_id=deal_id,
                action="create_order",
                payload_hash=payload_hash,
            )
            invoice_key = self.db.idempotency_build_key(
                source_system="bitrix",
                entity_type="deal",
                entity_id=deal_id,
                action="trigger_invoice",
                payload_hash=payload_hash,
            )
            crm_key = self.db.idempotency_build_key(
                source_system="bitrix",
                entity_type="deal",
                entity_id=deal_id,
                action="update_deal",
                payload_hash=payload_hash,
            )
            existing_order = self.db.idempotency_response(order_key)
            if existing_order and existing_order.get("order_id"):
                order_id = existing_order["order_id"]
            else:
                customer_code = self.omie.upsert_customer(
                    customer_document=str(remote_deal.get("UF_CRM_CPF_CNPJ", "")),
                    customer_name=str(remote_deal.get("TITLE", deal["customer_id"])),
                    phone=str(remote_deal.get("PHONE", "")),
                )
                order_id = self.omie.create_sales_order(
                    integration_code=f"{event_id}:{deal_id}",
                    customer_code=customer_code,
                    items=deal["products"],
                    payment_terms=str(remote_deal.get("UF_CRM_PAYMENT_TERMS", "000")),
                    branch=str(remote_deal.get("UF_CRM_BRANCH", "DEFAULT")),
                    delivery_city=remote_deal.get("UF_CRM_DELIVERY_CITY"),
                    delivery_uf=remote_deal.get("UF_CRM_DELIVERY_UF"),
                    notes=f"deal_id={deal_id}",
                )
                self.db.idempotency_acquire(order_key, "omie.create_order")
                self.db.idempotency_store_response(order_key, {"order_id": order_id})
                self.db.save_id_map(source_system="bitrix", source_id=deal_id, target_system="omie.order", target_id=order_id)

            existing_invoice = self.db.idempotency_response(invoice_key)
            if existing_invoice and existing_invoice.get("invoice_id"):
                invoice_id = existing_invoice["invoice_id"]
            else:
                invoice_id = self.omie.trigger_invoicing(order_id=order_id)
                self.db.idempotency_acquire(invoice_key, "omie.trigger_invoice")
                self.db.idempotency_store_response(invoice_key, {"invoice_id": invoice_id})
                self.db.save_id_map(source_system="bitrix", source_id=deal_id, target_system="omie.invoice", target_id=invoice_id)

            self.db.update_deal(
                deal_id,
                status="WON",
                omie_order_id=order_id,
                omie_invoice_id=invoice_id,
                last_event_id=event_id,
            )
            event = self.db.event_by_id(event_id)
            payload = {
                "UF_CRM_OMIE_ORDER_ID": order_id,
                "UF_CRM_OMIE_INVOICE_ID": invoice_id,
                "UF_CRM_OMIE_STATUS": "WON",
                "UF_CRM_LAST_SYNCED_AT": event["created_at"].isoformat() if event else "",
                "STAGE_ID": "WON",
            }
            self.db.idempotency_acquire(crm_key, "bitrix.update_deal")
            self.bitrix.update_deal_fields(deal_id, payload, event_id=event_id)
            response = {"status": "ok", "deal_id": deal_id, "order_id": order_id, "invoice_id": invoice_id}
            self.db.idempotency_store_response(order_key, response)
            self.db.mark_event_done(event_id)
            return response
        except Exception as exc:  # noqa: BLE001
            retries = self.db.mark_event_error(event_id, str(exc))
            return {"status": "error", "error": str(exc), "retries": retries}

    @staticmethod
    def _validate_required_fields(*, deal: dict[str, Any], remote_deal: dict[str, Any]) -> list[str]:
        missing: list[str] = []
        if not deal.get("customer_id"):
            missing.append("customer_id")
        if not deal.get("products"):
            missing.append("items")
        if not remote_deal.get("UF_CRM_PAYMENT_TERMS"):
            missing.append("payment_terms")
        if not remote_deal.get("UF_CRM_BRANCH"):
            missing.append("branch")
        if not remote_deal.get("UF_CRM_CPF_CNPJ"):
            missing.append("cpf_cnpj")
        return missing
