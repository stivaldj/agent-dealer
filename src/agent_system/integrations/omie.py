from __future__ import annotations

import json
import time
from dataclasses import dataclass
from http import HTTPStatus
from typing import Any
from urllib import error, request

from ..config import SETTINGS
from ..infra.observability import get_logger


class OmieIntegrationError(RuntimeError):
    pass


@dataclass
class OmieClient:
    timeout_seconds: float = 20.0
    max_retries: int = 3

    def __post_init__(self) -> None:
        self.logger = get_logger("integrations.omie")

    def list_products(self, *, page: int = 1, records_per_page: int = 100) -> dict[str, Any]:
        return self._call(
            service=SETTINGS.omie_products_service,
            endpoint="/",
            call=SETTINGS.omie_products_list_call,
            param=[{"pagina": page, "registros_por_pagina": records_per_page}],
        )

    def list_stock_by_location(
        self,
        *,
        sku: str | None = None,
        location: str | None = None,
        page: int = 1,
        records_per_page: int = 100,
    ) -> dict[str, Any]:
        filters: dict[str, Any] = {"pagina": page, "registros_por_pagina": records_per_page}
        if sku:
            filters["codigo"] = sku
        if location:
            filters["local_estoque"] = location
        return self._call(
            service=SETTINGS.omie_stock_service,
            endpoint="/",
            call=SETTINGS.omie_stock_list_call,
            param=[filters],
        )

    def fetch_products(self) -> list[dict[str, Any]]:
        page = 1
        all_rows: list[dict[str, Any]] = []
        while True:
            payload = self.list_products(page=page)
            products = payload.get("produto_servico_cadastro", []) or payload.get("produtos", [])
            for p in products:
                all_rows.append(
                    {
                        "sku": str(p.get("codigo") or p.get("codigo_produto") or p.get("codigo_prod") or ""),
                        "name": p.get("descricao", ""),
                        "category": p.get("descricao_familia", "") or p.get("ncm", "general"),
                        "location": p.get("local_estoque", "default"),
                        "quantity": int(float(p.get("quantidade_estoque", 0) or 0)),
                    }
                )
            total_pages = int(payload.get("total_de_paginas", 1) or 1)
            if page >= total_pages:
                break
            page += 1
        return [row for row in all_rows if row["sku"]]

    def upsert_customer(
        self,
        *,
        customer_document: str,
        customer_name: str,
        email: str | None = None,
        phone: str | None = None,
        address: dict[str, Any] | None = None,
    ) -> str:
        list_payload = self._call(
            service=SETTINGS.omie_clients_service,
            endpoint="/",
            call=SETTINGS.omie_client_list_call,
            param=[{"pagina": 1, "registros_por_pagina": 50, "cnpj_cpf": customer_document}],
        )
        clients = list_payload.get("clientes_cadastro", []) or []
        existing_code = str(clients[0].get("codigo_cliente_omie")) if clients else None

        client_payload = {
            "razao_social": customer_name,
            "cnpj_cpf": customer_document,
            "email": email or "",
            "telefone1_numero": phone or "",
        }
        if address:
            client_payload.update(address)
        if existing_code:
            client_payload["codigo_cliente_omie"] = existing_code

        response = self._call(
            service=SETTINGS.omie_clients_service,
            endpoint="/",
            call=SETTINGS.omie_client_upsert_call,
            param=[client_payload],
        )
        code = response.get("codigo_cliente_omie") or response.get("codigo_cliente_integracao") or existing_code
        if not code:
            raise OmieIntegrationError("omie_upsert_customer_missing_id")
        return str(code)

    def create_sales_order(
        self,
        *,
        integration_code: str,
        customer_code: str,
        items: list[dict[str, Any]],
        payment_terms: str,
        branch: str,
        delivery_city: str | None = None,
        delivery_uf: str | None = None,
        notes: str | None = None,
    ) -> str:
        pedido = {
            "cabecalho": {
                "codigo_cliente": customer_code,
                "codigo_pedido_integracao": integration_code,
                "etapa": "10",
                "codigo_parcela": payment_terms,
            },
            "det": [{"ide": {"codigo_item_integracao": item["sku"]}, "produto": {"codigo_produto": item["sku"], "quantidade": item["qty"]}} for item in items],
            "frete": {"cfo": branch},
            "informacoes_adicionais": {"codVend": branch, "obs_venda": notes or ""},
        }
        if delivery_city or delivery_uf:
            pedido["lista_parcelas"] = [{"cidade": delivery_city or "", "estado": delivery_uf or ""}]
        payload = self._call(
            service=SETTINGS.omie_order_service,
            endpoint="/",
            call=SETTINGS.omie_order_create_call,
            param=[pedido],
        )
        order_id = payload.get("codigo_pedido") or payload.get("numero_pedido") or payload.get("pedido_venda")
        if not order_id:
            raise OmieIntegrationError("omie_create_order_missing_id")
        return str(order_id)

    def trigger_invoicing(self, *, order_id: str, stage: str = "faturar") -> str:
        payload = self._call(
            service=SETTINGS.omie_invoice_service,
            endpoint="/",
            call=SETTINGS.omie_invoice_call,
            param=[{"codigo_pedido": order_id, "acao": stage}],
        )
        invoice_id = payload.get("codigo_nfe") or payload.get("numero_nfe") or payload.get("codigo_lancamento")
        if not invoice_id:
            return f"READY_TO_INVOICE:{order_id}"
        return str(invoice_id)

    def create_order(
        self,
        event_id: str,
        customer_id: str,
        items: list[dict[str, Any]],
        idempotency_key: str | None = None,
    ) -> str:
        return self.create_sales_order(
            integration_code=idempotency_key or event_id,
            customer_code=customer_id,
            items=items,
            payment_terms="000",
            branch="DEFAULT",
        )

    def trigger_invoice(self, order_id: str, idempotency_key: str | None = None) -> str:
        _ = idempotency_key
        return self.trigger_invoicing(order_id=order_id)

    def _call(self, *, service: str, endpoint: str, call: str, param: list[dict[str, Any]]) -> dict[str, Any]:
        if SETTINGS.dry_run:
            self.logger.info("omie_dry_run", extra={"extra": {"service": service, "call": call, "param": param}})
            if call == SETTINGS.omie_products_list_call:
                return {
                    "total_de_paginas": 1,
                    "produto_servico_cadastro": [
                        {
                            "codigo": "ABC123",
                            "descricao": "Maintenance Kit A",
                            "descricao_familia": "parts",
                            "local_estoque": "MAIN",
                            "quantidade_estoque": 10,
                        }
                    ],
                }
            if call == SETTINGS.omie_stock_list_call:
                return {"total_de_paginas": 1, "estoque": [{"codigo": "ABC123", "local_estoque": "MAIN", "quantidade": 10}]}
            if call == SETTINGS.omie_client_list_call:
                return {"clientes_cadastro": []}
            if call == SETTINGS.omie_client_upsert_call:
                return {"codigo_cliente_omie": "CLI-DRY"}
            if call == SETTINGS.omie_order_create_call:
                return {"codigo_pedido": "PED-DRY-001"}
            if call == SETTINGS.omie_invoice_call:
                return {"codigo_nfe": "NFE-DRY-001"}
            return {"codigo_status": "0"}
        if not SETTINGS.omie_app_key or not SETTINGS.omie_app_secret:
            raise OmieIntegrationError("Omie credentials are not configured")
        path = "/".join(part for part in [service.strip("/"), endpoint.strip("/")] if part)
        url = f"{SETTINGS.omie_base_url.rstrip('/')}/{path}/"
        payload = {
            "call": call,
            "app_key": SETTINGS.omie_app_key,
            "app_secret": SETTINGS.omie_app_secret,
            "param": param,
        }
        headers = {"Content-Type": "application/json"}
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
                        "omie_request",
                        extra={
                            "extra": {
                                "omie_service": service,
                                "omie_method": call,
                                "status": resp.status,
                                "duration_ms": duration_ms,
                                "omie_error": data.get("faultstring") or data.get("descricao_status"),
                            }
                        },
                    )
                    if self._has_error(data):
                        if attempt < self.max_retries:
                            time.sleep(2**attempt)
                            continue
                        raise OmieIntegrationError(f"omie_error:{self._extract_error(data)}")
                    return data
            except error.HTTPError as exc:
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
        raise OmieIntegrationError(f"omie_request_failed:{service}:{call}") from last_exc

    @staticmethod
    def _has_error(payload: dict[str, Any]) -> bool:
        if "faultcode" in payload:
            return True
        if payload.get("codigo_status") not in (None, "0", 0):
            return True
        return False

    @staticmethod
    def _extract_error(payload: dict[str, Any]) -> str:
        return str(payload.get("faultstring") or payload.get("descricao_status") or payload.get("mensagem", "unknown"))
