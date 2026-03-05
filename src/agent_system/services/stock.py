from __future__ import annotations

from typing import Any

from ..db import Database, utc_now_iso
from ..integrations.omie import OmieClient


class StockService:
    def __init__(self, db: Database, omie: OmieClient) -> None:
        self.db = db
        self.omie = omie

    def full_sync(self) -> int:
        products = self.omie.fetch_products()
        stock_map: dict[tuple[str, str], int] = {}
        try:
            page = 1
            while True:
                stock_payload = self.omie.list_stock_by_location(page=page)
                stock_rows = stock_payload.get("estoque", []) or stock_payload.get("posicoes_estoque", [])
                for row in stock_rows:
                    sku = str(row.get("codigo") or row.get("codigo_produto") or "")
                    location = str(row.get("local_estoque") or "default")
                    if not sku:
                        continue
                    stock_map[(sku, location)] = int(float(row.get("quantidade", 0) or 0))
                total_pages = int(stock_payload.get("total_de_paginas", 1) or 1)
                if page >= total_pages:
                    break
                page += 1
        except Exception:
            stock_map = {}
        rows: list[dict[str, Any]] = []
        now = utc_now_iso()
        for product in products:
            key = (product["sku"], product["location"])
            rows.append(
                {
                    "sku": product["sku"],
                    "name": product["name"],
                    "category": product["category"],
                    "location": product["location"],
                    "quantity": int(stock_map.get(key, product["quantity"])),
                    "updated_at": now,
                }
            )
        self.db.upsert_stock(rows)
        return len(rows)

    def query_stock(self, sku: str) -> dict[str, Any]:
        entries = self.db.stock_by_sku(sku)
        total = sum(item["quantity"] for item in entries)
        return {"sku": sku, "total_quantity": total, "by_location": entries}

    def has_stock(self, sku: str, requested_qty: int = 1) -> bool:
        return self.query_stock(sku)["total_quantity"] >= requested_qty

    def suggest_alternatives(self, category: str, min_qty: int = 1) -> list[dict[str, Any]]:
        alternatives = [
            item
            for item in self.db.all_stock()
            if item["category"] == category and item["quantity"] >= min_qty
        ]
        return sorted(alternatives, key=lambda item: item["quantity"], reverse=True)
