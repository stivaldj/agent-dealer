from __future__ import annotations

import json
from contextlib import contextmanager
from datetime import datetime, timezone
from typing import Any, Iterator

from psycopg import connect
from psycopg.rows import dict_row

from .config import SETTINGS


def utc_now_iso() -> str:
    return datetime.now(timezone.utc).isoformat()


class Database:
    def __init__(self, database_url: str | None = None) -> None:
        self.database_url = database_url or SETTINGS.database_url
        self._init_schema()

    @contextmanager
    def connection(self) -> Iterator[Any]:
        conn = connect(self.database_url, row_factory=dict_row)
        try:
            yield conn
            conn.commit()
        finally:
            conn.close()

    def _init_schema(self) -> None:
        with self.connection() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    """
                    CREATE TABLE IF NOT EXISTS clients (
                        id TEXT PRIMARY KEY,
                        name TEXT NOT NULL,
                        phone TEXT NOT NULL UNIQUE,
                        city TEXT NOT NULL DEFAULT '',
                        state TEXT NOT NULL DEFAULT '',
                        bitrix_contact_id TEXT,
                        bitrix_company_id TEXT,
                        created_at TIMESTAMPTZ NOT NULL
                    );
                    CREATE TABLE IF NOT EXISTS id_map (
                        id BIGSERIAL PRIMARY KEY,
                        source_system TEXT NOT NULL,
                        source_id TEXT NOT NULL,
                        target_system TEXT NOT NULL,
                        target_id TEXT NOT NULL,
                        created_at TIMESTAMPTZ NOT NULL
                    );
                    CREATE TABLE IF NOT EXISTS stock_snapshot (
                        sku TEXT NOT NULL,
                        name TEXT NOT NULL,
                        category TEXT NOT NULL,
                        location TEXT NOT NULL,
                        quantity INTEGER NOT NULL,
                        updated_at TIMESTAMPTZ NOT NULL,
                        PRIMARY KEY (sku, location)
                    );
                    CREATE TABLE IF NOT EXISTS machines (
                        id TEXT PRIMARY KEY,
                        client_id TEXT,
                        FOREIGN KEY (client_id) REFERENCES clients(id),
                        brand TEXT NOT NULL DEFAULT '',
                        model TEXT NOT NULL,
                        serial TEXT NOT NULL,
                        year INTEGER NOT NULL,
                        telemetry_status TEXT NOT NULL DEFAULT 'inactive',
                        telemetry_active BOOLEAN NOT NULL DEFAULT FALSE,
                        created_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
                    );
                    CREATE TABLE IF NOT EXISTS machine_ownership (
                        client_id TEXT NOT NULL REFERENCES clients(id),
                        machine_id TEXT NOT NULL REFERENCES machines(id),
                        start_at TIMESTAMPTZ NOT NULL,
                        end_at TIMESTAMPTZ,
                        PRIMARY KEY (client_id, machine_id, start_at)
                    );
                    CREATE TABLE IF NOT EXISTS deals (
                        id TEXT PRIMARY KEY,
                        customer_id TEXT,
                        client_id TEXT,
                        products_json JSONB NOT NULL,
                        quantity INTEGER NOT NULL DEFAULT 1,
                        status TEXT NOT NULL,
                        omie_order_id TEXT,
                        omie_invoice_id TEXT,
                        last_event_id TEXT,
                        created_at TIMESTAMPTZ NOT NULL,
                        updated_at TIMESTAMPTZ NOT NULL
                    );
                    CREATE TABLE IF NOT EXISTS campaign_log (
                        id BIGSERIAL PRIMARY KEY,
                        campaign_id TEXT NOT NULL,
                        customer_id TEXT NOT NULL,
                        phone TEXT NOT NULL,
                        message_sent_at TIMESTAMPTZ NOT NULL,
                        response_at TIMESTAMPTZ,
                        response TEXT,
                        outcome TEXT NOT NULL,
                        wave TEXT NOT NULL
                    );
                    CREATE TABLE IF NOT EXISTS conversation_log (
                        id BIGSERIAL PRIMARY KEY,
                        customer_id TEXT,
                        phone TEXT NOT NULL,
                        direction TEXT NOT NULL,
                        message TEXT NOT NULL,
                        intent TEXT,
                        handoff INTEGER NOT NULL DEFAULT 0,
                        created_at TIMESTAMPTZ NOT NULL
                    );
                    CREATE TABLE IF NOT EXISTS conversation_state (
                        conversation_id TEXT PRIMARY KEY,
                        customer_id TEXT,
                        last_intent TEXT,
                        context_summary TEXT,
                        updated_at TIMESTAMPTZ NOT NULL
                    );
                    CREATE TABLE IF NOT EXISTS salesperson_tasks (
                        id BIGSERIAL PRIMARY KEY,
                        customer_id TEXT,
                        phone TEXT NOT NULL,
                        title TEXT NOT NULL,
                        summary TEXT NOT NULL,
                        context_json JSONB NOT NULL,
                        status TEXT NOT NULL,
                        created_at TIMESTAMPTZ NOT NULL
                    );
                    CREATE TABLE IF NOT EXISTS telemetry_activation_log (
                        id BIGSERIAL PRIMARY KEY,
                        machine_id TEXT NOT NULL,
                        customer_id TEXT NOT NULL,
                        contacted_at TIMESTAMPTZ NOT NULL,
                        status TEXT NOT NULL,
                        response TEXT
                    );
                    CREATE TABLE IF NOT EXISTS telemetry_targets (
                        id BIGSERIAL PRIMARY KEY,
                        customer_id TEXT NOT NULL,
                        machine_id TEXT,
                        client_id TEXT,
                        phone TEXT NOT NULL,
                        required_fields_json JSONB NOT NULL DEFAULT '{}'::jsonb,
                        collected_fields_json JSONB NOT NULL DEFAULT '{}'::jsonb,
                        status TEXT NOT NULL DEFAULT 'PENDING',
                        last_contact_at TIMESTAMPTZ,
                        created_at TIMESTAMPTZ NOT NULL
                    );
                    CREATE TABLE IF NOT EXISTS offer_rules (
                        id TEXT PRIMARY KEY,
                        rule_type TEXT NOT NULL,
                        predicate JSONB NOT NULL DEFAULT '{}'::jsonb,
                        offer_template TEXT NOT NULL,
                        sku_list JSONB NOT NULL DEFAULT '[]'::jsonb,
                        priority INTEGER NOT NULL DEFAULT 100,
                        enabled BOOLEAN NOT NULL DEFAULT TRUE
                    );
                    CREATE TABLE IF NOT EXISTS campaigns (
                        id TEXT PRIMARY KEY,
                        name TEXT NOT NULL,
                        type TEXT NOT NULL,
                        status TEXT NOT NULL,
                        schedule TEXT NOT NULL,
                        segment JSONB NOT NULL DEFAULT '{}'::jsonb,
                        template TEXT NOT NULL,
                        created_at TIMESTAMPTZ NOT NULL
                    );
                    CREATE TABLE IF NOT EXISTS campaign_targets (
                        id BIGSERIAL PRIMARY KEY,
                        campaign_id TEXT NOT NULL REFERENCES campaigns(id),
                        client_id TEXT NOT NULL,
                        machine_id TEXT,
                        phone TEXT NOT NULL,
                        status TEXT NOT NULL,
                        attempts INTEGER NOT NULL DEFAULT 0,
                        last_attempt_at TIMESTAMPTZ,
                        last_result TEXT,
                        UNIQUE(campaign_id, client_id, machine_id, phone)
                    );
                    CREATE TABLE IF NOT EXISTS daily_reports (
                        id BIGSERIAL PRIMARY KEY,
                        report_date DATE NOT NULL UNIQUE,
                        payload_json JSONB NOT NULL,
                        created_at TIMESTAMPTZ NOT NULL
                    );
                    CREATE TABLE IF NOT EXISTS event_store (
                        event_id TEXT PRIMARY KEY,
                        source_system TEXT NOT NULL,
                        payload JSONB NOT NULL,
                        status TEXT NOT NULL,
                        attempt_count INTEGER NOT NULL DEFAULT 0,
                        created_at TIMESTAMPTZ NOT NULL
                    );
                    CREATE TABLE IF NOT EXISTS idempotency_keys (
                        key TEXT PRIMARY KEY,
                        operation TEXT NOT NULL,
                        response_json JSONB,
                        created_at TIMESTAMPTZ NOT NULL
                    );
                    CREATE TABLE IF NOT EXISTS opt_out (
                        phone TEXT PRIMARY KEY,
                        reason TEXT NOT NULL,
                        created_at TIMESTAMPTZ NOT NULL
                    );
                    CREATE TABLE IF NOT EXISTS contact_frequency (
                        phone TEXT PRIMARY KEY,
                        last_contact_at TIMESTAMPTZ NOT NULL
                    );
                    CREATE INDEX IF NOT EXISTS idx_event_store_status ON event_store(status, created_at);
                    CREATE INDEX IF NOT EXISTS idx_campaign_log_date ON campaign_log(message_sent_at);
                    CREATE INDEX IF NOT EXISTS idx_conversation_log_phone ON conversation_log(phone, id);
                    CREATE INDEX IF NOT EXISTS idx_id_map_source ON id_map(source_system, source_id, target_system);
                    CREATE UNIQUE INDEX IF NOT EXISTS idx_id_map_unique ON id_map(source_system, source_id, target_system, target_id);
                    CREATE INDEX IF NOT EXISTS idx_machine_ownership_client ON machine_ownership(client_id, end_at);
                    CREATE INDEX IF NOT EXISTS idx_machine_ownership_machine ON machine_ownership(machine_id, end_at);
                    CREATE INDEX IF NOT EXISTS idx_campaign_targets_status ON campaign_targets(campaign_id, status);
                    CREATE UNIQUE INDEX IF NOT EXISTS idx_campaign_targets_unique_expr
                        ON campaign_targets(campaign_id, client_id, COALESCE(machine_id, ''), phone);
                    CREATE INDEX IF NOT EXISTS idx_telemetry_targets_status ON telemetry_targets(status, last_contact_at);
                    """
                )
                cur.execute(
                    """
                    ALTER TABLE machines ADD COLUMN IF NOT EXISTS brand TEXT NOT NULL DEFAULT '';
                    ALTER TABLE machines ADD COLUMN IF NOT EXISTS client_id TEXT;
                    ALTER TABLE machines ADD COLUMN IF NOT EXISTS telemetry_active BOOLEAN NOT NULL DEFAULT FALSE;
                    ALTER TABLE machines ADD COLUMN IF NOT EXISTS created_at TIMESTAMPTZ NOT NULL DEFAULT NOW();
                    ALTER TABLE deals ADD COLUMN IF NOT EXISTS client_id TEXT;
                    ALTER TABLE telemetry_targets ADD COLUMN IF NOT EXISTS machine_id TEXT;
                    ALTER TABLE telemetry_targets ADD COLUMN IF NOT EXISTS client_id TEXT;
                    ALTER TABLE telemetry_targets ADD COLUMN IF NOT EXISTS collected_fields_json JSONB NOT NULL DEFAULT '{}'::jsonb;
                    ALTER TABLE telemetry_targets ADD COLUMN IF NOT EXISTS last_contact_at TIMESTAMPTZ;
                    """
                )
                cur.execute(
                    """
                    DO $$
                    BEGIN
                      IF EXISTS (
                        SELECT 1 FROM information_schema.columns
                        WHERE table_schema='public' AND table_name='machines' AND column_name='customer_id'
                      ) THEN
                        UPDATE machines SET client_id = COALESCE(client_id, customer_id) WHERE client_id IS NULL;
                        ALTER TABLE machines DROP COLUMN customer_id;
                      END IF;
                    END $$;
                    """
                )
                cur.execute(
                    """
                    DO $$
                    BEGIN
                      IF NOT EXISTS (
                        SELECT 1 FROM pg_constraint WHERE conname = 'machines_client_id_fkey'
                      ) THEN
                        ALTER TABLE machines
                        ADD CONSTRAINT machines_client_id_fkey
                        FOREIGN KEY (client_id) REFERENCES clients(id);
                      END IF;
                    END $$;
                    """
                )
                cur.execute(
                    """
                    DO $$
                    BEGIN
                      IF EXISTS (SELECT 1 FROM information_schema.tables WHERE table_schema='public' AND table_name='customers') THEN
                        INSERT INTO clients (id, name, phone, city, state, created_at)
                        SELECT c.id, c.name, c.phone, COALESCE(c.store, ''), COALESCE(c.region, ''), NOW()
                        FROM customers c
                        ON CONFLICT (id) DO NOTHING;
                      END IF;
                    END $$;
                    """
                )
                cur.execute("UPDATE deals SET client_id = COALESCE(client_id, customer_id) WHERE client_id IS NULL")
                cur.execute("UPDATE telemetry_targets SET client_id = COALESCE(client_id, customer_id) WHERE client_id IS NULL")
                cur.execute("DROP TABLE IF EXISTS customers CASCADE")

    def upsert_stock(self, rows: list[dict[str, Any]]) -> None:
        if not rows:
            return
        query = (
            """
            INSERT INTO stock_snapshot (sku, name, category, location, quantity, updated_at)
            VALUES (%(sku)s, %(name)s, %(category)s, %(location)s, %(quantity)s, %(updated_at)s)
            ON CONFLICT(sku, location) DO UPDATE SET
                name = EXCLUDED.name,
                category = EXCLUDED.category,
                quantity = EXCLUDED.quantity,
                updated_at = EXCLUDED.updated_at
            """
        )
        with self.connection() as conn:
            with conn.cursor() as cur:
                cur.executemany(query, rows)

    def stock_by_sku(self, sku: str) -> list[dict[str, Any]]:
        with self.connection() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    "SELECT sku, name, category, location, quantity, updated_at FROM stock_snapshot WHERE sku = %s",
                    (sku,),
                )
                return cur.fetchall()

    def all_stock(self) -> list[dict[str, Any]]:
        with self.connection() as conn:
            with conn.cursor() as cur:
                cur.execute("SELECT sku, name, category, location, quantity, updated_at FROM stock_snapshot")
                return cur.fetchall()

    def add_customer(self, customer: dict[str, Any]) -> None:
        self.add_client(
            {
                "id": customer["id"],
                "name": customer["name"],
                "phone": customer["phone"],
                "city": customer.get("store", ""),
                "state": customer.get("region", ""),
                "created_at": customer.get("created_at", utc_now_iso()),
            }
        )

    def customer_by_phone(self, phone: str) -> dict[str, Any] | None:
        row = self.client_by_phone(phone)
        if not row:
            return None
        return {"id": row["id"], "name": row["name"], "phone": row["phone"], "region": row.get("state", ""), "store": row.get("city", "")}

    def customers(self) -> list[dict[str, Any]]:
        return [{"id": c["id"], "name": c["name"], "phone": c["phone"], "region": c.get("state", ""), "store": c.get("city", "")} for c in self.clients()]

    def add_client(self, client: dict[str, Any]) -> None:
        payload = {
            "id": client["id"],
            "name": client["name"],
            "phone": client["phone"],
            "city": client.get("city", ""),
            "state": client.get("state", ""),
            "bitrix_contact_id": client.get("bitrix_contact_id"),
            "bitrix_company_id": client.get("bitrix_company_id"),
            "created_at": client.get("created_at", utc_now_iso()),
        }
        with self.connection() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    """
                    INSERT INTO clients (id, name, phone, city, state, bitrix_contact_id, bitrix_company_id, created_at)
                    VALUES (%(id)s, %(name)s, %(phone)s, %(city)s, %(state)s, %(bitrix_contact_id)s, %(bitrix_company_id)s, %(created_at)s)
                    ON CONFLICT(id) DO UPDATE SET
                        name = EXCLUDED.name,
                        phone = EXCLUDED.phone,
                        city = EXCLUDED.city,
                        state = EXCLUDED.state,
                        bitrix_contact_id = EXCLUDED.bitrix_contact_id,
                        bitrix_company_id = EXCLUDED.bitrix_company_id
                    """,
                    payload,
                )

    def clients(self) -> list[dict[str, Any]]:
        with self.connection() as conn:
            with conn.cursor() as cur:
                cur.execute("SELECT * FROM clients ORDER BY id")
                return cur.fetchall()

    def client_by_phone(self, phone: str) -> dict[str, Any] | None:
        with self.connection() as conn:
            with conn.cursor() as cur:
                cur.execute("SELECT * FROM clients WHERE phone = %s", (phone,))
                return cur.fetchone()

    def add_machine(self, machine: dict[str, Any]) -> None:
        with self.connection() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    """
                    INSERT INTO machines (id, client_id, brand, model, serial, year, telemetry_status, telemetry_active, created_at)
                    VALUES (%(id)s, %(client_id)s, %(brand)s, %(model)s, %(serial)s, %(year)s, %(telemetry_status)s, %(telemetry_active)s, %(created_at)s)
                    ON CONFLICT(id) DO UPDATE SET
                        client_id = EXCLUDED.client_id,
                        brand = EXCLUDED.brand,
                        model = EXCLUDED.model,
                        serial = EXCLUDED.serial,
                        year = EXCLUDED.year,
                        telemetry_status = EXCLUDED.telemetry_status,
                        telemetry_active = EXCLUDED.telemetry_active
                    """,
                    {
                        **machine,
                        "client_id": machine["client_id"],
                        "brand": machine.get("brand", ""),
                        "telemetry_active": machine.get("telemetry_active", machine.get("telemetry_status") == "active"),
                        "created_at": machine.get("created_at", utc_now_iso()),
                    },
                )

    def add_machine_ownership(self, *, client_id: str, machine_id: str, start_at: str | None = None, end_at: str | None = None) -> None:
        with self.connection() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    """
                    INSERT INTO machine_ownership (client_id, machine_id, start_at, end_at)
                    VALUES (%s, %s, %s, %s)
                    ON CONFLICT(client_id, machine_id, start_at) DO UPDATE SET end_at = EXCLUDED.end_at
                    """,
                    (client_id, machine_id, start_at or utc_now_iso(), end_at),
                )

    def active_machine_ownerships(self) -> list[dict[str, Any]]:
        with self.connection() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    """
                    SELECT o.client_id, o.machine_id, c.name AS client_name, c.phone, c.city, c.state,
                           m.brand, m.model, m.serial, m.year, m.telemetry_active
                    FROM machine_ownership o
                    JOIN clients c ON c.id = o.client_id
                    JOIN machines m ON m.id = o.machine_id
                    WHERE o.end_at IS NULL
                    """
                )
                return cur.fetchall()

    def machines_without_telemetry(self) -> list[dict[str, Any]]:
        with self.connection() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    """
                    SELECT m.id, m.client_id, m.brand, m.model, m.serial, m.year, c.phone, c.name
                    FROM machines m
                    JOIN clients c ON c.id = m.client_id
                    WHERE m.telemetry_status != 'active' OR m.telemetry_active = FALSE
                    """
                )
                return cur.fetchall()

    def machines(self) -> list[dict[str, Any]]:
        with self.connection() as conn:
            with conn.cursor() as cur:
                cur.execute("SELECT id, client_id, brand, model, serial, year, telemetry_status, telemetry_active, created_at FROM machines")
                return cur.fetchall()

    def create_deal(self, deal: dict[str, Any]) -> str:
        now = utc_now_iso()
        payload = {
            "id": deal["id"],
            "customer_id": deal["customer_id"],
            "client_id": deal.get("client_id", deal["customer_id"]),
            "products_json": json.dumps(deal["products"]),
            "quantity": deal.get("quantity", 1),
            "status": deal.get("status", "NEW"),
            "omie_order_id": deal.get("omie_order_id"),
            "omie_invoice_id": deal.get("omie_invoice_id"),
            "last_event_id": deal.get("last_event_id"),
            "created_at": now,
            "updated_at": now,
        }
        with self.connection() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    """
                    INSERT INTO deals
                    (id, customer_id, client_id, products_json, quantity, status, omie_order_id, omie_invoice_id, last_event_id, created_at, updated_at)
                    VALUES
                    (%(id)s, %(customer_id)s, %(client_id)s, %(products_json)s::jsonb, %(quantity)s, %(status)s, %(omie_order_id)s, %(omie_invoice_id)s, %(last_event_id)s, %(created_at)s, %(updated_at)s)
                    ON CONFLICT(id) DO UPDATE SET
                        customer_id = EXCLUDED.customer_id,
                        client_id = EXCLUDED.client_id,
                        products_json = EXCLUDED.products_json,
                        quantity = EXCLUDED.quantity,
                        status = EXCLUDED.status,
                        omie_order_id = EXCLUDED.omie_order_id,
                        omie_invoice_id = EXCLUDED.omie_invoice_id,
                        last_event_id = EXCLUDED.last_event_id,
                        updated_at = EXCLUDED.updated_at
                    """,
                    payload,
                )
        return deal["id"]

    def deal_by_id(self, deal_id: str) -> dict[str, Any] | None:
        with self.connection() as conn:
            with conn.cursor() as cur:
                cur.execute("SELECT * FROM deals WHERE id = %s", (deal_id,))
                row = cur.fetchone()
        if not row:
            return None
        row["products"] = row.pop("products_json")
        return row

    def deals_by_date_prefix(self, prefix: str) -> list[dict[str, Any]]:
        with self.connection() as conn:
            with conn.cursor() as cur:
                cur.execute("SELECT * FROM deals WHERE CAST(created_at AS TEXT) LIKE %s", (f"{prefix}%",))
                rows = cur.fetchall()
        for row in rows:
            row["products"] = row.pop("products_json")
        return rows

    def update_deal(self, deal_id: str, **fields: Any) -> None:
        if not fields:
            return
        fields["updated_at"] = utc_now_iso()
        set_clause = ", ".join([f"{key} = %({key})s" for key in fields.keys()])
        fields["id"] = deal_id
        with self.connection() as conn:
            with conn.cursor() as cur:
                cur.execute(f"UPDATE deals SET {set_clause} WHERE id = %(id)s", fields)

    def log_campaign(self, item: dict[str, Any]) -> None:
        with self.connection() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    """
                    INSERT INTO campaign_log (campaign_id, customer_id, phone, message_sent_at, response_at, response, outcome, wave)
                    VALUES (%(campaign_id)s, %(customer_id)s, %(phone)s, %(message_sent_at)s, %(response_at)s, %(response)s, %(outcome)s, %(wave)s)
                    """,
                    item,
                )

    def campaign_entries(self) -> list[dict[str, Any]]:
        with self.connection() as conn:
            with conn.cursor() as cur:
                cur.execute("SELECT * FROM campaign_log")
                return cur.fetchall()

    def log_conversation(self, row: dict[str, Any]) -> None:
        with self.connection() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    """
                    INSERT INTO conversation_log (customer_id, phone, direction, message, intent, handoff, created_at)
                    VALUES (%(customer_id)s, %(phone)s, %(direction)s, %(message)s, %(intent)s, %(handoff)s, %(created_at)s)
                    """,
                    row,
                )

    def conversation_history(self, phone: str) -> list[dict[str, Any]]:
        with self.connection() as conn:
            with conn.cursor() as cur:
                cur.execute("SELECT * FROM conversation_log WHERE phone = %s ORDER BY id ASC", (phone,))
                return cur.fetchall()

    def upsert_conversation_state(
        self,
        *,
        conversation_id: str,
        customer_id: str | None,
        last_intent: str,
        context_summary: str,
    ) -> None:
        with self.connection() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    """
                    INSERT INTO conversation_state (conversation_id, customer_id, last_intent, context_summary, updated_at)
                    VALUES (%s, %s, %s, %s, %s)
                    ON CONFLICT(conversation_id) DO UPDATE SET
                        customer_id = EXCLUDED.customer_id,
                        last_intent = EXCLUDED.last_intent,
                        context_summary = EXCLUDED.context_summary,
                        updated_at = EXCLUDED.updated_at
                    """,
                    (conversation_id, customer_id, last_intent, context_summary, utc_now_iso()),
                )

    def conversation_state(self, conversation_id: str) -> dict[str, Any] | None:
        with self.connection() as conn:
            with conn.cursor() as cur:
                cur.execute("SELECT * FROM conversation_state WHERE conversation_id = %s", (conversation_id,))
                return cur.fetchone()

    def create_sales_task(self, task: dict[str, Any]) -> None:
        with self.connection() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    """
                    INSERT INTO salesperson_tasks (customer_id, phone, title, summary, context_json, status, created_at)
                    VALUES (%(customer_id)s, %(phone)s, %(title)s, %(summary)s, %(context_json)s::jsonb, %(status)s, %(created_at)s)
                    """,
                    task,
                )

    def all_sales_tasks(self) -> list[dict[str, Any]]:
        with self.connection() as conn:
            with conn.cursor() as cur:
                cur.execute("SELECT * FROM salesperson_tasks")
                return cur.fetchall()

    def log_telemetry_activation(self, item: dict[str, Any]) -> None:
        with self.connection() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    """
                    INSERT INTO telemetry_activation_log (machine_id, customer_id, contacted_at, status, response)
                    VALUES (%(machine_id)s, %(customer_id)s, %(contacted_at)s, %(status)s, %(response)s)
                    """,
                    item,
                )

    def telemetry_logs(self) -> list[dict[str, Any]]:
        with self.connection() as conn:
            with conn.cursor() as cur:
                cur.execute("SELECT * FROM telemetry_activation_log")
                return cur.fetchall()

    def store_event(self, *, event_id: str, source_system: str, payload: dict[str, Any], status: str = "PENDING") -> bool:
        with self.connection() as conn:
            with conn.cursor() as cur:
                cur.execute("SELECT event_id FROM event_store WHERE event_id = %s", (event_id,))
                if cur.fetchone():
                    return False
                cur.execute(
                    """
                    INSERT INTO event_store (event_id, source_system, payload, status, attempt_count, created_at)
                    VALUES (%s, %s, %s::jsonb, %s, 0, %s)
                    """,
                    (event_id, source_system, json.dumps(payload), status, utc_now_iso()),
                )
        return True

    def claim_event(self, event_id: str) -> bool:
        with self.connection() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    """
                    UPDATE event_store
                    SET status = 'PROCESSING', attempt_count = attempt_count + 1
                    WHERE event_id = %s AND status IN ('PENDING', 'RETRY')
                    RETURNING event_id
                    """,
                    (event_id,),
                )
                return bool(cur.fetchone())

    def mark_event_done(self, event_id: str) -> None:
        self._mark_event_status(event_id, "DONE")

    def event_by_id(self, event_id: str) -> dict[str, Any] | None:
        with self.connection() as conn:
            with conn.cursor() as cur:
                cur.execute("SELECT * FROM event_store WHERE event_id = %s", (event_id,))
                return cur.fetchone()

    def mark_event_error(self, event_id: str, error: str) -> int:
        with self.connection() as conn:
            with conn.cursor() as cur:
                cur.execute("SELECT attempt_count FROM event_store WHERE event_id = %s", (event_id,))
                row = cur.fetchone()
                attempts = int(row["attempt_count"]) if row else 0
                status = "DLQ" if attempts >= SETTINGS.max_retry_attempts else "RETRY"
                payload = {"error": error}
                cur.execute(
                    """
                    UPDATE event_store
                    SET status = %s, payload = payload || %s::jsonb
                    WHERE event_id = %s
                    """,
                    (status, json.dumps(payload), event_id),
                )
                return attempts

    def replayable_events(self, limit: int = 500) -> list[dict[str, Any]]:
        with self.connection() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    "SELECT event_id, source_system, payload, status, attempt_count, created_at FROM event_store WHERE status IN ('RETRY', 'DLQ') ORDER BY created_at ASC LIMIT %s",
                    (limit,),
                )
                return cur.fetchall()

    def idempotency_acquire(self, key: str, operation: str) -> bool:
        with self.connection() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    """
                    INSERT INTO idempotency_keys (key, operation, created_at)
                    VALUES (%s, %s, %s)
                    ON CONFLICT(key) DO NOTHING
                    RETURNING key
                    """,
                    (key, operation, utc_now_iso()),
                )
                return bool(cur.fetchone())

    def idempotency_build_key(
        self,
        *,
        source_system: str,
        entity_type: str,
        entity_id: str,
        action: str,
        payload_hash: str,
    ) -> str:
        return f"{source_system}:{entity_type}:{entity_id}:{action}:{payload_hash}"

    def idempotency_store_response(self, key: str, response: dict[str, Any]) -> None:
        with self.connection() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    "UPDATE idempotency_keys SET response_json = %s::jsonb WHERE key = %s",
                    (json.dumps(response), key),
                )

    def idempotency_response(self, key: str) -> dict[str, Any] | None:
        with self.connection() as conn:
            with conn.cursor() as cur:
                cur.execute("SELECT response_json FROM idempotency_keys WHERE key = %s", (key,))
                row = cur.fetchone()
                if not row:
                    return None
                return row["response_json"]

    def save_id_map(self, *, source_system: str, source_id: str, target_system: str, target_id: str) -> None:
        with self.connection() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    """
                    INSERT INTO id_map (source_system, source_id, target_system, target_id, created_at)
                    VALUES (%s, %s, %s, %s, %s)
                    ON CONFLICT DO NOTHING
                    """,
                    (source_system, source_id, target_system, target_id, utc_now_iso()),
                )

    def get_id_map(self, *, source_system: str, source_id: str, target_system: str) -> str | None:
        with self.connection() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    """
                    SELECT target_id FROM id_map
                    WHERE source_system = %s AND source_id = %s AND target_system = %s
                    ORDER BY id DESC
                    LIMIT 1
                    """,
                    (source_system, source_id, target_system),
                )
                row = cur.fetchone()
                return row["target_id"] if row else None

    def set_opt_out(self, phone: str, reason: str) -> None:
        with self.connection() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    """
                    INSERT INTO opt_out (phone, reason, created_at)
                    VALUES (%s, %s, %s)
                    ON CONFLICT(phone) DO UPDATE SET reason = EXCLUDED.reason
                    """,
                    (phone, reason, utc_now_iso()),
                )

    def is_opted_out(self, phone: str) -> bool:
        with self.connection() as conn:
            with conn.cursor() as cur:
                cur.execute("SELECT phone FROM opt_out WHERE phone = %s", (phone,))
                return bool(cur.fetchone())

    def allow_contact(self, phone: str, cooldown_hours: int) -> bool:
        with self.connection() as conn:
            with conn.cursor() as cur:
                cur.execute("SELECT last_contact_at FROM contact_frequency WHERE phone = %s", (phone,))
                row = cur.fetchone()
                if not row:
                    return True
                last = row["last_contact_at"]
                if isinstance(last, str):
                    last_dt = datetime.fromisoformat(last)
                else:
                    last_dt = last
                return (datetime.now(timezone.utc) - last_dt).total_seconds() >= cooldown_hours * 3600

    def mark_contacted(self, phone: str) -> None:
        with self.connection() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    """
                    INSERT INTO contact_frequency (phone, last_contact_at)
                    VALUES (%s, %s)
                    ON CONFLICT(phone) DO UPDATE SET last_contact_at = EXCLUDED.last_contact_at
                    """,
                    (phone, utc_now_iso()),
                )

    def save_daily_report(self, report_date: str, payload: dict[str, Any]) -> None:
        with self.connection() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    """
                    INSERT INTO daily_reports (report_date, payload_json, created_at)
                    VALUES (%s, %s::jsonb, %s)
                    ON CONFLICT(report_date) DO UPDATE SET
                        payload_json = EXCLUDED.payload_json,
                        created_at = EXCLUDED.created_at
                    """,
                    (report_date, json.dumps(payload), utc_now_iso()),
                )

    def report_by_date(self, report_date: str) -> dict[str, Any] | None:
        with self.connection() as conn:
            with conn.cursor() as cur:
                cur.execute("SELECT payload_json FROM daily_reports WHERE report_date = %s", (report_date,))
                row = cur.fetchone()
                return row["payload_json"] if row else None

    def telemetry_targets(self) -> list[dict[str, Any]]:
        with self.connection() as conn:
            with conn.cursor() as cur:
                cur.execute("SELECT * FROM telemetry_targets WHERE status IN ('NEW', 'PENDING') ORDER BY id ASC")
                return cur.fetchall()

    def upsert_telemetry_target(
        self,
        *,
        customer_id: str,
        phone: str,
        machine_id: str | None,
        client_id: str | None,
        required_fields: dict[str, Any],
        status: str = "NEW",
    ) -> None:
        with self.connection() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    """
                    INSERT INTO telemetry_targets
                    (customer_id, machine_id, client_id, phone, required_fields_json, collected_fields_json, status, last_contact_at, created_at)
                    VALUES (%s, %s, %s, %s, %s::jsonb, '{}'::jsonb, %s, NULL, %s)
                    ON CONFLICT DO NOTHING
                    """,
                    (customer_id, machine_id, client_id, phone, json.dumps(required_fields), status, utc_now_iso()),
                )

    def telemetry_target_by_phone(self, phone: str) -> dict[str, Any] | None:
        with self.connection() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    """
                    SELECT * FROM telemetry_targets
                    WHERE phone = %s AND status IN ('NEW', 'PENDING')
                    ORDER BY id DESC LIMIT 1
                    """,
                    (phone,),
                )
                return cur.fetchone()

    def update_telemetry_target_progress(self, *, target_id: int, collected_fields: dict[str, Any], status: str) -> None:
        with self.connection() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    """
                    UPDATE telemetry_targets
                    SET collected_fields_json = %s::jsonb,
                        status = %s,
                        last_contact_at = %s
                    WHERE id = %s
                    """,
                    (json.dumps(collected_fields), status, utc_now_iso(), target_id),
                )

    def create_campaign(
        self,
        *,
        campaign_id: str,
        name: str,
        campaign_type: str,
        status: str,
        schedule: str,
        segment: dict[str, Any],
        template: str,
    ) -> None:
        with self.connection() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    """
                    INSERT INTO campaigns (id, name, type, status, schedule, segment, template, created_at)
                    VALUES (%s, %s, %s, %s, %s, %s::jsonb, %s, %s)
                    ON CONFLICT(id) DO UPDATE SET
                        status = EXCLUDED.status,
                        segment = EXCLUDED.segment,
                        template = EXCLUDED.template
                    """,
                    (campaign_id, name, campaign_type, status, schedule, json.dumps(segment), template, utc_now_iso()),
                )

    def enabled_offer_rules(self) -> list[dict[str, Any]]:
        with self.connection() as conn:
            with conn.cursor() as cur:
                cur.execute("SELECT * FROM offer_rules WHERE enabled = TRUE ORDER BY priority ASC")
                return cur.fetchall()

    def upsert_offer_rule(self, rule: dict[str, Any]) -> None:
        with self.connection() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    """
                    INSERT INTO offer_rules (id, rule_type, predicate, offer_template, sku_list, priority, enabled)
                    VALUES (%(id)s, %(rule_type)s, %(predicate)s::jsonb, %(offer_template)s, %(sku_list)s::jsonb, %(priority)s, %(enabled)s)
                    ON CONFLICT(id) DO UPDATE SET
                        rule_type = EXCLUDED.rule_type,
                        predicate = EXCLUDED.predicate,
                        offer_template = EXCLUDED.offer_template,
                        sku_list = EXCLUDED.sku_list,
                        priority = EXCLUDED.priority,
                        enabled = EXCLUDED.enabled
                    """,
                    {
                        "id": rule["id"],
                        "rule_type": rule.get("rule_type", "machine"),
                        "predicate": json.dumps(rule.get("predicate", {})),
                        "offer_template": rule.get("offer_template", ""),
                        "sku_list": json.dumps(rule.get("sku_list", [])),
                        "priority": int(rule.get("priority", 100)),
                        "enabled": bool(rule.get("enabled", True)),
                    },
                )

    def upsert_campaign_target(
        self,
        *,
        campaign_id: str,
        client_id: str,
        machine_id: str | None,
        phone: str,
        status: str = "NEW",
    ) -> None:
        with self.connection() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    """
                    INSERT INTO campaign_targets (campaign_id, client_id, machine_id, phone, status, attempts, last_attempt_at, last_result)
                    VALUES (%s, %s, %s, %s, %s, 0, NULL, NULL)
                    ON CONFLICT DO NOTHING
                    """,
                    (campaign_id, client_id, machine_id, phone, status),
                )

    def campaign_targets(self, *, campaign_id: str, statuses: tuple[str, ...] = ("NEW", "PENDING", "RETRY")) -> list[dict[str, Any]]:
        with self.connection() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    """
                    SELECT * FROM campaign_targets
                    WHERE campaign_id = %s AND status = ANY(%s)
                    ORDER BY id ASC
                    """,
                    (campaign_id, list(statuses)),
                )
                return cur.fetchall()

    def update_campaign_target_attempt(self, *, target_id: int, status: str, result: str) -> None:
        with self.connection() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    """
                    UPDATE campaign_targets
                    SET status = %s,
                        attempts = attempts + 1,
                        last_attempt_at = %s,
                        last_result = %s
                    WHERE id = %s
                    """,
                    (status, utc_now_iso(), result[:500], target_id),
                )

    def set_conversation_handoff_state(self, *, conversation_id: str, customer_id: str | None, packet: dict[str, Any]) -> None:
        with self.connection() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    """
                    INSERT INTO conversation_state (conversation_id, customer_id, last_intent, context_summary, updated_at)
                    VALUES (%s, %s, 'HANDOFF_ACTIVE', %s, %s)
                    ON CONFLICT(conversation_id) DO UPDATE SET
                        customer_id = EXCLUDED.customer_id,
                        last_intent = EXCLUDED.last_intent,
                        context_summary = EXCLUDED.context_summary,
                        updated_at = EXCLUDED.updated_at
                    """,
                    (conversation_id, customer_id, json.dumps(packet), utc_now_iso()),
                )

    def _mark_event_status(self, event_id: str, status: str) -> None:
        with self.connection() as conn:
            with conn.cursor() as cur:
                cur.execute("UPDATE event_store SET status = %s WHERE event_id = %s", (status, event_id))
