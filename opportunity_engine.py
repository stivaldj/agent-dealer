#!/usr/bin/env python3

import os
import json
from sqlalchemy import create_engine, text

DATABASE_URL = os.environ.get("DATABASE_URL")
TENANT_ID = os.environ.get("TENANT_ID", "ibl")

engine = create_engine(DATABASE_URL)


def ensure_table():

    sql = """
    create table if not exists opportunities (

        id bigserial primary key,
        tenant_id text,
        serial_number text,
        model text,
        branch text,

        opportunity_type text,
        priority int,

        description text,

        status text default 'open',

        created_at timestamptz default now(),

        unique (tenant_id, serial_number, opportunity_type)
    );
    """

    with engine.begin() as cx:
        cx.execute(text(sql))


def generate_service():

    sql = """
    insert into opportunities
    (
        tenant_id,
        serial_number,
        model,
        branch,
        opportunity_type,
        priority,
        description
    )

    select
        tenant_id,
        serial_number,
        model,
        branch,
        'REVISAO',
        2,
        'Revisão prevista nos próximos 30 dias'

    from machine_master

    where tenant_id = :t
    and next_service_date <= current_date + interval '30 days'

    on conflict (tenant_id, serial_number, opportunity_type)
    do nothing
    """

    with engine.begin() as cx:
        cx.execute(text(sql), {"t": TENANT_ID})


def generate_alerts():

    sql = """
    insert into opportunities
    (
        tenant_id,
        serial_number,
        model,
        branch,
        opportunity_type,
        priority,
        description
    )

    select
        tenant_id,
        serial_number,
        model,
        branch,
        'ALERTA_TECNICO',
        1,
        'Máquina possui alertas ativos'

    from machine_master

    where tenant_id = :t
    and open_alerts > 0

    on conflict (tenant_id, serial_number, opportunity_type)
    do nothing
    """

    with engine.begin() as cx:
        cx.execute(text(sql), {"t": TENANT_ID})


def generate_telemetry():

    sql = """
    insert into opportunities
    (
        tenant_id,
        serial_number,
        model,
        branch,
        opportunity_type,
        priority,
        description
    )

    select
        tenant_id,
        serial_number,
        model,
        branch,
        'TELEMETRIA_OFF',
        3,
        'Máquina parou de transmitir telemetria'

    from machine_master

    where tenant_id = :t
    and telemetry_active = false

    on conflict (tenant_id, serial_number, opportunity_type)
    do nothing
    """

    with engine.begin() as cx:
        cx.execute(text(sql), {"t": TENANT_ID})


def proof():

    with engine.begin() as cx:

        rows = cx.execute(text("""
        select opportunity_type, count(*)
        from opportunities
        where tenant_id = :t
        group by opportunity_type
        """), {"t": TENANT_ID}).mappings().all()

        print("\n=== OPPORTUNITIES ===\n")

        print(json.dumps([dict(r) for r in rows], indent=2))


def main():

    ensure_table()

    generate_service()

    generate_alerts()

    generate_telemetry()

    proof()


if __name__ == "__main__":
    main()