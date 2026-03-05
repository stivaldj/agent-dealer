#!/usr/bin/env python3
import os
import re
import json
import math
from pathlib import Path
from datetime import datetime

import pandas as pd
from dateutil import parser as dateparser
from sqlalchemy import create_engine, text

DATABASE_URL = os.environ.get("DATABASE_URL")
if not DATABASE_URL:
    raise SystemExit("Missing DATABASE_URL")

TENANT_ID = os.environ.get("TENANT_ID", "ibl")
DATA_INBOX = Path(os.environ.get("DATA_INBOX", "data_inbox")).resolve()

BRANCH_MAP = {
    2: "Varzea Grande",
    8: "Agua Boa",
    9: "Sinop",
}

BRANCH_ALIASES = {
    "Várzea Grande": "Varzea Grande",
    "Varzea Grande": "Varzea Grande",
    "Agua Boa": "Agua Boa",
    "Água Boa": "Agua Boa",
    "Sinop": "Sinop",
}

def norm_branch(x):
    if x is None or (isinstance(x, float) and math.isnan(x)):
        return None
    s = str(x).strip()
    return BRANCH_ALIASES.get(s, s)

def norm_serial(x):
    if x is None or (isinstance(x, float) and math.isnan(x)):
        return None
    s = str(x).strip().upper()
    s = re.sub(r"\s+", "", s)
    s = s.replace("-", "")
    return s or None

def norm_numeric(x):
    if x is None:
        return None
    s = str(x).replace(",", ".")
    try:
        return float(s)
    except:
        return None

def parse_dt(x):
    if x is None:
        return None
    try:
        return dateparser.parse(str(x), dayfirst=True)
    except:
        return None

def parse_date(x):
    dt = parse_dt(x)
    return dt.date() if dt else None


def read_excel_first_sheet(path):
    return pd.read_excel(path, dtype=str)


def read_csv_robust(path):
    for enc in ("utf-8", "latin-1"):
        try:
            return pd.read_csv(path, dtype=str, encoding=enc)
        except:
            pass
    raise RuntimeError("Cannot read CSV")


def ensure_schema(engine):
    ddl = """
    create table if not exists tenants(
        id text primary key,
        name text
    );

    create table if not exists telemetry_population(
        id bigserial primary key,
        tenant_id text,
        serial_number text,
        model text,
        branch text,
        current_hours numeric,
        last_comm_at timestamptz
    );

    create table if not exists services_due(
        id bigserial primary key,
        tenant_id text,
        serial_number text,
        model text,
        branch text,
        estimated_service_date date,
        current_hours numeric,
        warranty_state text
    );

    create table if not exists alerts(
        id bigserial primary key,
        tenant_id text,
        serial_number text,
        model text,
        branch text,
        description text,
        status text,
        opened_at timestamptz
    );

    create table if not exists insights(
        id bigserial primary key,
        tenant_id text,
        serial_number text,
        description text,
        severity text,
        opened_at timestamptz
    );

    create table if not exists telemetry_fleet(
        id bigserial primary key,
        tenant_id text,
        serial_number text,
        model text,
        branch text,
        current_hours numeric,
        last_comm_at timestamptz
    );

    create table if not exists machine_master(
        tenant_id text,
        serial_number text primary key,
        model text,
        branch text,
        current_hours numeric,
        last_comm_at timestamptz,
        telemetry_active boolean,
        next_service_date date,
        warranty_state text,
        open_alerts int default 0
    );
    """

    with engine.begin() as cx:
        cx.execute(text(ddl))
        cx.execute(text(
            "insert into tenants(id,name) values(:id,'IBL') on conflict do nothing"
        ), {"id": TENANT_ID})


def reset_tables(engine):
    """
    IMPORTANTE
    evita duplicação quando o ETL roda mais de uma vez
    """
    with engine.begin() as cx:
        cx.execute(text("""
        TRUNCATE
            telemetry_population,
            services_due,
            alerts,
            insights,
            telemetry_fleet,
            machine_sales
        RESTART IDENTITY
        CASCADE;
        """))


def upsert_dataframe(engine, df, table):
    df.to_sql(table, engine, if_exists="append", index=False)


def load_population(engine, path):
    df = read_excel_first_sheet(path)

    df2 = pd.DataFrame({
        "tenant_id": TENANT_ID,
        "serial_number": df["Chassi"].map(norm_serial),
        "model": df["Modelo"],
        "branch": df["Filial"].map(norm_branch),
        "current_hours": df["Horímetro"].map(norm_numeric),
        "last_comm_at": df["Última comunicação"].map(parse_dt),
    })

    upsert_dataframe(engine, df2, "telemetry_population")
    return len(df2)


def load_services_due(engine, path):
    df = read_excel_first_sheet(path)

    df2 = pd.DataFrame({
        "tenant_id": TENANT_ID,
        "serial_number": df["Chassi"].map(norm_serial),
        "model": df["Modelo"],
        "branch": df["Filial"].map(norm_branch),
        "estimated_service_date": df["Data estimada de serviço"].map(parse_date),
        "current_hours": df["Horímetro"].map(norm_numeric),
        "warranty_state": df["Estado atual de garantia"]
    })

    upsert_dataframe(engine, df2, "services_due")
    return len(df2)


def load_alerts(engine, path):
    df = read_excel_first_sheet(path)

    df2 = pd.DataFrame({
        "tenant_id": TENANT_ID,
        "serial_number": df["Chassi"].map(norm_serial),
        "model": df["Modelo"],
        "branch": df["Filial"].map(norm_branch),
        "description": df["Descrição"],
        "status": df["Estado do alerta"],
        "opened_at": df["Abertura"].map(parse_dt),
    })

    upsert_dataframe(engine, df2, "alerts")
    return len(df2)


def load_insights(engine, path):
    df = read_excel_first_sheet(path)

    df2 = pd.DataFrame({
        "tenant_id": TENANT_ID,
        "serial_number": df["Chassi"].map(norm_serial),
        "description": df["Description PT-BR"],
        "severity": df["Prioridade"],
        "opened_at": df["Abertura"].map(parse_dt),
    })

    upsert_dataframe(engine, df2, "insights")
    return len(df2)


def load_fleet(engine, path):
    df = read_excel_first_sheet(path)

    df2 = pd.DataFrame({
        "tenant_id": TENANT_ID,
        "serial_number": df["Chassi"].map(norm_serial),
        "model": df["Modelo"],
        "branch": df["Filial"].map(norm_branch),
        "current_hours": df["Horímetro"].map(norm_numeric),
        "last_comm_at": df["Última Comunicação"].map(parse_dt),
    })

    upsert_dataframe(engine, df2, "telemetry_fleet")
    return len(df2)


def refresh_machine_master(engine):
    sql = """
    insert into machine_master
    select
        tenant_id,
        serial_number,
        model,
        branch,
        current_hours,
        last_comm_at,
        case when last_comm_at > now() - interval '14 days'
            then true else false end,
        null,
        null,
        0
    from telemetry_population
    where serial_number is not null
    on conflict (serial_number)
    do update set
        current_hours = excluded.current_hours,
        last_comm_at = excluded.last_comm_at;
    """

    with engine.begin() as cx:
        cx.execute(text(sql))


def proof(engine):

    checks = {}

    with engine.begin() as cx:

        for table in [
            "telemetry_population",
            "services_due",
            "alerts",
            "insights",
            "telemetry_fleet",
            "machine_master"
        ]:

            n = cx.execute(text(f"select count(*) from {table}")).scalar()
            checks[f"count.{table}"] = int(n)

    print(json.dumps(checks, indent=2, default=str))


def main():

    engine = create_engine(DATABASE_URL)

    ensure_schema(engine)

    # 🔴 CORREÇÃO CRÍTICA
    reset_tables(engine)

    loaded = []

    for p in DATA_INBOX.glob("*.xlsx"):

        name = p.name.lower()

        if "popula" in name:
            loaded.append(("telemetry_population", p.name, load_population(engine, p)))

        elif "servi" in name:
            loaded.append(("services_due", p.name, load_services_due(engine, p)))

        elif "alerta" in name:
            loaded.append(("alerts", p.name, load_alerts(engine, p)))

        elif "insight" in name:
            loaded.append(("insights", p.name, load_insights(engine, p)))

        elif name.startswith("data"):
            loaded.append(("telemetry_fleet", p.name, load_fleet(engine, p)))

    refresh_machine_master(engine)

    print("=== LOADED FILES ===")

    for t, fn, n in loaded:
        print(f"{t}: {fn} -> {n} rows")

    print("\n=== PROOF ===")

    proof(engine)


if __name__ == "__main__":
    main()