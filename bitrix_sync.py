#!/usr/bin/env python3

import os
import json
import requests
from sqlalchemy import create_engine, text

DATABASE_URL = os.environ.get("DATABASE_URL")
BITRIX_WEBHOOK_URL = os.environ.get("BITRIX_WEBHOOK_URL")
TENANT_ID = os.environ.get("TENANT_ID", "ibl")

if not DATABASE_URL:
    raise SystemExit("Missing DATABASE_URL")

if not BITRIX_WEBHOOK_URL:
    raise SystemExit("Missing BITRIX_WEBHOOK_URL")

engine = create_engine(DATABASE_URL)


def fetch_open_opportunities(limit=50):

    sql = """
    select
        id,
        serial_number,
        model,
        branch,
        opportunity_type,
        description,
        priority
    from opportunities

    where tenant_id = :t
    and status = 'open'

    order by priority asc, created_at asc
    limit :limit
    """

    with engine.begin() as cx:
        rows = cx.execute(text(sql), {"t": TENANT_ID, "limit": limit}).mappings().all()

    return [dict(r) for r in rows]


def create_bitrix_lead(opp):

    title = f"{opp['opportunity_type']} - {opp['model']} - {opp['serial_number']}"

    comments = f"""
Oportunidade gerada automaticamente

Tipo: {opp['opportunity_type']}
Máquina: {opp['model']}
Chassi: {opp['serial_number']}
Filial: {opp['branch']}

Descrição:
{opp['description']}
"""

    payload = {
        "fields": {
            "TITLE": title,
            "COMMENTS": comments,
            "SOURCE_ID": "OTHER"
        }
    }

    url = BITRIX_WEBHOOK_URL + "crm.lead.add"

    r = requests.post(url, json=payload)

    if r.status_code != 200:
        raise RuntimeError(f"Bitrix error {r.status_code}: {r.text}")

    data = r.json()

    if "result" not in data:
        raise RuntimeError(data)

    return data["result"]


def mark_sent(opportunity_id, bitrix_id):

    sql = """
    update opportunities
    set
        status = 'sent',
        description = description || ' | BitrixID=' || :bid
    where id = :id
    """

    with engine.begin() as cx:
        cx.execute(text(sql), {"id": opportunity_id, "bid": bitrix_id})


def main():

    opps = fetch_open_opportunities()

    if not opps:
        print("No opportunities to sync")
        return

    print(f"Syncing {len(opps)} opportunities...")

    for opp in opps:

        try:

            lead_id = create_bitrix_lead(opp)

            mark_sent(opp["id"], lead_id)

            print(
                f"✓ Sent {opp['serial_number']} "
                f"{opp['opportunity_type']} → Bitrix {lead_id}"
            )

        except Exception as e:

            print(
                f"ERROR sending opportunity {opp['id']}: {str(e)}"
            )


if __name__ == "__main__":
    main()