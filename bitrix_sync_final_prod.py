#!/usr/bin/env python3
"""IBL Opportunity Engine -> Bitrix Sync (production)

Step-by-step behavior:
- Reads Postgres table `opportunities` where status='open' and tenant_id=TENANT_ID
- For each row, creates the right entity in Bitrix (idempotent):
    REVISAO        -> DEAL  (categoryId=12, stageId=C12:NEW)
    TELEMETRIA_OFF -> DEAL  (categoryId=4,  stageId=C4:UC_L2OMKR)
    ALERTA_TECNICO -> SPA1088 (stageId today=DT1088_32:UC_7QDHAK else DT1088_32:NEW)
- Enriches with `machine_master` (same tenant_id + serial_number)
- Avoids duplicates using Bitrix ORIGINATOR_ID + ORIGIN_ID (crm.deal.*) and originatorId+originId (crm.item.*)
- Assigns responsible by *Oficina* department (real Bitrix departments you listed):
    MT->42, MS->46, AC->96, AM->98, RO->100, RR->102, Sinop->168
  Then selects an ACTIVE user inside that department:
    - Prefer roles matching keywords (default: coordenador/líder/gerente/supervisor)
    - Else deterministic hash(serial+type) across dept users
  Fallback: ASSIGNED_DEFAULT_USER_ID (default 1)

Required env:
- BITRIX_WEBHOOK_URL
- DATABASE_URL  (postgresql://... is ok)

Optional env:
- TENANT_ID (default: ibl)
- DRY_RUN=1  (no create)
- LIMIT=200
- ASSIGNED_DEFAULT_USER_ID=1
- ASSIGN_LEAD_KEYWORDS='coordenador,lider,líder,gerente,supervisor'
"""

from __future__ import annotations
import hashlib, os, sys, re
from dataclasses import dataclass
from datetime import datetime, date
from typing import Any, Dict, List, Optional

import requests
from sqlalchemy import create_engine, text

TENANT_ID = os.getenv("TENANT_ID", "ibl").strip()
BITRIX_WEBHOOK_URL = os.getenv("BITRIX_WEBHOOK_URL", "").strip()
DATABASE_URL = os.getenv("DATABASE_URL", "").strip()
DRY_RUN = os.getenv("DRY_RUN", "0").lower() in ("1","true","yes")
LIMIT = int(os.getenv("LIMIT", "200"))
ASSIGNED_DEFAULT_USER_ID = int(os.getenv("ASSIGNED_DEFAULT_USER_ID", "1"))

# Routing
DEAL_REVISAO_CATEGORY_ID = 12
DEAL_REVISAO_STAGE_ID = "C12:NEW"
DEAL_TELE_OFF_CATEGORY_ID = 4
DEAL_TELE_OFF_STAGE_ID = "C4:UC_L2OMKR"

SPA_ALERT_ENTITY_TYPE_ID = 1088
SPA_ALERT_STAGE_TODAY = "DT1088_32:UC_7QDHAK"
SPA_ALERT_STAGE_LATE  = "DT1088_32:NEW"

# Deal required enums (from your field dumps)
DEAL_UF_CRM_PRECISO_DE = "UF_CRM_6867E43E95146"  # required
DEAL_UF_CRM_MARCA      = "UF_CRM_683855AED1E7E"  # required
DEAL_UF_CRM_ESTADO     = "UF_CRM_ESTADO"         # required

PRECISO_DE_IDS = {
  "REVISAO": "254",          # Revisão
  "TELEMETRIA_OFF": "258",   # Diagnóstico (Oficina)
}
MARCA_IDS = {"CASE":"140", "DYNAPAC":"142"}
ESTADO_ENUM_ID_BY_UF = {"MT":"56","MS":"58","AM":"60","RO":"62","RR":"64","AC":"66"}

# Oficina departments
OFICINA_DEPT_BY_UF = {"MT":42,"MS":46,"AC":96,"AM":98,"RO":100,"RR":102}
OFICINA_DEPT_SINOP = 168

LEAD_KEYWORDS = [k.strip().lower() for k in os.getenv(
    "ASSIGN_LEAD_KEYWORDS",
    "coordenador,lider,líder,gerente,supervisor"
).split(",") if k.strip()]

CITY_OR_BRANCH_TO_UF = {
    "boa vista":"RR", "rio branco":"AC", "manaus":"AM", "porto velho":"RO", "campo grande":"MS",
    "cuiaba":"MT", "cuiabá":"MT", "varzea grande":"MT", "várzea grande":"MT",
    "agua boa":"MT", "água boa":"MT", "sinop":"MT", "sn":"MT",
}

def die(msg:str)->None:
    print(f"[ERROR] {msg}", file=sys.stderr)
    raise SystemExit(2)

def clean(s: Any) -> str:
    return ("" if s is None else str(s)).strip()

def stable_hash_int(s:str)->int:
    h = hashlib.sha256(s.encode("utf-8")).hexdigest()
    return int(h[:12], 16)

def b24_call(method:str, payload:Optional[dict]=None)->dict:
    if not BITRIX_WEBHOOK_URL:
        die("BITRIX_WEBHOOK_URL not set")
    url = BITRIX_WEBHOOK_URL.rstrip("/") + f"/{method}.json"
    if DRY_RUN and method.endswith(".add"):
        return {"result":"DRY_RUN"}
    r = requests.post(url, json=payload or {}, timeout=60)
    r.raise_for_status()
    data = r.json()
    if "error" in data:
        raise RuntimeError(f"Bitrix error {data.get('error')}: {data.get('error_description')}")
    return data

def originator_id()->str:
    return f"opp-engine:{TENANT_ID}"

def origin_id(opp_type:str, serial:str)->str:
    return f"{opp_type}:{serial}"

def parse_yyyy_mm_dd(text_:str)->Optional[date]:
    m = re.search(r"\b(20\d{2}-\d{2}-\d{2})\b", text_ or "")
    if not m: return None
    try: return datetime.strptime(m.group(1), "%Y-%m-%d").date()
    except Exception: return None

def infer_uf(branch:str, portal_state:str, warranty_state:str)->Optional[str]:
    b = clean(branch).lower()
    if b in CITY_OR_BRANCH_TO_UF:
        return CITY_OR_BRANCH_TO_UF[b]
    for field in (portal_state, warranty_state):
        f = clean(field).upper()
        if f in ESTADO_ENUM_ID_BY_UF:
            return f
    return None

def infer_brand(model:str)->str:
    m = clean(model).upper()
    if "DYNAPAC" in m:
        return "DYNAPAC"
    return "CASE"

@dataclass
class B24User:
    id:int
    active:bool
    work_position:str
    departments:List[int]

def load_b24_users()->List[B24User]:
    data = b24_call("user.get", {})
    out=[]
    for u in data.get("result",[]):
        try: uid=int(u.get("ID"))
        except Exception: continue
        out.append(B24User(
            id=uid,
            active=bool(u.get("ACTIVE")),
            work_position=clean(u.get("WORK_POSITION")).lower(),
            departments=[int(x) for x in (u.get("UF_DEPARTMENT") or []) if str(x).isdigit()]
        ))
    return out

def dept_for(branch:str, uf:Optional[str])->Optional[int]:
    b = clean(branch).lower()
    if b in ("sinop","sn"):
        return OFICINA_DEPT_SINOP
    if uf and uf in OFICINA_DEPT_BY_UF:
        return OFICINA_DEPT_BY_UF[uf]
    return None

def choose_assignee(users:List[B24User], dept_id:Optional[int], serial:str, opp_type:str)->int:
    if not dept_id:
        return ASSIGNED_DEFAULT_USER_ID
    cand=[u for u in users if u.active and dept_id in u.departments]
    if not cand:
        return ASSIGNED_DEFAULT_USER_ID
    for u in cand:
        if any(k in u.work_position for k in LEAD_KEYWORDS):
            return u.id
    idx = stable_hash_int(f"{serial}:{opp_type}") % len(cand)
    return cand[idx].id

def sa_engine():
    if not DATABASE_URL:
        die("DATABASE_URL not set")
    url = DATABASE_URL
    if url.startswith("postgresql://") and "+psycopg2" not in url:
        url = url.replace("postgresql://","postgresql+psycopg2://",1)
    return create_engine(url, pool_pre_ping=True)

def fetch_open_opps(conn)->List[dict]:
    q=text("""
      select id, tenant_id, serial_number, model, branch, opportunity_type,
             priority, description, created_at, source_table, status
      from opportunities
      where status='open' and tenant_id=:tenant
      order by priority desc, created_at asc
      limit :limit
    """)
    rows = conn.execute(q,{"tenant":TENANT_ID,"limit":LIMIT}).mappings().all()
    return [dict(r) for r in rows]

def fetch_mm(conn, tenant:str, serial:str)->Optional[dict]:
    q=text("""select * from machine_master where tenant_id=:t and serial_number=:s limit 1""")
    r=conn.execute(q,{"t":tenant,"s":serial}).mappings().first()
    return dict(r) if r else None

def mark_status(conn, opp_id:int, status:str)->None:
    conn.execute(text("update opportunities set status=:st where id=:id"),{"st":status,"id":opp_id})

def find_deal(originator:str, origin:str)->Optional[int]:
    data=b24_call("crm.deal.list", {"filter": {"=ORIGINATOR_ID":originator, "=ORIGIN_ID":origin}, "select":["ID"]})
    res=data.get("result") or []
    if not res: return None
    try: return int(res[0]["ID"])
    except Exception: return None

def create_deal(fields:dict, originator:str, origin:str, assigned:int)->int:
    payload={"fields":{**fields,"ORIGINATOR_ID":originator,"ORIGIN_ID":origin,"ASSIGNED_BY_ID":assigned}}
    data=b24_call("crm.deal.add", payload)
    if data.get("result")=="DRY_RUN": return -1
    return int(data["result"])

def find_spa(originator:str, origin:str)->Optional[int]:
    data=b24_call("crm.item.list", {"entityTypeId":SPA_ALERT_ENTITY_TYPE_ID, "filter": {"=originatorId":originator, "=originId":origin}, "select":["id"]})
    items=(data.get("result") or {}).get("items") or []
    if not items: return None
    try: return int(items[0]["id"])
    except Exception: return None

def create_spa(fields:dict, originator:str, origin:str)->int:
    data=b24_call("crm.item.add", {"entityTypeId":SPA_ALERT_ENTITY_TYPE_ID, "fields": {**fields, "originatorId":originator, "originId":origin}})
    if data.get("result")=="DRY_RUN": return -1
    return int(((data.get("result") or {}).get("item") or {}).get("id"))

def timeline_comment(entity_type:str, entity_id:int, comment:str)->None:
    b24_call("crm.timeline.comment.add", {"fields": {"ENTITY_TYPE":entity_type, "ENTITY_ID":entity_id, "COMMENT":comment}})

def spa_stage_for(row:dict)->str:
    ca=row.get("created_at")
    if isinstance(ca, datetime) and ca.date()==datetime.now(ca.tzinfo).date():
        return SPA_ALERT_STAGE_TODAY
    d=parse_yyyy_mm_dd(clean(row.get("description")))
    if d and d==datetime.now().date():
        return SPA_ALERT_STAGE_TODAY
    return SPA_ALERT_STAGE_LATE

def main()->int:
    if not BITRIX_WEBHOOK_URL: die("Set BITRIX_WEBHOOK_URL")
    if not DATABASE_URL: die("Set DATABASE_URL")

    users=load_b24_users()
    eng=sa_engine()

    created=skipped=errors=0
    with eng.begin() as conn:
        opps=fetch_open_opps(conn)
        if not opps:
            print("[OK] No open opportunities.")
            return 0

        for row in opps:
            opp_id=row["id"]
            opp_type=row["opportunity_type"]
            serial=row["serial_number"]
            try:
                mm=fetch_mm(conn, row["tenant_id"], serial) or {}
                branch = clean(row.get("branch") or mm.get("branch"))
                model  = clean(row.get("model")  or mm.get("model"))
                uf = infer_uf(branch, mm.get("portal_state"), mm.get("warranty_state")) or "MT"
                dept = dept_for(branch, uf)
                assigned = choose_assignee(users, dept, serial, opp_type)

                origator=originator_id()
                orig=origin_id(opp_type, serial)

                if opp_type in ("REVISAO","TELEMETRIA_OFF"):
                    if opp_type=="REVISAO":
                        cat=DEAL_REVISAO_CATEGORY_ID; stage=DEAL_REVISAO_STAGE_ID
                    else:
                        cat=DEAL_TELE_OFF_CATEGORY_ID; stage=DEAL_TELE_OFF_STAGE_ID

                    existing=find_deal(origator, orig)
                    if existing:
                        skipped += 1
                        if not DRY_RUN:
                            timeline_comment("deal", existing, f"[SYNC] Já existia (idempotente). UF={uf} Assigned={assigned}")
                        mark_status(conn, opp_id, "synced")
                        print(f"[SKIP] deal id={existing} {orig}")
                        continue

                    brand=infer_brand(model)
                    fields={
                        "TITLE": f"[{opp_type}] {model} — {serial}",
                        "CATEGORY_ID": cat,
                        "STAGE_ID": stage,
                        "UF_CRM_CIDADE": branch,
                        DEAL_UF_CRM_PRECISO_DE: PRECISO_DE_IDS.get(opp_type, "258"),
                        DEAL_UF_CRM_MARCA: MARCA_IDS.get(brand, "140"),
                        DEAL_UF_CRM_ESTADO: ESTADO_ENUM_ID_BY_UF.get(uf, "56"),
                    }
                    deal_id=create_deal(fields, origator, orig, assigned)
                    if deal_id!=-1 and not DRY_RUN:
                        timeline_comment("deal", deal_id, f"[SYNC] Criado pelo Opportunity Engine. UF={uf} Assigned={assigned}")
                    mark_status(conn, opp_id, "synced")
                    created += 1
                    print(f"[OK] deal id={deal_id} type={opp_type} serial={serial} assigned={assigned}")

                elif opp_type=="ALERTA_TECNICO":
                    existing=find_spa(origator, orig)
                    if existing:
                        skipped += 1
                        if not DRY_RUN:
                            timeline_comment("dynamic", existing, f"[SYNC] Já existia (idempotente). UF={uf} Assigned={assigned}")
                        mark_status(conn, opp_id, "synced")
                        print(f"[SKIP] spa id={existing} {orig}")
                        continue

                    stage=spa_stage_for(row)
                    fields={
                        "title": f"[ALERTA] {model} — {serial}",
                        "stageId": stage,
                        "assignedById": assigned,
                        "opened": True,
                        # from your SPA fields dump:
                        "ufCrm24_1755642721411": model,
                        "ufCrm24_1755888328": branch,
                    }
                    spa_id=create_spa(fields, origator, orig)
                    if spa_id!=-1 and not DRY_RUN:
                        timeline_comment("dynamic", spa_id, f"[SYNC] Criado pelo Opportunity Engine. UF={uf} Assigned={assigned}\n{clean(row.get('description'))}")
                    mark_status(conn, opp_id, "synced")
                    created += 1
                    print(f"[OK] spa id={spa_id} serial={serial} stage={stage} assigned={assigned}")
                else:
                    mark_status(conn, opp_id, "ignored")
                    print(f"[WARN] ignored type={opp_type} id={opp_id}")

            except Exception as e:
                errors += 1
                print(f"[ERROR] opp_id={opp_id} type={opp_type} serial={serial}: {e}", file=sys.stderr)

    print(f"[DONE] created={created} skipped={skipped} errors={errors} dry_run={DRY_RUN}")
    return 0 if errors==0 else 1

if __name__=="__main__":
    raise SystemExit(main())
