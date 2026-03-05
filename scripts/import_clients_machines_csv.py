from __future__ import annotations

import csv
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "src"))

from agent_system.db import Database, utc_now_iso


def main() -> None:
    if len(sys.argv) < 2:
        print("usage: import_clients_machines_csv.py <csv_file>")
        raise SystemExit(1)
    path = Path(sys.argv[1])
    if not path.exists():
        print(f"csv file not found: {path}")
        raise SystemExit(1)

    db = Database()
    with path.open("r", newline="", encoding="utf-8") as f:
        reader = csv.DictReader(f)
        for row in reader:
            client_id = row["client_id"].strip()
            machine_id = row["machine_id"].strip()
            db.add_client(
                {
                    "id": client_id,
                    "name": row["client_name"].strip(),
                    "phone": row["phone"].strip(),
                    "city": row.get("city", "").strip(),
                    "state": row.get("state", "").strip(),
                }
            )
            db.add_customer(
                {
                    "id": client_id,
                    "name": row["client_name"].strip(),
                    "phone": row["phone"].strip(),
                    "region": row.get("state", "").strip(),
                    "store": row.get("city", "").strip(),
                }
            )
            db.add_machine(
                {
                    "id": machine_id,
                    "client_id": client_id,
                    "brand": row.get("brand", "").strip(),
                    "model": row.get("model", "").strip(),
                    "serial": row.get("serial", "").strip(),
                    "year": int(row.get("year", "0") or 0),
                    "telemetry_status": "active" if row.get("telemetry_active", "").lower() == "true" else "inactive",
                    "telemetry_active": row.get("telemetry_active", "").lower() == "true",
                    "created_at": utc_now_iso(),
                }
            )
            db.add_machine_ownership(client_id=client_id, machine_id=machine_id, start_at=utc_now_iso())

    print(f"import complete: {path}")


if __name__ == "__main__":
    main()
