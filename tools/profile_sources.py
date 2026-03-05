import pandas as pd
import json, re, sys
from pathlib import Path

def sniff_phone(s):
    if s is None: return None
    s = re.sub(r"\D+", "", str(s))
    return s

def sniff_serial(s):
    if s is None: return None
    s = str(s).strip().upper()
    s = re.sub(r"\s+", "", s)
    return s

def profile_df(df, name):
    out = {"name": name, "rows": int(len(df)), "cols": []}
    for c in df.columns:
        series = df[c]
        sample = series.dropna().astype(str).head(5).tolist()
        null_pct = float(series.isna().mean())
        out["cols"].append({
            "col": str(c),
            "dtype": str(series.dtype),
            "null_pct": round(null_pct, 4),
            "sample": sample
        })
    return out

def load_file(p: Path):
    if p.suffix.lower() == ".csv":
        return pd.read_csv(p, dtype=str, encoding_errors="ignore")
    if p.suffix.lower() in (".xlsx", ".xls"):
        # lê 1ª aba por padrão; se quiser todas, ajuste aqui
        return pd.read_excel(p, dtype=str)
    raise ValueError(f"unsupported: {p}")

def main():
    root = Path(sys.argv[1])
    files = [p for p in root.rglob("*") if p.suffix.lower() in (".csv",".xlsx",".xls")]
    reports = []
    for f in files:
        try:
            df = load_file(f)
            reports.append(profile_df(df, str(f)))
        except Exception as e:
            reports.append({"name": str(f), "error": str(e)})
    print(json.dumps(reports, ensure_ascii=False, indent=2))

if __name__ == "__main__":
    main()