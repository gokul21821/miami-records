# src/miami_mor_step3.py
import os, json, argparse, datetime as dt
from pathlib import Path
from typing import List, Dict, Any, Optional
import pandas as pd

# Company detection removed - not needed with simplified borrower identification

def normalize_address(rec: Dict[str, Any]) -> str:
    parts = []
    if rec.get("addressnounit"):
        parts.append(str(rec["addressnounit"]).strip())
    if rec.get("addressunit"):
        parts.append(str(rec["addressunit"]).strip())
    if rec.get("address"):
        parts.append(str(rec["address"]).strip())
    # Deduplicate tokens
    seen, out = set(), []
    for p in parts:
        if p and p not in seen:
            seen.add(p)
            out.append(p)
    return " ".join(out).strip()

# Borrower identification logic removed - partY_CODE "D" guarantees borrower is firsT_PARTY

def map_record(rec: Dict[str, Any]) -> Dict[str, Any]:
    # partY_CODE "D" guarantees borrower is firsT_PARTY
    borrower = (rec.get("firsT_PARTY") or "").strip()
    address = normalize_address(rec)
    # Dates may be like "1/2/2025 12:00:00 AM" — use recording date as primary
    doc_date = (rec.get("reC_DATE") or rec.get("doC_DATE") or "").strip()
    # Consideration_1 appears to hold loan amount in sample payloads
    loan_amount = rec.get("consideratioN_1")
    # Interest rate not in metadata; leave blank for now
    return {
        "Name": borrower,
        "Address": address,
        "Phone1": "",
        "Phone2": "",
        "Phone3": "",
        "Phone4": "",
        "Rate of Interest": "",
        "Loan Amount": loan_amount if loan_amount is not None else "",
        "Date of Document": doc_date,
        "Doc Type": (rec.get("doC_TYPE") or "").strip(),
        "CFN_Master_ID": rec.get("cfN_MASTER_ID") or "",
        "Rec_Book": str(rec.get("reC_BOOK") or ""),
        "Rec_Page": str(rec.get("reC_PAGE") or ""),
        "Rec_BookPage": rec.get("reC_BOOKPAGE") or "",
        "Book_Type": (rec.get("booK_TYPE") or "").strip(),
    }

def process_day(day_dir: Path) -> pd.DataFrame:
    records_path = day_dir / "records.json"
    if not records_path.exists():
        return pd.DataFrame()
    with open(records_path, "r", encoding="utf-8") as f:
        data = json.load(f)
    # records.json may be a list; if dict with key recordingModels, extract that list
    if isinstance(data, dict) and "recordingModels" in data:
        data = data["recordingModels"]
    if not isinstance(data, list):
        return pd.DataFrame()
    rows = []
    for rec in data:
        # Filter to keep only "D" (Direct) records - borrower as first party
        party_code = rec.get("partY_CODE", "")
        if party_code != "D":
            continue  # Skip "R" (Reverse) records - lender as first party

        mapped = map_record(rec)
        # Include all remaining records (no duplicate or company filters)
        rows.append(mapped)

    return pd.DataFrame(rows)

def month_key(date_str: str) -> str:
    d = dt.date.fromisoformat(date_str)
    return f"{d.year}-{d.month:02d}"

def main():
    ap = argparse.ArgumentParser(description="Step 3: Normalize daily MOR records into borrower-centric rows")
    ap.add_argument("--start-date", required=True)
    ap.add_argument("--end-date", required=True)
    ap.add_argument("--document-type", default="MORTGAGE - MOR")
    ap.add_argument("--bronze-root", default="data/bronze")
    ap.add_argument("--silver-root", default="data/silver")
    ap.add_argument("--force", action="store_true")
    args = ap.parse_args()

    start = dt.date.fromisoformat(args.start_date)
    end = dt.date.fromisoformat(args.end_date)

    doc_folder = args.document_type.replace(" - ", "_").replace(" ", "_").upper()

    # Group dates per month
    cur = start
    by_month: Dict[str, List[str]] = {}
    while cur <= end:
        mk = f"{cur.year}-{cur.month:02d}"
        by_month.setdefault(mk, []).append(cur.isoformat())
        cur += dt.timedelta(days=1)

    for mk, dates in by_month.items():
        # Build month output path
        out_dir = Path(args.silver_root) / "monthly" / doc_folder
        out_dir.mkdir(parents=True, exist_ok=True)
        out_csv = out_dir / f"{mk}_normalized.csv"

        # If not --force and CSV exists, rebuild only if new days are found
        if out_csv.exists() and not args.force:
            print(f"Skipping month {mk} (exists). Use --force to rebuild.")
            continue

        month_frames = []
        for date_str in dates:
            day_dir = Path(args.bronze_root) / date_str / doc_folder
            df = process_day(day_dir)
            if not df.empty:
                # add date column for traceability
                df.insert(0, "Date", date_str)
                month_frames.append(df)

        if month_frames:
            month_df = pd.concat(month_frames, ignore_index=True)
        else:
            month_df = pd.DataFrame(columns=[
                "Date","Name","Address","Phone1","Phone2","Phone3","Phone4","Rate of Interest",
                "Loan Amount","Date of Document","Doc Type","CFN_Master_ID",
                "Rec_Book","Rec_Page","Rec_BookPage","Book_Type"
            ])

        month_df.to_csv(out_csv, index=False, encoding="utf-8")
        print(f"Wrote {len(month_df)} rows -> {out_csv}")

if __name__ == "__main__":
    main()
