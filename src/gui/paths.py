from __future__ import annotations

import datetime as dt
from pathlib import Path
from typing import List, Optional, Tuple

DOC_FOLDER = "MORTGAGE_MOR"

ROOT_DIR = Path(__file__).resolve().parents[2]
SILVER_DIR = ROOT_DIR / "data" / "silver" / "monthly" / DOC_FOLDER
GOLD_DIR = ROOT_DIR / "data" / "gold" / "monthly" / DOC_FOLDER


def month_key_from_date(d: dt.date) -> str:
    return f"{d.year:04d}-{d.month:02d}"


def parse_iso_date(s: str) -> dt.date:
    return dt.date.fromisoformat(s)


def months_in_range(start_iso: str, end_iso: str) -> List[str]:
    start = parse_iso_date(start_iso)
    end = parse_iso_date(end_iso)
    if start > end:
        start, end = end, start

    months: List[str] = []
    cur = dt.date(start.year, start.month, 1)
    last = dt.date(end.year, end.month, 1)
    while cur <= last:
        months.append(month_key_from_date(cur))
        if cur.month == 12:
            cur = dt.date(cur.year + 1, 1, 1)
        else:
            cur = dt.date(cur.year, cur.month + 1, 1)
    return months


def normalized_csv_path(month: str) -> Path:
    return SILVER_DIR / f"{month}_normalized.csv"


def normalized_clean_csv_path(month: str) -> Path:
    return SILVER_DIR / f"{month}_normalized_clean.csv"


def enriched_csv_path(month: str) -> Path:
    return GOLD_DIR / f"{month}_enriched.csv"


def discover_available_months() -> List[str]:
    months = set()
    if SILVER_DIR.exists():
        for p in SILVER_DIR.glob("*_normalized*.csv"):
            name = p.name
            if "_normalized" in name:
                month = name.split("_normalized")[0]
                months.add(month)
    return sorted(months)


def pick_enrichment_input(month: str) -> Tuple[Optional[Path], str]:
    cleaned = normalized_clean_csv_path(month)
    if cleaned.exists():
        return cleaned, "normalized_clean"
    base = normalized_csv_path(month)
    if base.exists():
        return base, "normalized"
    return None, "missing"


