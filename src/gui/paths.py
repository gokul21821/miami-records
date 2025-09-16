from __future__ import annotations

import datetime as dt
from pathlib import Path
from typing import List, Optional, Tuple

DOC_FOLDER = "MORTGAGE_MOR"

ROOT_DIR = Path(__file__).resolve().parents[2]


def doc_folder_for(code_or_label: str) -> str:
    """Convert document type code/label to folder name."""
    from ..config.doc_types import get_folder_name
    return get_folder_name(code_or_label)


def silver_dir_for(doc_folder: str = DOC_FOLDER) -> Path:
    """Get silver directory for document type."""
    return ROOT_DIR / "data" / "silver" / "monthly" / doc_folder


def gold_dir_for(doc_folder: str = DOC_FOLDER) -> Path:
    """Get gold directory for document type."""
    return ROOT_DIR / "data" / "gold" / "monthly" / doc_folder


# Backward compatibility
SILVER_DIR = silver_dir_for()
GOLD_DIR = gold_dir_for()


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


def normalized_csv_path(month: str, doc_folder: str = DOC_FOLDER) -> Path:
    silver_dir = silver_dir_for(doc_folder)
    return silver_dir / f"{month}_normalized.csv"


def normalized_clean_csv_path(month: str, doc_folder: str = DOC_FOLDER) -> Path:
    silver_dir = silver_dir_for(doc_folder)
    return silver_dir / f"{month}_normalized_clean.csv"


def enriched_csv_path(month: str, doc_folder: str = DOC_FOLDER) -> Path:
    gold_dir = gold_dir_for(doc_folder)
    return gold_dir / f"{month}_enriched.csv"


def discover_available_months(doc_folder: str = DOC_FOLDER) -> List[str]:
    silver_dir = silver_dir_for(doc_folder)
    months = set()
    if silver_dir.exists():
        for p in silver_dir.glob("*_normalized*.csv"):
            name = p.name
            if "_normalized" in name:
                month = name.split("_normalized")[0]
                months.add(month)
    return sorted(months)


def pick_enrichment_input(month: str, doc_folder: str = DOC_FOLDER) -> Tuple[Optional[Path], str]:
    cleaned = normalized_clean_csv_path(month, doc_folder)
    if cleaned.exists():
        return cleaned, "normalized_clean"
    base = normalized_csv_path(month, doc_folder)
    if base.exists():
        return base, "normalized"
    return None, "missing"


