from __future__ import annotations

import json
from pathlib import Path
from typing import Any, Dict

ROOT_DIR = Path(__file__).resolve().parents[2]
STATE_PATH = ROOT_DIR / "data" / "state" / "gui_state.json"


def load_state() -> Dict[str, Any]:
    try:
        if STATE_PATH.exists():
            loaded_state = json.loads(STATE_PATH.read_text(encoding="utf-8"))
            # Merge with defaults to handle new fields
            merged_state = get_default_state().copy()
            merged_state.update(loaded_state)
            return merged_state
    except Exception:
        pass
    return get_default_state()


def get_default_state() -> Dict[str, Any]:
    """Get default state values"""
    from src.config.doc_types import get_label_from_code, DEFAULT_DOC_TYPE

    default_label = get_label_from_code(DEFAULT_DOC_TYPE)

    return {
        # Document type selections for each tab
        'fetch_doc_type': default_label,
        'csv_doc_type': default_label,
        'enrich_doc_type': default_label,

        # Existing fields
        'cookies': '',
        'fetch_start': '2025-01-01',
        'fetch_end': '2025-01-31',
        'csv_start': '2025-01-01',
        'csv_end': '2025-01-31',
        'sleep_sec': '1.0',
        'month': '',
    }


def save_state(state: Dict[str, Any]) -> None:
    try:
        STATE_PATH.parent.mkdir(parents=True, exist_ok=True)
        STATE_PATH.write_text(json.dumps(state, indent=2, ensure_ascii=False), encoding="utf-8")
    except Exception:
        pass


