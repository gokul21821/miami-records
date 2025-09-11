"""
Caching utilities for storing and retrieving lookup results.
"""

import json
import pathlib
from typing import Dict, Any

def load_cache(cache_path: pathlib.Path) -> Dict[str, Any]:
    """Load cached results"""
    if cache_path.exists():
        try:
            with open(cache_path, 'r', encoding='utf-8') as f:
                return json.load(f)
        except Exception:
            pass
    return {}

def save_cache(cache: Dict[str, Any], cache_path: pathlib.Path):
    """Save results to cache"""
    try:
        cache_path.parent.mkdir(parents=True, exist_ok=True)
        with open(cache_path, 'w', encoding='utf-8') as f:
            json.dump(cache, f, indent=2, ensure_ascii=False)
    except Exception as e:
        print(f"Warning: Could not save cache: {e}")
