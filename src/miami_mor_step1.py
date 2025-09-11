# src/miami_mor_step1.py
import os
import time
import json
import argparse
import datetime as dt
from typing import Dict, Any, List, Optional
import requests
from dotenv import load_dotenv

# Load environment variables from .env file
load_dotenv()

BASE_URL = "https://onlineservices.miamidadeclerk.gov/officialrecords/api"
STANDARD_SEARCH = f"{BASE_URL}/home/standardsearch"
GET_RECORDS = f"{BASE_URL}/SearchResults/getStandardRecords"

DEFAULT_DOC_TYPE = "MORTGAGE - MOR"

def build_session(
    cookies_str: str,
    user_agent: Optional[str] = None
) -> requests.Session:
    s = requests.Session()
    ua = user_agent or (
        "Mozilla/5.0 (X11; Linux aarch64) AppleWebKit/537.36 (KHTML, like Gecko) "
        "Chrome/139.0.0.0 Safari/537.36 CrKey/1.54.250320"
    )
    s.headers.update({
        "User-Agent": ua,
        "Accept": "*/*",
        "Accept-Language": "en-US,en;q=0.9,ta;q=0.8",
        "Connection": "keep-alive",
        "Sec-Fetch-Dest": "empty",
        "Sec-Fetch-Mode": "cors",
        "Sec-Fetch-Site": "same-origin",
        "Origin": "https://onlineservices.miamidadeclerk.gov",
    })

    # Parse and set cookies from the cookie string
    if cookies_str:
        # Split by semicolon and parse each cookie
        for cookie_pair in cookies_str.split(';'):
            cookie_pair = cookie_pair.strip()
            if '=' in cookie_pair:
                name, value = cookie_pair.split('=', 1)
                s.cookies.set(name.strip(), value.strip(), domain="onlineservices.miamidadeclerk.gov")

    return s

def post_standard_search(
    session: requests.Session,
    date_str: str,
    document_type: str = DEFAULT_DOC_TYPE,
    retries: int = 3,
    backoff_sec: float = 1.5
) -> str:
    params = {
        "partyName": "",
        "dateRangeFrom": date_str,
        "dateRangeTo": date_str,  # daily granularity
        "documentType": document_type,
        "searchT": document_type,
        "firstQuery": "y",
        "searchtype": "Name/Document",
        "token": "",
    }

    # Ensure Origin header is set for POST requests
    original_headers = session.headers.copy()
    session.headers.update({
        "Content-Type": "application/x-www-form-urlencoded",
        "Referer": "https://onlineservices.miamidadeclerk.gov/officialrecords/",
    })

    try:
        # The endpoint expects POST with Content-Length: 0, parameters in query string
        # We send data="" to keep body empty
        for attempt in range(1, retries + 1):
            resp = session.post(STANDARD_SEARCH, params=params, data=b"")
            if resp.ok:
                try:
                    data = resp.json()
                    # Response shape may be {"qs": "..."} or similar; support common keys
                    qs = data.get("qs") or data.get("QS") or data.get("result") or ""
                    if not qs:
                        raise ValueError(f"QS missing in response keys={list(data.keys())}")
                    return qs
                except json.JSONDecodeError as e:
                    error_msg = f"Non-JSON response (status {resp.status_code}): {resp.text[:500]}"
                    if attempt < retries:
                        print(f"Attempt {attempt} failed: {error_msg}. Retrying...")
                        time.sleep(backoff_sec * attempt)
                        continue
                    # Include response text in the exception for error logging
                    raise RuntimeError(f"{error_msg}|RESPONSE_TEXT:{resp.text[:500]}")
            elif resp.status_code in [403, 401]:
                error_msg = f"Authentication error (status {resp.status_code}): {resp.text[:500]}"
                if attempt < retries:
                    print(f"Attempt {attempt} failed: {error_msg}. Retrying...")
                    time.sleep(backoff_sec * attempt)
                    continue
                raise RuntimeError(f"{error_msg}. Please refresh cookies from browser DevTools.")
            else:
                if attempt < retries:
                    time.sleep(backoff_sec * attempt)
                else:
                    resp.raise_for_status()
        raise RuntimeError("Unreachable")
    finally:
        # Restore original headers
        session.headers.clear()
        session.headers.update(original_headers)

def get_qs(
    session: requests.Session,
    date_str: str,
    document_type: str,
    mode: str,
    qs_arg: Optional[str] = None
) -> str:
    """Get QS either from argument (manual mode) or by calling standard search (auto mode)"""
    if mode == "manual":
        if not qs_arg:
            raise SystemExit("Manual mode requires --qs to be provided")
        return qs_arg
    elif mode == "auto":
        return post_standard_search(session, date_str, document_type)
    else:
        raise SystemExit(f"Invalid mode: {mode}. Must be 'auto' or 'manual'")

def get_standard_records(
    session: requests.Session,
    qs: str,
    retries: int = 3,
    backoff_sec: float = 1.5
) -> List[Dict[str, Any]]:
    params = {"qs": qs}

    # Set the referer header for this specific request
    referer_url = f"https://onlineservices.miamidadeclerk.gov/officialrecords/SearchResults?qs={qs}"

    # Store original headers to restore later
    original_headers = session.headers.copy()
    session.headers.update({"Referer": referer_url})

    try:
        for attempt in range(1, retries + 1):
            resp = session.get(GET_RECORDS, params=params)
            if resp.ok:
                try:
                    data = resp.json()
                    # Response may be either a dict with "recordingModels" or a list
                    if isinstance(data, dict) and "recordingModels" in data:
                        return data["recordingModels"]
                    if isinstance(data, list):
                        return data
                    # Some responses include searchCritiriea and recordingModels (sample provided)
                    # Extract conservatively if present
                    recs = data.get("recordingModels")
                    if recs is not None:
                        return recs
                    raise ValueError(f"Unexpected records response shape keys={list(data.keys())}")
                except json.JSONDecodeError as e:
                    error_msg = f"Non-JSON response (status {resp.status_code}): {resp.text[:500]}"
                    if attempt < retries:
                        print(f"Attempt {attempt} failed: {error_msg}. Retrying...")
                        time.sleep(backoff_sec * attempt)
                        continue
                    # Include response text in the exception for error logging
                    raise RuntimeError(f"{error_msg}|RESPONSE_TEXT:{resp.text[:500]}")
            elif resp.status_code in [403, 401]:
                error_msg = f"Authentication error (status {resp.status_code}): {resp.text[:500]}"
                if attempt < retries:
                    print(f"Attempt {attempt} failed: {error_msg}. Retrying...")
                    time.sleep(backoff_sec * attempt)
                    continue
                raise RuntimeError(f"{error_msg}. Please refresh cookies from browser DevTools.")
            else:
                if attempt < retries:
                    time.sleep(backoff_sec * attempt)
                else:
                    resp.raise_for_status()
        raise RuntimeError("Unreachable")
    finally:
        # Restore original headers
        session.headers.clear()
        session.headers.update(original_headers)

def ensure_dirs(path: str) -> None:
    os.makedirs(path, exist_ok=True)

def write_json(path: str, obj: Any) -> None:
    with open(path, "w", encoding="utf-8") as f:
        json.dump(obj, f, ensure_ascii=False, indent=2)

def main():
    parser = argparse.ArgumentParser(description="Step 1: Fetch daily MORTGAGE - MOR records")
    parser.add_argument("--date", required=True, help="YYYY-MM-DD (daily granularity)")
    parser.add_argument("--qs", help="Query string token for the search (required in manual mode)")
    parser.add_argument("--mode", choices=["auto", "manual"], default="manual", help="Mode: auto=generate QS via API, manual=use provided QS")
    parser.add_argument("--document-type", default=DEFAULT_DOC_TYPE, help='e.g., "MORTGAGE - MOR"')
    parser.add_argument("--cookies", default=os.getenv("COOKIES"), help="Full cookie string from browser")
    parser.add_argument("--out-root", default="data/bronze", help="Root output folder")
    args = parser.parse_args()

    # Validate date
    dt.date.fromisoformat(args.date)

    if not args.cookies:
        raise SystemExit("Missing cookies: set COOKIES env var or pass via --cookies")

    # Validate mode-specific requirements
    if args.mode == "manual" and not args.qs:
        raise SystemExit("Manual mode requires --qs to be provided")
    elif args.mode == "auto" and not args.document_type:
        raise SystemExit("Auto mode requires --document-type to be provided")

    session = build_session(args.cookies)

    # Get QS based on mode
    qs = get_qs(session, args.date, args.document_type, args.mode, args.qs)

    # Fetch records
    records = get_standard_records(session, qs)

    day_dir = os.path.join(args.out_root, args.date, "MOR")
    ensure_dirs(day_dir)
    out_json = os.path.join(day_dir, "records.json")
    write_json(out_json, records)

    count = len(records) if isinstance(records, list) else 0
    overflow = count >= 500

    # Extract cookie keys for auditability (not values)
    cookie_keys = []
    if args.cookies:
        for cookie_pair in args.cookies.split(';'):
            cookie_pair = cookie_pair.strip()
            if '=' in cookie_pair:
                name, _ = cookie_pair.split('=', 1)
                cookie_keys.append(name.strip())

    summary = {
        "date": args.date,
        "document_type": args.document_type,
        "mode": args.mode,
        "count": count,
        "overflow_500_cap": overflow,
        "qs_used": qs,
        "cookie_keys_present": cookie_keys,
        "saved_to": out_json,
        "fetched_at": dt.datetime.utcnow().isoformat() + "Z",
    }
    write_json(os.path.join(day_dir, "summary.json"), summary)

    print(json.dumps(summary, indent=2))

if __name__ == "__main__":
    main()
