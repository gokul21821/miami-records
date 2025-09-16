# src/miami_mor_step2.py
"""
Step 2: Multi-day MORTGAGE - MOR records extraction with checkpointing and consolidation.

This script processes date ranges, handles checkpointing for resumability,
and creates month-wise consolidated files for pilot preparation.
"""

import os
import time
import json
import csv
import argparse
import datetime as dt
from typing import Dict, Any, List, Optional
from pathlib import Path
import requests
from dotenv import load_dotenv

# Load environment variables from .env file
load_dotenv()

# Import functions from step1
try:
    # Try relative import first (when run as part of package)
    from .miami_mor_step1 import (
        build_session,
        get_qs,
        get_standard_records,
        ensure_dirs,
        write_json,
        post_standard_search  # For error handling consistency
    )
except ImportError:
    # Fall back to absolute import (when run as script)
    try:
        from miami_mor_step1 import (
            build_session,
            get_qs,
            get_standard_records,
            ensure_dirs,
            write_json,
            post_standard_search  # For error handling consistency
        )
    except ImportError:
        # Add src directory to path and try again
        import sys
        from pathlib import Path
        src_path = Path(__file__).parent
        if str(src_path) not in sys.path:
            sys.path.insert(0, str(src_path))

        from miami_mor_step1 import (
            build_session,
            get_qs,
            get_standard_records,
            ensure_dirs,
            write_json,
            post_standard_search  # For error handling consistency
        )

DEFAULT_DOC_TYPE = "MORTGAGE - MOR"

def generate_date_range(start_date: str, end_date: str) -> List[str]:
    """Generate list of dates from start_date to end_date (inclusive)."""
    start = dt.date.fromisoformat(start_date)
    end = dt.date.fromisoformat(end_date)
    if start > end:
        raise ValueError("start_date must be before or equal to end_date")

    dates = []
    current = start
    while current <= end:
        dates.append(current.isoformat())
        current += dt.timedelta(days=1)
    return dates

def load_qs_map(qs_map_path: str) -> Dict[str, str]:
    """Load QS mapping from JSON file for manual mode."""
    with open(qs_map_path, 'r', encoding='utf-8') as f:
        return json.load(f)

def load_state(state_path: str) -> Dict[str, Any]:
    """Load processing state from JSON file."""
    if os.path.exists(state_path):
        with open(state_path, 'r', encoding='utf-8') as f:
            return json.load(f)
    return {"processed_dates": [], "last_processed": None}

def save_state(state_path: str, state: Dict[str, Any]) -> None:
    """Save processing state to JSON file."""
    ensure_dirs(os.path.dirname(state_path))
    with open(state_path, 'w', encoding='utf-8') as f:
        json.dump(state, f, indent=2, ensure_ascii=False)

def is_date_processed(date_str: str, out_root: str, document_type: str) -> bool:
    """Check if a date has already been processed by looking for summary.json."""
    # Convert document type to folder name (e.g., "MORTGAGE - MOR" -> "MOR")
    doc_folder = document_type.replace(" - ", "_").replace(" ", "_").upper()
    summary_path = os.path.join(out_root, date_str, doc_folder, "summary.json")
    return os.path.exists(summary_path)

def write_jsonl_record(file_path: str, record: Dict[str, Any]) -> None:
    """Append a single record to JSONL file."""
    ensure_dirs(os.path.dirname(file_path))
    with open(file_path, 'a', encoding='utf-8') as f:
        json.dump(record, f, ensure_ascii=False)
        f.write('\n')

def write_error_log(error_path: str, error_msg: str, response_text: str = "", date_str: str = "", document_type: str = "") -> None:
    """Write error details to a log file for diagnosis."""
    try:
        ensure_dirs(os.path.dirname(error_path))
        with open(error_path, 'w', encoding='utf-8') as f:
            f.write(f"Error: {error_msg}\n")
            if date_str:
                f.write(f"Date: {date_str}\n")
            if document_type:
                f.write(f"Document Type: {document_type}\n")
            if response_text:
                f.write(f"Response (first 500 chars): {response_text[:500]}\n")
            f.write(f"Timestamp: {dt.datetime.utcnow().isoformat() + 'Z'}\n")
    except Exception as log_error:
        # Fallback: write to a general error log if specific error path fails
        fallback_path = os.path.join("data", "bronze", "errors", f"{date_str or 'unknown'}_error.log")
        try:
            ensure_dirs(os.path.dirname(fallback_path))
            with open(fallback_path, 'a', encoding='utf-8') as f:
                f.write(f"[{dt.datetime.utcnow().isoformat() + 'Z'}] Failed to write to {error_path}: {log_error}\n")
                f.write(f"Original error: {error_msg}\n")
                if response_text:
                    f.write(f"Response: {response_text[:500]}\n")
                f.write("---\n")
        except Exception:
            # Last resort: just print to console
            print(f"CRITICAL: Could not write error log. Original error: {error_msg}")


def get_error_log_path(out_root: str, date_str: str, document_type: str, specific_file: str = "error.log") -> str:
    """Get a safe error log path, with fallback to general error directory."""
    doc_folder = document_type.replace(" - ", "_").replace(" ", "_").upper()
    primary_path = os.path.join(out_root, date_str, doc_folder, specific_file)

    # Check if primary path is accessible
    try:
        ensure_dirs(os.path.dirname(primary_path))
        # Test if we can write to the directory
        test_file = os.path.join(os.path.dirname(primary_path), "test_write.tmp")
        with open(test_file, 'w') as f:
            f.write("test")
        os.remove(test_file)
        return primary_path
    except Exception:
        # Fallback to general error directory
        return os.path.join("data", "bronze", "errors", f"{date_str}_{specific_file}")

def write_monthly_csv(csv_path: str, date_str: str, count: int, overflow: bool) -> None:
    """Append daily summary to monthly CSV file."""
    ensure_dirs(os.path.dirname(csv_path))
    file_exists = os.path.exists(csv_path)

    with open(csv_path, 'a', newline='', encoding='utf-8') as f:
        writer = csv.writer(f)
        if not file_exists:
            writer.writerow(['date', 'count', 'overflow_500_cap'])
        writer.writerow([date_str, count, overflow])

def rebuild_monthly_files(dates: List[str], out_root: str, document_type: str) -> None:
    """Rebuild monthly JSONL and CSV files from existing daily records, grouped by month."""
    if not dates:
        return

    # Group dates by month
    dates_by_month = {}
    for date_str in dates:
        date_obj = dt.date.fromisoformat(date_str)
        month_str = f"{date_obj.year}-{date_obj.month:02d}"
        if month_str not in dates_by_month:
            dates_by_month[month_str] = []
        dates_by_month[month_str].append(date_str)

    doc_folder = document_type.replace(" - ", "_").replace(" ", "_").upper()

    # Process each month separately
    for month_str, month_dates in dates_by_month.items():
        # Paths for monthly files
        monthly_dir = os.path.join(out_root, "monthly", doc_folder)
        jsonl_path = os.path.join(monthly_dir, f"{month_str}_records.jsonl")
        csv_path = os.path.join(monthly_dir, f"{month_str}_summary.csv")

        # Clear existing files if they exist
        if os.path.exists(jsonl_path):
            os.remove(jsonl_path)
        if os.path.exists(csv_path):
            os.remove(csv_path)

        # Rebuild from daily records for this month
        for date_str in month_dates:
            day_dir = os.path.join(out_root, date_str, doc_folder)
            records_path = os.path.join(day_dir, "records.json")
            summary_path = os.path.join(day_dir, "summary.json")

            if os.path.exists(records_path) and os.path.exists(summary_path):
                # Load records and summary
                with open(records_path, 'r', encoding='utf-8') as f:
                    records = json.load(f)

                with open(summary_path, 'r', encoding='utf-8') as f:
                    summary = json.load(f)

                # Append each record to JSONL
                for record in records:
                    write_jsonl_record(jsonl_path, record)

                # Append to CSV
                write_monthly_csv(csv_path, date_str, summary['count'], summary['overflow_500_cap'])

        print(f"Rebuilt monthly files for {month_str}: {len(month_dates)} dates")

def process_date(
    date_str: str,
    session: requests.Session,
    document_type: str,
    mode: str,
    qs_arg: Optional[str],
    out_root: str,
    sleep_sec: float = 1.0
) -> Dict[str, Any]:
    """Process a single date and return summary information."""
    print(f"Processing date: {date_str}")

    # Get QS for this date
    qs = get_qs(session, date_str, document_type, mode, qs_arg)
    print(f"Using QS: {qs[:50]}...")

    # Sleep to be polite
    if sleep_sec > 0:
        time.sleep(sleep_sec)

    # Fetch records
    try:
        records = get_standard_records(session, qs)
    except RuntimeError as e:
        error_str = str(e)
        if "Non-JSON response" in error_str:
            # Extract response text if available in the error message
            response_text = ""
            if "|RESPONSE_TEXT:" in error_str:
                response_text = error_str.split("|RESPONSE_TEXT:")[1]

            # Get safe error log path with fallback
            error_path = get_error_log_path(out_root, date_str, document_type, "error.log")
            write_error_log(error_path, error_str.split("|RESPONSE_TEXT:")[0], response_text, date_str, document_type)
            print(f"Error logged to {error_path}")
        raise

    # Calculate overflow flag
    count = len(records) if isinstance(records, list) else 0
    overflow = count >= 500

    # Create document folder name
    doc_folder = document_type.replace(" - ", "_").replace(" ", "_").upper()

    # Define day_dir early for error logging
    day_dir = os.path.join(out_root, date_str, doc_folder)

    # Write daily files
    ensure_dirs(day_dir)

    records_path = os.path.join(day_dir, "records.json")
    write_json(records_path, records)

    # Extract cookie keys for auditability
    cookie_keys = []
    cookies_env = os.getenv("COOKIES", "")
    if cookies_env:
        for cookie_pair in cookies_env.split(';'):
            cookie_pair = cookie_pair.strip()
            if '=' in cookie_pair:
                name, _ = cookie_pair.split('=', 1)
                cookie_keys.append(name.strip())

    # Create summary
    summary = {
        "date": date_str,
        "document_type": document_type,
        "mode": mode,
        "count": count,
        "overflow_500_cap": overflow,
        "qs_used": qs,
        "cookie_keys_present": cookie_keys,
        "saved_to": records_path,
        "fetched_at": dt.datetime.utcnow().isoformat() + "Z",
    }

    summary_path = os.path.join(day_dir, "summary.json")
    write_json(summary_path, summary)

    print(f"Completed: {count} records, overflow={overflow}")
    return summary

def main():
    parser = argparse.ArgumentParser(
        description="Step 2: Multi-day MORTGAGE - MOR records extraction with checkpointing"
    )
    parser.add_argument("--start-date", required=True, help="Start date (YYYY-MM-DD)")
    parser.add_argument("--end-date", required=True, help="End date (YYYY-MM-DD)")
    parser.add_argument("--document-type", default=DEFAULT_DOC_TYPE, help='e.g., "MORTGAGE - MOR"')
    parser.add_argument("--mode", choices=["auto", "manual"], default="manual",
                       help="Mode: auto=generate QS via API, manual=use provided QS")
    parser.add_argument("--qs-map", help="JSON file mapping dates to QS values (required for manual mode)")
    parser.add_argument("--cookies", default=os.getenv("COOKIES"),
                       help="Cookie string from browser (semicolon-delimited)")
    parser.add_argument("--out-root", default="data/bronze", help="Root output folder")
    parser.add_argument("--state-path", default="data/state/mor_step2_state.json",
                       help="Path to state file for checkpointing")
    parser.add_argument("--sleep-sec", type=float, default=1.0,
                       help="Seconds to sleep between requests")
    parser.add_argument("--force", action="store_true",
                       help="Force reprocessing of already completed dates")

    args = parser.parse_args()

    # Validate arguments
    try:
        dt.date.fromisoformat(args.start_date)
        dt.date.fromisoformat(args.end_date)
    except ValueError as e:
        raise SystemExit(f"Invalid date format: {e}")

    if not args.cookies:
        raise SystemExit("Missing cookies: set COOKIES env var or pass via --cookies")

    if args.mode == "manual" and not args.qs_map:
        raise SystemExit("Manual mode requires --qs-map to be provided")

    # Generate date range first
    dates = generate_date_range(args.start_date, args.end_date)

    # Load and validate QS map for manual mode
    qs_map = None
    if args.mode == "manual":
        if not os.path.exists(args.qs_map):
            raise SystemExit(f"QS map file not found: {args.qs_map}")
        qs_map = load_qs_map(args.qs_map)

        # Validate that all dates in range have QS values
        missing_dates = []
        for date_str in dates:
            if date_str not in qs_map:
                missing_dates.append(date_str)

        if missing_dates:
            print(f"Warning: Missing QS values for {len(missing_dates)} dates:")
            for date in missing_dates[:10]:  # Show first 10
                print(f"  - {date}")
            if len(missing_dates) > 10:
                print(f"  ... and {len(missing_dates) - 10} more")
            print("\nPlease add QS values for these dates in your qs_map.json file.")
            print("Continuing with available dates only...")

    print(f"Processing {len(dates)} dates from {args.start_date} to {args.end_date}")

    # Build session
    session = build_session(args.cookies)

    # Load state
    state = load_state(args.state_path)

    # Process each date
    processed_dates = []
    failed_dates = []

    for date_str in dates:
        try:
            # Check if already processed (unless force is set)
            if not args.force and is_date_processed(date_str, args.out_root, args.document_type):
                print(f"Skipping {date_str} (already processed)")
                continue

            # Get QS for manual mode
            qs_arg = None
            if args.mode == "manual":
                if date_str not in qs_map:
                    print(f"Warning: No QS found for {date_str} in qs_map, skipping")
                    continue
                qs_arg = qs_map[date_str]

            # Process the date
            summary = process_date(
                date_str=date_str,
                session=session,
                document_type=args.document_type,
                mode=args.mode,
                qs_arg=qs_arg,
                out_root=args.out_root,
                sleep_sec=args.sleep_sec
            )

            processed_dates.append(date_str)

            # Update state
            state["processed_dates"] = processed_dates
            state["last_processed"] = date_str
            save_state(args.state_path, state)

        except RuntimeError as e:
            if "Authentication error" in str(e) or "Please refresh cookies" in str(e):
                print(f"Authentication error on {date_str}: {e}")
                print("Please refresh cookies from browser DevTools and restart")
                break
            else:
                print(f"Error processing {date_str}: {e}")
                failed_dates.append(date_str)
        except Exception as e:
            print(f"Unexpected error processing {date_str}: {e}")
            failed_dates.append(date_str)

    # Rebuild monthly files for processed dates
    if processed_dates:
        print(f"Rebuilding monthly files for {len(processed_dates)} processed dates...")
        rebuild_monthly_files(processed_dates, args.out_root, args.document_type)

    # Final summary
    print(f"\nProcessing complete:")
    print(f"- Total dates: {len(dates)}")
    print(f"- Processed: {len(processed_dates)}")
    print(f"- Failed: {len(failed_dates)}")
    if failed_dates:
        print(f"- Failed dates: {', '.join(failed_dates)}")

if __name__ == "__main__":
    main()
