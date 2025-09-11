"""
Command-line interface for the AnyWho phone enrichment tool.
"""

import argparse
import time
import pathlib
import pandas as pd
from typing import Dict, Any
import sys

# Ensure line-buffered stdout/stderr when possible for live GUI logs
try:
    sys.stdout.reconfigure(line_buffering=True)
    sys.stderr.reconfigure(line_buffering=True)
except Exception:
    pass

from src.scrapers.anywho_scraper import build_session, enrich_name
from src.utils.caching import load_cache, save_cache
from src.utils.file_handlers import (
    load_or_create_enriched_df, update_enriched_df,
    calculate_row_range, validate_row_range
)
from src.config.settings import DEFAULT_SLEEP_SEC, DEFAULT_CACHE_PATH

def create_parser() -> argparse.ArgumentParser:
    """Create command line argument parser"""
    parser = argparse.ArgumentParser(description="Enrich borrower data with phone numbers from AnyWho")
    parser.add_argument("--test", action="store_true", help="Run test with sample data")
    parser.add_argument("--cache-path", default=DEFAULT_CACHE_PATH, help="Cache file path")
    parser.add_argument("--sleep-sec", type=float, default=DEFAULT_SLEEP_SEC, help="Delay between requests")
    parser.add_argument("--refresh", action="store_true", help="Ignore cache and refresh lookups")
    parser.add_argument("--max-rows", type=int, help="Maximum number of rows to process")
    parser.add_argument("--start-row", type=int, default=0, help="Starting row index (0-based) - DEPRECATED: Use --from-row instead")
    parser.add_argument("--end-row", type=int, help="Ending row index (0-based) - DEPRECATED: Use --to-row instead")

    # Enhanced row range arguments (human-friendly)
    parser.add_argument("--from-row", type=int, help="Starting row number (1-based, human-friendly)")
    parser.add_argument("--to-row", type=int, help="Ending row number (1-based, human-friendly)")
    parser.add_argument("--limit", type=int, help="Maximum number of rows to process from start")
    parser.add_argument("--last", type=int, help="Process last N rows of the file")

    parser.add_argument("input_file", nargs='?', help="Input CSV file (normalized)")
    parser.add_argument("output_file", nargs='?', help="Output CSV file (enriched)")
    
    return parser

def process_file(args):
    """Process a file with the given arguments"""
    print(f"Enhanced AnyWho Phone Enrichment")
    print(f"Input: {args.input_file}")
    print(f"Output: {args.output_file}")
    print(f"Cache: {args.cache_path}")
    print(f"Sleep: {args.sleep_sec}s between requests")
    
    # Load data
    try:
        df = pd.read_csv(args.input_file)
        print(f"Loaded {len(df)} rows from {args.input_file}")
    except Exception as e:
        print(f"Error loading input file: {e}")
        return
    
    # Validate required columns
    required_cols = ['Name', 'Address']
    missing_cols = [col for col in required_cols if col not in df.columns]
    if missing_cols:
        print(f"Error: Missing required columns: {missing_cols}")
        return

    # Load or create enriched dataframe (incremental updates)
    enriched_df = load_or_create_enriched_df(args.output_file, df)

    # Ensure enriched dataframe has the same core columns as input
    for col in required_cols:
        if col not in enriched_df.columns:
            if col in df.columns:
                enriched_df[col] = df[col]
            else:
                print(f"Error: Required column '{col}' missing from both input and existing enriched file")
                return

    # Initialize phone columns in input dataframe for processing
    df['Phone1'] = ''
    df['Phone2'] = ''
    df['Phone3'] = ''
    df['Phone4'] = ''

    # Load cache
    cache_path = pathlib.Path(args.cache_path)
    cache = {} if args.refresh else load_cache(cache_path)
    
    # Build session
    session = build_session()
    
    # Process rows - Enhanced row range calculation
    start_idx, end_idx = calculate_row_range(df, args)

    # Validate the row range
    if not validate_row_range(start_idx, end_idx, len(df), args):
        return

    total_rows = end_idx - start_idx
    processed = 0

    # Enhanced progress display with human-friendly row numbers
    start_display = start_idx + 1  # Convert to 1-based for display
    end_display = end_idx  # Already 1-based equivalent
    print(f"Processing rows {start_display} to {end_display} (out of {len(df)} total)")
    print(f"Range: {total_rows} records")
    
    for idx in range(start_idx, end_idx):
        row = df.iloc[idx]
        name = str(row['Name']).strip()
        addr = str(row['Address']).strip()
        
        if not name:
            continue
        
        # Create cache key
        cache_key = f"{name}|{addr}"
        
        processed += 1
        current_row = start_display + processed - 1  # Human-friendly current row number
        print(f"[{processed}/{total_rows}] Row {current_row}: {name} - {addr}")
        
        # Check cache first
        if cache_key in cache and not args.refresh:
            cached_result = cache[cache_key]
            df.at[idx, 'Phone1'] = cached_result.get('phone1', '')
            df.at[idx, 'Phone2'] = cached_result.get('phone2', '')
            df.at[idx, 'Phone3'] = cached_result.get('phone3', '')
            df.at[idx, 'Phone4'] = cached_result.get('phone4', '')
            print(f"  Cached: {cached_result.get('phone1', 'None')}, {cached_result.get('phone2', 'None')}, {cached_result.get('phone3', 'None')}, {cached_result.get('phone4', 'None')}")
            continue
        
        # Perform lookup
        phone1, phone2, phone3, phone4, candidates = enrich_name(session, name, addr, sleep_sec=args.sleep_sec)
        
        # Update dataframe
        df.at[idx, 'Phone1'] = phone1 or ''
        df.at[idx, 'Phone2'] = phone2 or ''
        df.at[idx, 'Phone3'] = phone3 or ''
        df.at[idx, 'Phone4'] = phone4 or ''
        
        # Cache result
        cache[cache_key] = {
            'phone1': phone1,
            'phone2': phone2,
            'phone3': phone3,
            'phone4': phone4,
            'timestamp': time.time(),
            'candidates_count': len(candidates)
        }
        
        print(f"  Result: {phone1 or 'None'}, {phone2 or 'None'}, {phone3 or 'None'}, {phone4 or 'None'}")
        
        # Save cache periodically
        if processed % 10 == 0:
            save_cache(cache, cache_path)
    
    # Final cache save
    save_cache(cache, cache_path)
    
    # Update enriched dataframe with processed results and save
    try:
        output_path = pathlib.Path(args.output_file)
        output_path.parent.mkdir(parents=True, exist_ok=True)

        # Update only the processed rows in the enriched dataframe
        enriched_df = update_enriched_df(enriched_df, df, start_idx, end_idx, processed)

        # Save the updated enriched dataframe
        enriched_df.to_csv(output_path, index=False)
        print(f"Saved enriched data to {args.output_file}")

        # Print summary for entire enriched file
        phone1_count = (enriched_df['Phone1'] != '').sum()
        phone2_count = (enriched_df['Phone2'] != '').sum()
        phone3_count = (enriched_df['Phone3'] != '').sum() if 'Phone3' in enriched_df.columns else 0
        phone4_count = (enriched_df['Phone4'] != '').sum() if 'Phone4' in enriched_df.columns else 0
        print(f"Summary: P1={phone1_count}, P2={phone2_count}, P3={phone3_count}, P4={phone4_count}")
        print(f"Total rows in enriched file: {len(enriched_df)}")

        # Print summary for this processing session
        processed_phone1 = (df['Phone1'] != '').sum()
        processed_phone2 = (df['Phone2'] != '').sum()
        processed_phone3 = (df['Phone3'] != '').sum()
        processed_phone4 = (df['Phone4'] != '').sum()
        print(f"This session: P1={processed_phone1}, P2={processed_phone2}, P3={processed_phone3}, P4={processed_phone4} in rows {start_display}-{end_display}")

    except Exception as e:
        print(f"Error saving output file: {e}")

def test_enhanced_scraping():
    """Test the enhanced scraping with sample data"""
    print("Testing Enhanced AnyWho Scraping...")
    
    # Test data
    test_cases = [
        ("PASCUAL MARIO I", "123 MAIN ST MIAMI FL"),
        ("GARCIA MARIA", "456 OCEAN DR MIAMI FL"), 
        ("RODRIGUEZ JOSE", "789 BISCAYNE BLVD MIAMI FL")
    ]
    
    session = build_session()
    
    for name, addr in test_cases:
        print(f"\n--- Testing: {name} at {addr} ---")
        phone1, phone2, phone3, phone4, candidates = enrich_name(session, name, addr, sleep_sec=0.5)
        
        print(f"Results: Phone1={phone1}, Phone2={phone2}, Phone3={phone3}, Phone4={phone4}")
        print(f"Found {len(candidates)} candidates")
        
        for i, candidate in enumerate(candidates[:3]):  # Show top 3
            print(f"  {i+1}. {candidate['name']} - {candidate['phone']}")
            print(f"     Address: {candidate['address']}")
            from src.processors.data_processor import normalize_name, normalize_address
            from src.algorithms.scoring import score_candidate
            print(f"     Score: {score_candidate(normalize_name(name), normalize_address(addr), candidate):.1f}")
