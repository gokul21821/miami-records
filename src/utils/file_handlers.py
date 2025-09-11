"""
File handling utilities for CSV processing and dataframe operations.
"""

import pathlib
import pandas as pd
from typing import Tuple

def load_or_create_enriched_df(output_path: str, input_df: pd.DataFrame) -> pd.DataFrame:
    """Load existing enriched CSV or create new one with phone columns"""
    output_path_obj = pathlib.Path(output_path)

    if output_path_obj.exists():
        try:
            # Load existing enriched data
            existing_df = pd.read_csv(output_path_obj)
            print(f"Loaded existing enriched data with {len(existing_df)} rows from {output_path}")

            # Ensure phone columns exist
            if 'Phone1' not in existing_df.columns:
                existing_df['Phone1'] = ''
            if 'Phone2' not in existing_df.columns:
                existing_df['Phone2'] = ''
            if 'Phone3' not in existing_df.columns:
                existing_df['Phone3'] = ''
            if 'Phone4' not in existing_df.columns:
                existing_df['Phone4'] = ''

            return existing_df
        except Exception as e:
            print(f"Warning: Could not load existing enriched file ({e}). Creating new one.")
            # Fall through to create new dataframe

    # Create new dataframe with phone columns
    new_df = input_df.copy()
    new_df['Phone1'] = ''
    new_df['Phone2'] = ''
    new_df['Phone3'] = ''
    new_df['Phone4'] = ''
    print(f"Created new enriched dataframe with {len(new_df)} rows")
    return new_df

def update_enriched_df(existing_df: pd.DataFrame, input_df: pd.DataFrame,
                      start_idx: int, end_idx: int, total_processed: int) -> pd.DataFrame:
    """Update only the processed rows in existing enriched dataframe"""
    # Ensure we don't exceed the bounds of the existing dataframe
    max_idx = min(end_idx, len(existing_df), len(input_df))

    updated_count = 0
    for idx in range(start_idx, max_idx):
        if idx < len(input_df) and idx < len(existing_df):
            phone1_value = input_df.at[idx, 'Phone1'] if 'Phone1' in input_df.columns else ''
            phone2_value = input_df.at[idx, 'Phone2'] if 'Phone2' in input_df.columns else ''
            phone3_value = input_df.at[idx, 'Phone3'] if 'Phone3' in input_df.columns else ''
            phone4_value = input_df.at[idx, 'Phone4'] if 'Phone4' in input_df.columns else ''

            existing_df.at[idx, 'Phone1'] = phone1_value if phone1_value else pd.NA
            existing_df.at[idx, 'Phone2'] = phone2_value if phone2_value else pd.NA
            existing_df.at[idx, 'Phone3'] = phone3_value if phone3_value else pd.NA
            existing_df.at[idx, 'Phone4'] = phone4_value if phone4_value else pd.NA
            updated_count += 1

    print(f"Updated {updated_count} rows in enriched dataframe (processed range: {start_idx} to {max_idx-1})")
    return existing_df

def calculate_row_range(df: pd.DataFrame, args) -> Tuple[int, int]:
    """Calculate start and end indices based on user arguments (human-friendly)"""
    total_rows = len(df)

    # Handle backward compatibility with old arguments
    if not args.from_row and not args.to_row and not args.limit and not args.last:
        # Use old arguments if new ones aren't specified
        start_idx = args.start_row if args.start_row else 0
        if args.end_row:
            end_idx = min(args.end_row + 1, total_rows)  # Convert to exclusive end
        elif args.max_rows:
            end_idx = min(start_idx + args.max_rows, total_rows)
        else:
            end_idx = total_rows
        return start_idx, end_idx

    # Use new human-friendly arguments
    # Convert 1-based row numbers to 0-based indices
    if args.from_row:
        start_idx = max(0, args.from_row - 1)  # Convert to 0-based
    else:
        start_idx = 0

    if args.to_row:
        end_idx = min(args.to_row, total_rows)  # Keep 1-based for user, but cap at total_rows
    elif args.limit:
        end_idx = min(start_idx + args.limit, total_rows)
    elif args.last:
        start_idx = max(0, total_rows - args.last)
        end_idx = total_rows
    else:
        end_idx = total_rows

    return start_idx, end_idx

def validate_row_range(start_idx: int, end_idx: int, total_rows: int, args) -> bool:
    """Validate row range and provide user feedback"""
    if args.from_row and args.from_row > total_rows:
        print(f"Error: Starting row {args.from_row} exceeds file size ({total_rows})")
        return False

    if args.from_row and args.to_row and args.from_row > args.to_row:
        print(f"Error: Starting row {args.from_row} cannot be greater than ending row {args.to_row}")
        return False

    if args.last and args.last > total_rows:
        print(f"Error: Cannot process last {args.last} rows from file with only {total_rows} rows")
        return False

    if start_idx >= end_idx:
        print(f"Error: Invalid range - start index ({start_idx}) >= end index ({end_idx})")
        return False

    return True
