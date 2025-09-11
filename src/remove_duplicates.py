#!/usr/bin/env python3
"""
CSV Duplicate Remover

This script removes duplicate rows from a CSV file based on the Name column values.
It keeps the first occurrence of each unique name and removes subsequent duplicates.
Additionally, it removes rows where names contain business-related terms.

Usage:
    python remove_duplicates.py input.csv [output.csv]

If no output file is specified, it will create a backup and overwrite the input file.
"""

import pandas as pd
import sys
import os
import re
from pathlib import Path


def contains_business_terms(name):
    """
    Check if a name contains business-related terms that should be filtered out.
    
    Args:
        name (str): The name to check
        
    Returns:
        bool: True if name contains business terms, False otherwise
    """
    if pd.isna(name) or not isinstance(name, str):
        return False
    
    # Business terms to filter out (case-insensitive)
    business_terms = [
        'LLC', 'INC', 'CORP', 'LLP', 'BANK', 'N.A.', 'TRUST', 'FUND', 
        'CAPITAL', 'MORTGAGE', 'COMPANY', 'ASSOCIATION', 'PRODUCTS',
        'EVERGREEN', 'LP', 'REGISTRATION', 'UNION', 'NBA', 'PROPERTIES', 'LOANS','URBAN','FINANCIAL','HOME',
        'FINANCE','INVESTMENTS','DEVELOPMENT','DEBT','CAPITAL','FINANCE','INVESTMENTS','DEVELOPMENT','DEBT','CAPITAL',
        'CORPORATION', 'MORTGAGE'
    ]
    
    # Convert to uppercase for case-insensitive matching
    name_upper = name.upper()
    
    # Check if any business term is contained in the name
    for term in business_terms:
        if term in name_upper:
            return True
    
    return False


def remove_duplicates_and_business_names(input_file, output_file=None):
    """
    Remove duplicate rows and business names from CSV based on Name column.
    
    Args:
        input_file (str): Path to input CSV file
        output_file (str, optional): Path to output CSV file. If None, overwrites input.
    
    Returns:
        tuple: (original_count, deduplicated_count, removed_count, business_removed_count)
    """
    try:
        # Read the CSV file
        print(f"Reading CSV file: {input_file}")
        df = pd.read_csv(input_file)
        
        # Get original count
        original_count = len(df)
        print(f"Original row count: {original_count}")
        
        # Check if Name column exists
        if 'Name' not in df.columns:
            print("Error: 'Name' column not found in CSV file.")
            print(f"Available columns: {list(df.columns)}")
            return None, None, None, None
        
        # Remove business names first
        print("Filtering out business names...")
        df_filtered = df[~df['Name'].apply(contains_business_terms)]
        business_removed_count = original_count - len(df_filtered)
        print(f"Business names removed: {business_removed_count}")
        
        # Remove duplicates based on Name column, keeping first occurrence
        print("Removing duplicates based on Name column...")
        df_deduplicated = df_filtered.drop_duplicates(subset=['Name'], keep='first')
        
        # Get deduplicated count
        deduplicated_count = len(df_deduplicated)
        duplicate_removed_count = len(df_filtered) - deduplicated_count
        total_removed_count = original_count - deduplicated_count
        
        print(f"Duplicate rows removed: {duplicate_removed_count}")
        print(f"Rows after all processing: {deduplicated_count}")
        print(f"Total rows removed: {total_removed_count}")
        
        # Determine output file
        if output_file is None:
            # Create backup of original file
            backup_file = f"{input_file}.backup"
            print(f"Creating backup: {backup_file}")
            df.to_csv(backup_file, index=False)
            
            # Overwrite original file
            output_file = input_file
            print(f"Overwriting original file: {output_file}")
        else:
            print(f"Writing to output file: {output_file}")
        
        # Write deduplicated data
        df_deduplicated.to_csv(output_file, index=False)
        
        return original_count, deduplicated_count, total_removed_count, business_removed_count
        
    except FileNotFoundError:
        print(f"Error: File '{input_file}' not found.")
        return None, None, None, None
    except Exception as e:
        print(f"Error processing file: {str(e)}")
        return None, None, None, None


def main():
    """Main function to handle command line arguments and execute deduplication."""
    
    # Check command line arguments
    if len(sys.argv) < 2:
        print("Usage: python remove_duplicates.py input.csv [output.csv]")
        print("\nExamples:")
        print("  python remove_duplicates.py data.csv                    # Overwrite original")
        print("  python remove_duplicates.py data.csv clean_data.csv     # Save to new file")
        sys.exit(1)
    
    input_file = sys.argv[1]
    output_file = sys.argv[2] if len(sys.argv) > 2 else None
    
    # Validate input file exists
    if not os.path.exists(input_file):
        print(f"Error: Input file '{input_file}' does not exist.")
        sys.exit(1)
    
    # Validate input file is CSV
    if not input_file.lower().endswith('.csv'):
        print(f"Warning: Input file '{input_file}' doesn't have .csv extension.")
    
    # Execute deduplication and business name filtering
    print("=" * 50)
    print("CSV Duplicate Remover & Business Name Filter")
    print("=" * 50)
    
    result = remove_duplicates_and_business_names(input_file, output_file)
    
    if result[0] is not None:
        original_count, deduplicated_count, total_removed_count, business_removed_count = result
        print("\n" + "=" * 50)
        print("SUMMARY")
        print("=" * 50)
        print(f"Original rows: {original_count}")
        print(f"Business names removed: {business_removed_count}")
        print(f"After business filtering: {original_count - business_removed_count}")
        print(f"After deduplication: {deduplicated_count}")
        print(f"Total rows removed: {total_removed_count}")
        print(f"Overall reduction: {total_removed_count/original_count*100:.1f}%")
        
        if output_file:
            print(f"\nCleaned data saved to: {output_file}")
        else:
            print(f"\nOriginal file backed up and overwritten: {input_file}")
    else:
        print("\nProcessing failed. Please check the error messages above.")
        sys.exit(1)


if __name__ == "__main__":
    main()
