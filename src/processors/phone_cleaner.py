import argparse
import re
from pathlib import Path
from typing import Iterable, List

import pandas as pd


DEFAULT_PHONE_COLUMNS: List[str] = ["Phone1", "Phone2", "Phone3", "Phone4"]
DROP_COLUMNS: List[str] = [
    "Address",
    "Rate of Interest",
    "Loan Amount",
    "Date of Document",
    "Date",
    "Doc Type",
    "CFN_Master_ID",
    "Rec_Book",
    "Rec_Page",
    "Rec_BookPage",
    "Book_Type",
    "Phone3",
    "Phone4",
]

# Tokens that should not be considered a first name when scanning from the end
CONNECTOR_TOKENS = {
    "de",
    "del",
    "la",
    "las",
    "los",
    "da",
    "dos",
    "y",
    "van",
    "von",
    "di",
    "le",
}

SUFFIX_TOKENS = {"jr", "jr.", "sr", "sr.", "ii", "iii", "iv", "v", "vi"}


def normalize_phone(value: object) -> str:
    """Normalize a phone number string.

    - Remove all non-digit characters
    - Ensure it starts with a leading '1'
    - Return empty string for empty/None-like values
    """
    if value is None:
        return ""

    text = str(value).strip()
    if text == "" or text.lower() in {"nan", "none", "null"}:
        return ""

    digits = re.sub(r"\D+", "", text)
    if digits == "":
        return ""

    # If already starts with country code '1', keep as is; otherwise prefix '1'
    return digits if digits.startswith("1") else f"1{digits}"


def extract_first_name(value: object) -> str:
    """Extract first name as the last significant token from a full name.

    Rules:
    - Scan tokens from right to left
    - Skip one-letter initials and common suffixes (JR, SR, II, ...)
    - Skip connector tokens (de, del, la, los, y, ...)
    - Return Title Case of the selected token
    """
    if value is None:
        return ""

    text = str(value).strip()
    if text == "" or text.lower() in {"nan", "none", "null"}:
        return ""

    tokens = text.split()
    for raw in reversed(tokens):
        token = raw.strip(".,;:()[]{}'\"")
        lower = token.lower()
        if lower == "":
            continue
        if len(lower) == 1:
            continue
        if lower in SUFFIX_TOKENS:
            continue
        if lower in CONNECTOR_TOKENS:
            continue
        return token.title()

    return ""


def clean_phone_columns(input_csv: Path, output_csv: Path, columns: Iterable[str]) -> None:
    """Read a CSV, clean phone columns, reduce Name to first name, drop extra columns, write to output CSV."""
    df = pd.read_csv(input_csv, dtype=str)
    # Preserve original row order to keep split phone rows adjacent per person
    df["_row_order"] = range(len(df))

    present_columns = [c for c in columns if c in df.columns]
    missing_columns = [c for c in columns if c not in df.columns]

    if not present_columns:
        raise ValueError(
            f"None of the specified phone columns are present in the file. Missing: {missing_columns}"
        )

    for col in present_columns:
        df[col] = df[col].map(normalize_phone)

    if missing_columns:
        print(f"[warn] Missing columns (skipped): {missing_columns}")

    # Transform Name -> first name (Title Case)
    if "Name" in df.columns:
        df["Name"] = df["Name"].map(extract_first_name)
    else:
        print("[warn] Missing column 'Name' (skipped first-name extraction)")

    # Drop non-required columns if present (keeps Phone1/Phone2 for reshaping)
    to_drop_present = [c for c in DROP_COLUMNS if c in df.columns]
    if to_drop_present:
        df.drop(columns=to_drop_present, inplace=True)
        print(f"[info] Dropped columns: {to_drop_present}")

    # Reshape Phone1/Phone2 into single 'Phone' column, duplicating rows per person
    phone_value_columns = [c for c in ["Phone1", "Phone2"] if c in df.columns]
    if not phone_value_columns:
        # If neither Phone1 nor Phone2 exist after drops, write empty with schema
        df_out = pd.DataFrame(columns=["Name", "Phone"]) if "Name" in df.columns else pd.DataFrame(columns=["Phone"])
    else:
        id_vars = ["Name", "_row_order"] if "Name" in df.columns else ["_row_order"]
        long_df = pd.melt(
            df,
            id_vars=id_vars,
            value_vars=phone_value_columns,
            var_name="_phone_source",
            value_name="Phone",
        )

        # Remove empty phones
        long_df["Phone"] = long_df["Phone"].fillna("").astype(str)
        long_df = long_df[long_df["Phone"] != ""]

        # Sort to keep per-person phone entries adjacent in original input order
        long_df.sort_values(by=["_row_order", "_phone_source"], inplace=True, kind="stable")

        # Final output order
        if "Name" in long_df.columns:
            df_out = long_df[["Name", "Phone"]]
        else:
            df_out = long_df[["Phone"]]

    # Write output
    output_csv.parent.mkdir(parents=True, exist_ok=True)
    df_out.to_csv(output_csv, index=False)
    print(f"[ok] Wrote cleaned CSV to: {output_csv}")


def derive_output_path(input_csv: Path, output: Path | None, inplace: bool) -> Path:
    if inplace:
        return input_csv
    if output is not None:
        return output
    return input_csv.with_name(f"{input_csv.stem}_phones_cleaned{input_csv.suffix}")


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description=(
            "Normalize phone columns (remove formatting, prefix '1'), extract first names, and drop non-essential columns."
        )
    )

    parser.add_argument(
        "--input",
        "-i",
        type=Path,
        required=True,
        help="Path to input CSV file",
    )
    parser.add_argument(
        "--output",
        "-o",
        type=Path,
        required=False,
        help=(
            "Optional output CSV path. If omitted, writes '<stem>_phones_cleaned<ext>'."
        ),
    )
    parser.add_argument(
        "--inplace",
        action="store_true",
        help="Overwrite the input file in place.",
    )
    parser.add_argument(
        "--columns",
        "-c",
        nargs="*",
        default=DEFAULT_PHONE_COLUMNS,
        help=(
            "Phone columns to clean (default: Phone1 Phone2 Phone3 Phone4). "
            "Only existing columns will be processed."
        ),
    )

    args = parser.parse_args()

    if args.inplace and args.output is not None:
        parser.error("--inplace cannot be used together with --output")

    return args


def main() -> None:
    args = parse_args()
    input_csv: Path = args.input
    output_csv: Path = derive_output_path(input_csv, args.output, args.inplace)

    print(
        f"[info] Cleaning phones in: {input_csv} -> {output_csv} | columns={args.columns}"
    )
    clean_phone_columns(input_csv=input_csv, output_csv=output_csv, columns=args.columns)


if __name__ == "__main__":
    main()


