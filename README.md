# Miami Real Estate Data Enrichment

This project provides tools for enriching real estate data with phone numbers using AnyWho lookups.

## Project Structure

The codebase has been refactored into a modular structure:

```
src/
├── algorithms/       # Scoring and matching algorithms
├── cli/              # Command-line interface
├── config/           # Configuration and constants
├── parsers/          # HTML parsing functionality
├── processors/       # Data processing utilities
├── scrapers/         # Web scraping functionality
├── utils/            # Utility functions
└── main.py           # Main entry point
```

## Usage

Run the script using:

```bash
python run.py input_file.csv output_file.csv
```

### Command Line Arguments

- `--test`: Run test with sample data
- `--cache-path`: Cache file path (default: "data/cache/anywho_cache_enhanced.json")
- `--sleep-sec`: Delay between requests (default: 1.0)
- `--refresh`: Ignore cache and refresh lookups
- `--from-row`: Starting row number (1-based, human-friendly)
- `--to-row`: Ending row number (1-based, human-friendly)
- `--limit`: Maximum number of rows to process from start
- `--last`: Process last N rows of the file

## Example

```bash
# Process rows 10-20 in the file
python run.py --from-row 10 --to-row 20 --sleep-sec 3 --refresh data/silver/monthly/MORTGAGE_MOR/2025-02_normalized.csv data/gold/monthly/MORTGAGE_MOR/2025-02_enriched.csv

# Process the last 100 rows
python run.py --last 100 data/silver/monthly/MORTGAGE_MOR/2025-02_normalized.csv data/gold/monthly/MORTGAGE_MOR/2025-02_enriched.csv

# Run a test with sample data
python run.py --test
```

## GUI (Tkinter)

You can run the full workflow via a simple desktop GUI.

```bash
python -m src.gui.app
```

Features:

- Fetch Records: set start/end dates and run Step 2 (QS + records) with cookies.
- Create CSV: run Step 3 normalization for the range, then dedupe to `*_normalized_clean.csv`.
- Enrich Phones: pick a month and row range; updates/creates `data/gold/monthly/MORTGAGE_MOR/<YYYY-MM>_enriched.csv`.

Notes:

- Cookies are prefilled per `project.md`; you can edit them in the GUI.
- Enrichment input prefers the cleaned normalized CSV; falls back to the normalized CSV.
- Logs stream in the bottom panel; Stop button terminates the active task.