# Miami Real Estate Data Enrichment Tool

## Setup Instructions

### Prerequisites
- Python 3.8 or higher
- Git (to clone the repository)

### 1. Clone the Repository
```bash
git clone <your-repository-url>
cd miami-real-estate
```

### 2. Create Virtual Environment

#### Windows:
```cmd
python -m venv venv
venv\Scripts\activate
```

#### Mac/Linux:
```bash
python3 -m venv venv
source venv/bin/activate
```

### 3. Install Dependencies
```bash
pip install -r requirements.txt
```

### 4. Setup Environment Variables
```bash
# Copy the example environment file
cp .env.example .env

# Edit .env file with your actual cookie values (see below)
```

## Getting Cookies (Required for Data Fetching)

### For Miami-Dade Clerk Records:
1. Open Chrome/Edge browser
2. Go to: https://onlineservices.miamidadeclerk.gov/officialrecords/
3. Open Developer Tools (F12)
4. Go to Network tab
5. Perform a search on the website
6. Find any request in Network tab
7. Right-click → Copy → Copy as cURL
8. Extract the cookie value from the cURL command
9. Paste it in your `.env` file:

```bash
COOKIES=NSC_JOeqtbnye4rqvqae52yysbdjdcwntcw=your_actual_cookie_here
```

## Running the GUI

### Windows:
```cmd
python -m src.gui.app
```

### Mac:
```bash
python3 -m src.gui.app
```

## GUI Features Guide

### 1. Fetch Records Tab
**Purpose:** Download mortgage records from Miami-Dade Clerk website

**Steps:**
1. Enter **Start Date** (YYYY-MM-DD format, e.g., 2025-01-01)
2. Enter **End Date** (YYYY-MM-DD format, e.g., 2025-01-31)
3. Paste your **Cookies** (from browser DevTools)
4. Check **"Force reprocess"** if you want to re-download existing dates
5. Click **"Fetch Records"**

**What happens:**
- Downloads mortgage records for each date in the range
- Saves data in `data/bronze/YYYY-MM-DD/MORTGAGE_MOR/`
- Creates `records.json` and `summary.json` files
- Shows progress in the log window

### 2. Create CSV Tab
**Purpose:** Convert raw records into normalized CSV files

**Steps:**
1. Enter **Start Date** (YYYY-MM-DD format)
2. Enter **End Date** (YYYY-MM-DD format)
3. Check **"Force rebuild"** if you want to recreate existing CSVs
4. Click **"Create CSV (normalize + dedupe)"**

**What happens:**
- Processes records from bronze to silver layer
- Creates normalized CSV files in `data/silver/monthly/MORTGAGE_MOR/`
- Removes duplicate names and business entries
- Groups data by month

### 3. Enrich Phones Tab
**Purpose:** Add phone numbers to borrower records using AnyWho

**Steps:**
1. Click **"Refresh Months"** to load available months
2. Select a **Month** from the dropdown
3. Set **From row** (1-based, e.g., 1 for first row)
4. Set **To row** (1-based, e.g., 100 for first 100 rows)
5. Set **Sleep sec** (delay between requests, recommended: 1.0)
6. Check **"Refresh cache"** if you want to ignore cached results
7. Click **"Enrich Phones"**

**What happens:**
- Searches AnyWho for phone numbers based on borrower names and addresses
- Adds Phone1, Phone2, Phone3, Phone4 columns to the data
- Saves enriched data in `data/gold/monthly/MORTGAGE_MOR/`
- Uses caching to avoid duplicate lookups

## Data Flow

```
Raw Records → Normalized CSV → Enriched CSV
    ↓             ↓              ↓
  Bronze       Silver          Gold
(Miami-Dade)  (Clean CSV)    (With Phones)
```

## File Locations

- **Bronze Layer:** `data/bronze/YYYY-MM-DD/MORTGAGE_MOR/`
- **Silver Layer:** `data/silver/monthly/MORTGAGE_MOR/YYYY-MM_normalized.csv`
- **Gold Layer:** `data/gold/monthly/MORTGAGE_MOR/YYYY-MM_enriched.csv`
- **Cache:** `data/cache/anywho_cache_enhanced.json`
- **State:** `data/state/gui_state.json`

## Troubleshooting

### GUI Won't Start
- Make sure you're in the virtual environment
- Check Python version: `python --version`
- Verify all dependencies are installed: `pip list`

### Fetch Records Fails
- Verify cookies are correct and not expired
- Check internet connection
- Try with a smaller date range first

### Phone Enrichment Issues
- Check internet connection
- Some addresses may not have phone listings
- Try different sleep intervals if getting blocked

### Permission Errors (Windows)
- Run command prompt as Administrator
- Or use `python` instead of `python3`

## Advanced Usage

### Command Line Usage
You can also run individual components from command line:

```bash
# Fetch records for specific date
python -m src.miami_mor_step1 --date 2025-01-01

# Process date range
python -m src.miami_mor_step2 --start-date 2025-01-01 --end-date 2025-01-31

# Create CSV from records
python -m src.miami_mor_step3 --start-date 2025-01-01 --end-date 2025-01-31

# Enrich phone numbers
python run.py input.csv output.csv --from-row 1 --to-row 100
```

### Manual Cookie Setup
If the GUI cookie field is empty, you can set it via environment variable:
```bash
# Windows
set COOKIES=your_cookie_here
python -m src.gui.app

# Mac/Linux
export COOKIES=your_cookie_here
python3 -m src.gui.app
```
