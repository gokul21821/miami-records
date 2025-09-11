# Mortgage Records Extraction Project Documentation

## Project Overview

This project involves building an automated backend workflow to extract mortgage records from the Miami-Dade Clerk's Official Records website for the **MORTGAGE – MOR** document type. The goal is to query public records day-by-day, process up to 500 records per query due to API limits, extract key borrower details, enrich with phone numbers via a people-lookup service (AnyWho), and generate monthly CSV files for lead generation. The process is one-time for January–June 2025 but parameterized for future reuse.

The workflow respects API constraints (e.g., 500-record cap per query string), focuses on individual borrowers (skipping companies), and conditionally parses the first two pages of document images if metadata alone is insufficient. A pilot phase will validate feasibility and decide on document parsing needs.

### Key Objectives

- **Maximize lead coverage** under API limits by querying per day
- **Enrich records** with up to two phone numbers without dropping ambiguous leads
- **Deliver CSVs** with columns: Name, Address, Phone1, Phone2, Rate of Interest, Loan Amount, Date of Document, plus provenance (Doc Type, CFN/Book/Page)
- **Ensure polite, compliant usage** to avoid rate limiting or blocks

### Assumptions

- Data is public and accessible via the provided endpoints; no authentication beyond cookies is needed
- Client accepts potential undercoverage on days exceeding 500 records
- Operational costs (e.g., any OCR/LLM) are borne by the project team

This documentation draws from standard mortgage document processing templates, focusing on automation phases like ingestion, extraction, enrichment, and output.

## Requirements

### Functional Requirements

- **Scope**: Initial focus on MORTGAGE – MOR; parameterized for date ranges (e.g., Jan 1–Jun 30, 2025) and future types (e.g., Liens, Deed with Mortgage)
- **Search Strategy**: One query per day per type to bypass the 500-record limit; log overflows for transparency
- **Lead Filtering**: Only individual borrowers (flag and skip LLCs/companies using name suffixes like "LLC", "Inc")

#### Extraction

- **From metadata**: Name (first/middle/last), Address (addressnounit/addressunit/address), Lender Name, Recording/Doc Date, Folio/Subdivision/Legal Description
- **From document** (pages 1–2, if needed): Loan Amount, Interest Rate, any missing address/date details

#### Enrichment

- Use AnyWho for Phone1/Phone2 based on name + Miami, FL
- Apply fuzzy address matching for disambiguation
- Include up to two candidates

#### Output

- Monthly CSVs (e.g., january.csv) with fixed schema
- Include a run report noting overflows and metrics

#### Pilot

- Test on 50–100 records from 2–3 January days to decide metadata-only vs. full parsing

### Non-Functional Requirements

- **Performance**: Handle long-running jobs (days/weeks); checkpoint per day for resumes
- **Reliability**: Idempotent by CFN/Book/Page; throttle requests with backoff to respect potential rate limits
- **Compliance**: Polite user-agent, respect robots.txt/TOS; no PII redaction needed as data is public
- **Quality**: ≥90% core field coverage in pilot; log ambiguities and overflows
- **Tools**: Python-based (e.g., Requests for APIs, OCRmyPDF/Tesseract for parsing if needed); no UI

## CSV Schema

| Column | Description | Source | Required? |
|--------|-------------|---------|-----------|
| Name | Borrower full name (first/middle/last) | Metadata/Document | Yes |
| Address | Property address (line/unit/full) | Metadata/Document | Yes |
| Phone1 | Primary enriched phone number | AnyWho | Optional |
| Phone2 | Secondary enriched phone number | AnyWho | Optional |
| Rate of Interest | Extracted interest rate (if present) | Document (if parsed) | Optional |
| Loan Amount | Extracted loan amount (if present) | Metadata/Document | Optional |
| Date of Document | Document or recording date | Metadata | Yes |
| Doc Type | e.g., "MORTGAGE - MOR" | Metadata | Yes |
| CFN_Master_ID | Unique record ID | Metadata | Yes |
| Rec_Book | Recording book number | Metadata | Yes |
| Rec_Page | Recording page number | Metadata | Yes |

## API Endpoints and Usage

All endpoints are under `https://onlineservices.miamidadeclerk.gov/officialrecords/api/`. Requests require specific headers (e.g., cookies like NSC_* and .PremierIDDade, Referer, Origin) to mimic browser behavior and avoid blocks.

### 1. Generate Query String (QS)

**Endpoint**: `POST /home/standardsearch`

**Parameters** (query string):
- `partyName`: "" (empty)
- `dateRangeFrom`: "YYYY-MM-DD" (e.g., "2025-01-01")
- `dateRangeTo`: "YYYY-MM-DD" (same as From for daily queries)
- `documentType`: "MORTGAGE - MOR"
- `searchT`: "MORTGAGE - MOR"
- `firstQuery`: "y"
- `searchtype`: "Name/Document"
- `token`: "" (often empty)

**Headers**: 
- `Accept: application/json`
- `Content-Length: 0`
- `Cookie`: (required values)
- `Origin/Referer`: site URLs
- `User-Agent`: browser-like

**Response**: Opaque QS string (e.g., encrypted token) unique to the filters

**Usage Note**: Generate one per day/type; cookies may need refreshing if expired

### 2. Fetch Records List

**Endpoint**: `GET /SearchResults/getStandardRecords?qs=<QS>`

**Parameters**: 
- `qs` (from previous step)

**Headers**: 
- `Accept: /`
- `Referer`: SearchResults page with QS
- `Cookie`: NSC_*
- `User-Agent`: browser-like

**Response**: JSON array of up to 500 records with fields like CFN_MASTER_ID, FIRSt_PARTY, SECond_PARTY, REC_BOOK, REC_PAGE, REC_DATE, DOC_DATE, ADDRESSNOUNIT, ADDRESS, FOLIO_NUMBER, CONSIDERATION_1 (possible loan amount proxy)

**Usage Note**: Cap at 500; log if count == 500 as potential overflow

### 3. Retrieve Document Image

**Endpoint**: `GET /DocumentImage/getdocumentimage?sBook=<book>&sPage=<page>&sBookType=O%20&redact=false`

**Parameters**: 
- `sBook` (from records)
- `sPage` (from records)
- `sBookType` ("O " or as in sample)
- `redact=false`

**Headers**: Standard browser headers; may include blob URLs for rendering

**Response**: PDF/image bytes of the full document (scanned, up to 30 pages)

**Usage Note**: Fetch only if pilot justifies; process pages 1–2 via OCR/LLM

### Enrichment Endpoint (AnyWho)

**Endpoint**: Use AnyWho's web search (e.g., via scraping or public API if available) for name + "Miami, FL"

**Usage Note**: Not a formal API; implement polite scraping with throttling; fallback to alternatives if needed

## Detailed Project Plan

This plan follows a phased approach inspired by mortgage document automation workflows, including ingestion, extraction, enrichment, and validation. It includes the pilot as Phase 1.

### Phase 1: Pilot (Days 0–4)

**Objective**: Validate end-to-end on 50–100 records; decide metadata-only vs. parsing

**Steps**:
1. Select 2–3 high-volume January days for MORTGAGE – MOR
2. For each day: POST to generate QS, GET records, persist JSON
3. Normalize and extract metadata fields; generate pilot CSV subset
4. Enrich with AnyWho: Query for phones, apply fuzzy matching, populate Phone1/Phone2
5. For 30–50 records: Fetch document images, parse pages 1–2 (OCRmyPDF + LLM), compare vs. metadata

**Deliverables**: Pilot CSV, metrics report (e.g., field coverage, enrichment rate), go/no-go on parsing

**Success Criteria**: ≥90% core fields from metadata; ≥50% phone hit-rate; ≥20% gain from parsing

### Phase 2: Setup and Orchestration (Days 5–6)

- Implement parameterization (dates, type) and daily loop: QS generation → records fetch → logging overflows
- Add reliability: Checkpoints per day, idempotency by CFN, throttling (e.g., 1–2 sec delay per request)

### Phase 3: Extraction and Enrichment (Days 7–10)

- Filter individuals; extract/normalize names, addresses, dates from metadata
- If parsing approved: Fetch images, OCR pages 1–2, LLM-extract loan/interest details
- Enrich: Query AnyWho, rank candidates, fill Phone1/Phone2

### Phase 4: Quality and Output (Days 11–14)

- Dedupe, validate fields, reconcile metadata vs. parsed data
- Generate monthly CSVs and run reports (rows, overflows, metrics)

### Phase 5: Full Run and Delivery (Days 15+)

- Execute for Jan–Jun; monitor and resume as needed
- Deliver CSVs, reports, and code (if requested)

## Risks and Mitigations

- **API Limits/Overflow**: Log and report; future: explore sub-filters if available
- **Session Expiry**: Auto-refresh cookies; fallback to manual capture
- **Enrichment Gaps**: Monitor hit-rate; add secondary lookup if <50% in pilot
- **Parsing Costs**: Conditional on pilot; use free OCR baseline
- **Compliance**: Throttle to 1 query/min; user-agent as browser

## Timeline and Resources

- **Total Duration**: 2–3 weeks (pilot: 4 days; full run: scalable by chunking months)
- **Resources**: Python dev environment; optional cloud for long runs (e.g., Azure VM)
- **Milestones**: Pilot complete by Day 4; first monthly CSV by Day 10; full delivery by end of window


records fetch:
export COOKIES="NSC_JOeqtbnye4rqvqae52yysbdjdcwntcw=7ce2a3d93287e39e0a3142520a74f0b88d9f176cdcf72de67d2df59bf583b8a94149188e; .PremierIDDade=hStXCTj14zaDgObXFky4Bw%3D%3D"
python src/miami_mor_step2.py --start-date 2025-01-01 --end-date 2025-01-31 --mode auto

records csv:
python -m src.miami_mor_step3   --start-date 2025-01-02  --end-date 2025-01-31 --force

docs:
python -m src.miami_mor_step4 --start-date 2025-01-01 --end-date 2025-01-03 --per-day 5


remove stuff:
python src/remove_duplicates.py "data/gold/monthly/MORTGAGE_MOR/2025-01_enriched.csv"