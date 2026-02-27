# TAAFT Scraper

Scrapes [theresanaiforthat.com](https://theresanaiforthat.com/) to collect **genuinely free** AI tools and agents for a task-based search engine index.

## Setup

```bash
# Python 3.11+ required
pip install curl_cffi beautifulsoup4 lxml
```

## Usage

All commands are run from the `taaft_scraper/` directory:

```bash
cd taaft_scraper

# Phase 1 — crawl listing pages, populate job queue with free tools
python scraper.py harvest

# Phase 2 — fetch individual tool pages for pending jobs
python scraper.py fetch

# Run both phases end to end
python scraper.py run

# Check queue and agent stats
python scraper.py status

# Export collected data
python scraper.py export -o agents.json
python scraper.py export -o agents.csv --format csv
python scraper.py export -o agents_only.json --agents-only

# Re-queue failed jobs for retry
python scraper.py reset-failed

# Re-fetch all done tools (full refresh)
python scraper.py fetch --refetch

# Limit Phase 2 to N tools per session
python scraper.py fetch --limit 50
```

## Architecture

### Two-phase pipeline

**Phase 1 (Harvest)** — Crawls listing and period pages, extracts tool slugs and pricing labels, filters to only "100% Free" tools, and populates the SQLite job queue.

**Phase 2 (Fetch)** — Processes pending jobs by fetching individual `/ai/[slug]/` pages, verifying pricing, extracting structured fields, running agent detection, and storing results.

### Session management

The scraper is resumable. Each run processes up to `MAX_PER_SESSION` (150) jobs and stops cleanly. Re-run to continue from where it left off.

### Rate limiting

- Random 2-5s delay between requests
- 60s backoff on HTTP 429
- Up to 3 retries per request
- Chrome TLS fingerprint via curl_cffi

## Data

- **SQLite DB**: `taaft_scraper.db` (auto-created)
- **Log file**: `scraper.log`
- **Export**: JSON or CSV via the `export` command

## File structure

```
taaft_scraper/
├── scraper.py      # CLI entry point
├── harvester.py    # Phase 1: listing page crawling
├── fetcher.py      # Phase 2: tool page fetching + parsing
├── db.py           # SQLite setup and helpers
├── filters.py      # Pricing filter + agent detection
├── config.py       # Constants and settings
├── export.py       # JSON/CSV export
└── README.md       # This file
```
