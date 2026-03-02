"""Configuration constants for the TAAFT scraper."""

# Entry points for Phase 1 harvesting
LISTING_URLS = [
    "https://free.theresanaiforthat.com/",
    # "https://free.theresanaiforthat.com/period/february/",
    # "https://free.theresanaiforthat.com/period/january/",
    # "https://free.theresanaiforthat.com/period/december/",
    # "https://free.theresanaiforthat.com/period/november/",
    # "https://free.theresanaiforthat.com/period/october/",
    # "https://theresanaiforthat.com/agents/",
]

BASE_URL = "https://theresanaiforthat.com"
FREE_BASE_URL = "https://free.theresanaiforthat.com"

# HTTP session config
IMPERSONATE_BROWSER = "chrome120"
REQUEST_TIMEOUT = 15
DEFAULT_HEADERS = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36",
    "Accept-Language": "en-GB,en;q=0.9",
    "Referer": "https://theresanaiforthat.com/",
}

# Rate limiting
BASE_DELAY = (2.0, 5.0)       # random uniform seconds between requests
MAX_PER_SESSION = 100          # stop after this many requests per run
BACKOFF_ON_429 = 60            # seconds to wait on 429
MAX_RETRIES = 3

# Database
DB_PATH = "taaft_scraper.db"

# Logging
LOG_FILE = "scraper.log"

# Agent detection keywords (must match 2+ for description-based inference)
AGENT_KEYWORDS = [
    "autonomous",
    "agent",
    "automated",
    "multi-step",
    "executes",
    "plans and",
    "tool use",
    "browser",
    "executes tasks",
]

# Task categories that indicate an agent
AGENT_TASK_CATEGORIES = {
    "agents",
    "autogpt",
    "automation",
    "task automation",
    "ai agents",
    "workflows",
    "autonomous",
}

# Pricing labels from listing cards that indicate genuinely free tools
FREE_PRICING_LABELS = {"100% free", "100 % free"}

# Pricing labels to discard
DISCARD_PRICING_PATTERNS = [
    "free +",
    "free+",
    "freemium",
    "free trial",
    "paid",
    "from $",
]
