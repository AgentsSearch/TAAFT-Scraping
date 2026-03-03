"""Phase 2: Fetch individual tool pages, parse structured data, and store results."""

import json
import logging
import re
import time
import random
from datetime import datetime, timezone

from bs4 import BeautifulSoup
from bs4.element import NavigableString
from curl_cffi import requests as cffi_requests

from config import (
    BASE_URL,
    IMPERSONATE_BROWSER,
    REQUEST_TIMEOUT,
    DEFAULT_HEADERS,
    BASE_DELAY,
    BACKOFF_ON_429,
    MAX_RETRIES,
    MAX_PER_SESSION,
)
from db import get_pending_jobs, mark_job, upsert_agent
from filters import is_free_from_detail_page, classify_agent

logger = logging.getLogger("taaft.fetcher")


def _make_session() -> cffi_requests.Session:
    """Create a curl_cffi session with Chrome impersonation."""
    session = cffi_requests.Session(impersonate=IMPERSONATE_BROWSER)
    session.headers.update(DEFAULT_HEADERS)
    return session


def _fetch_page(session: cffi_requests.Session, url: str) -> tuple[str | None, int]:
    """Fetch a URL with retries. Returns (html, status_code)."""
    last_status = 0
    for attempt in range(1, MAX_RETRIES + 1):
        try:
            timeout = REQUEST_TIMEOUT * (2 if attempt > 1 else 1)
            resp = session.get(url, timeout=timeout)
            last_status = resp.status_code

            if resp.status_code == 429:
                logger.warning(f"429 rate limit on {url}, sleeping {BACKOFF_ON_429}s")
                time.sleep(BACKOFF_ON_429)
                continue

            if resp.status_code == 404:
                return None, 404

            if resp.status_code >= 400:
                logger.warning(f"HTTP {resp.status_code} on {url} (attempt {attempt})")
                time.sleep(BASE_DELAY[1])
                continue

            return resp.text, resp.status_code

        except Exception as e:
            logger.warning(f"Request error on {url} (attempt {attempt}): {e}")
            if attempt < MAX_RETRIES:
                time.sleep(BASE_DELAY[1] * attempt)

    return None, last_status


def _delay():
    """Random delay between requests."""
    time.sleep(random.uniform(*BASE_DELAY))


def parse_tool_page(html: str, slug: str) -> dict:
    """Parse an individual tool page and extract all structured fields.

    Returns a dict with all fields for the agents table.
    """
    soup = BeautifulSoup(html, "lxml")
    data = {"slug": slug, "taaft_url": f"{BASE_URL}/ai/{slug}/"}

    # Name from <h1>
    h1 = soup.find("h1")
    data["name"] = h1.get_text(strip=True) if h1 else slug

    # Description from overview section
    data["description"] = _extract_description(soup)

    # External URL (the actual tool's website)
    data["external_url"] = _extract_external_url(soup)

    # Pricing model from structured pricing section
    data["pricing_model"] = _extract_pricing_model(soup)

    # Agent badge detection
    has_agent_link = _detect_agent_link(soup)

    # Task categories
    task_categories = _extract_task_categories(soup)
    data["task_categories"] = task_categories

    # Agent classification
    agent_info = classify_agent(has_agent_link, task_categories, data["description"] or "")
    data.update(agent_info)

    # Q&A content
    # data["qa_content"] = _extract_qa_content(soup)

    # Pros and cons
    data["pros"] = _extract_pros(soup)
    data["cons"] = _extract_cons(soup)

    # Traffic count (opens)
    data["saves_count"] = _extract_traffic_count(soup)

    # Leaderboard score
    data["leaderboard_score"] = _extract_leaderboard_score(soup)

    # Rating
    rating, rating_count = _extract_rating(soup)
    data["rating"] = rating
    data["rating_count"] = rating_count

    # Last updated (latest release date)
    data["last_updated"] = _extract_last_updated(soup)

    # Timestamp
    data["scraped_at"] = datetime.now(timezone.utc).isoformat()

    return data


def _extract_description(soup: BeautifulSoup) -> str | None:
    """Extract the main description text from div.ai_description.

    Collects only the bare text nodes and <br>-separated paragraphs,
    skipping child elements like h2, social links, and buttons.
    """
    desc_div = soup.find("div", class_="ai_description")
    if desc_div:
        # Remove child elements we don't want (headings, socials, buttons)
        skip_tags = {"h2", "h3", "div", "a", "img", "button", "span"}
        parts: list[str] = []
        for node in desc_div.children:
            if isinstance(node, str):
                text = node.strip()
                if text:
                    parts.append(text)
            elif node.name == "br":
                continue  # line breaks become natural spacing
            elif node.name not in skip_tags:
                text = node.get_text(strip=True)
                if text:
                    parts.append(text)
        text = " ".join(parts).strip()
        # Collapse multiple spaces
        text = re.sub(r"\s{2,}", " ", text)
        if len(text) > 20:
            return text

    # Fallback: meta description
    meta = soup.find("meta", attrs={"name": "description"})
    if meta and meta.get("content"):
        return meta["content"].strip()

    return None


def _extract_external_url(soup: BeautifulSoup) -> str | None:
    """Extract the tool's actual website URL from the 'use tool' button."""
    # Primary: find the "use tool" button (class visit_website_btn or parent div.visit_website)
    for a in soup.find_all("a", href=True):
        text = a.get_text(strip=True).lower()
        classes = a.get("class", [])
        if text == "use tool" and "visit_website_btn" in classes:
            return a["href"]

    # Fallback: any link with "use tool" text
    for a in soup.find_all("a", href=True):
        if a.get_text(strip=True).lower() == "use tool":
            href = a["href"]
            if href.startswith("http") and "theresanaiforthat.com" not in href:
                return href

    return None


def _extract_pricing_model(soup: BeautifulSoup) -> str | None:
    """Extract pricing model from the structured pricing section."""
    # Look for pricing section by id
    pricing_section = soup.find(id="pricing-options")
    if not pricing_section:
        # Try broader search
        for tag in soup.find_all(["div", "section"], class_=True):
            classes = " ".join(tag.get("class", []))
            if "pricing" in classes.lower():
                pricing_section = tag
                break

    if pricing_section:
        text = pricing_section.get_text(" ", strip=True)
        # Look for "Pricing model: X"
        match = re.search(r"pricing\s*model[:\s]+(\w+)", text, re.IGNORECASE)
        if match:
            return match.group(1).strip()

    # Broader search in full page text for pricing model
    for tag in soup.find_all(string=re.compile(r"pricing\s*model", re.IGNORECASE)):
        parent = tag.parent
        if parent:
            text = parent.get_text(" ", strip=True)
            match = re.search(r"pricing\s*model[:\s]+(\w+)", text, re.IGNORECASE)
            if match:
                return match.group(1).strip()

    return None


def _detect_agent_link(soup: BeautifulSoup) -> bool:
    """Check if the page has an [Agent] badge (link to /agents/)."""
    for a in soup.find_all("a", href=True):
        if "/agents/" in a["href"]:
            # Make sure it looks like a badge/tag, not just a nav link
            text = a.get_text(strip=True).lower()
            if "agent" in text and len(text) < 30:
                return True
    return False


_EMOJI_RE = re.compile(
    "["
    "\U0001F600-\U0001F64F"  # emoticons
    "\U0001F300-\U0001F5FF"  # symbols & pictographs
    "\U0001F680-\U0001F6FF"  # transport & map
    "\U0001F1E0-\U0001F1FF"  # flags
    "\U00002700-\U000027BF"  # dingbats
    "\U0001F900-\U0001F9FF"  # supplemental symbols
    "\U0001FA00-\U0001FA6F"  # chess symbols
    "\U0001FA70-\U0001FAFF"  # symbols extended-A
    "\U00002702-\U000027B0"  # dingbats
    "\U0000FE00-\U0000FE0F"  # variation selectors
    "\U0000200D"             # zero width joiner
    "\U000020E3"             # combining enclosing keycap
    "\U00002600-\U000026FF"  # misc symbols
    "]+",
    flags=re.UNICODE,
)


def _extract_task_categories(soup: BeautifulSoup) -> list[str]:
    """Extract task categories from the breadcrumb path at the top of the page.

    Looks for <span class="breadcrumb breadcrumb-for-task"> elements inside
    the breadcrumbs_wrap container.  These represent the hierarchical category
    path shown on each tool page (e.g. Work > Business > Industries > AI >
    Models > Large Language Models).

    Falls back to scanning /task/ links in the breadcrumbs container if the
    span-based approach yields nothing.
    """
    categories: list[str] = []
    seen: set[str] = set()

    # Locate the breadcrumbs container
    bc_wrap = soup.find("div", class_="breadcrumbs_wrap") or soup.find("div", class_="breadcrumbs")
    if not bc_wrap:
        # Fallback: try by id
        bc_wrap = soup.find(id="breadcrumbs_wrap") or soup.find(id="breadcrumbs")

    if bc_wrap:
        # Strategy 1: breadcrumb spans (preferred)
        for span in bc_wrap.find_all("span", class_=lambda c: c and "breadcrumb-for-task" in c):
            # Collect text only from direct children, skipping emoji and counter spans
            parts = []
            for child in span.find_all(recursive=False):
                cls = " ".join(child.get("class", []))
                if "emoji_icon" in cls or "breadcrumb_counter" in cls:
                    continue
                parts.append(child.get_text(strip=True))
            # Also grab any direct text nodes on the span itself
            for node in span.children:
                if isinstance(node, str) and node.strip():
                    parts.append(node.strip())
            text = " ".join(p for p in parts if p).strip()
            text = _EMOJI_RE.sub("", text).strip()
            # Strip all digits and commas
            text = re.sub(r"[\d,]+", "", text).strip()
            if text and text.lower() not in seen:
                seen.add(text.lower())
                categories.append(text)

        if categories:
            return categories

        # Strategy 2: /task/ links inside breadcrumbs container
        for a in bc_wrap.find_all("a", href=True):
            if "/task/" not in a["href"]:
                continue
            text = a.get_text(strip=True)
            if not text:
                continue
            if text.lower().startswith("go to"):
                text = text[5:].strip()
            text = _EMOJI_RE.sub("", text).strip()
            text = re.sub(r"\s*\([\d,]+\)\s*$", "", text).strip()
            if text and text.lower() not in seen:
                seen.add(text.lower())
                categories.append(text)

    return categories


def _extract_qa_content(soup: BeautifulSoup) -> list[dict]:
    """Extract Q&A pairs from the FAQ section.

    Returns a list of {"question": ..., "answer": ...} dicts.
    Each pair comes from a div.faq-info containing a
    div.faq-info-title (question) and div.faq-info-description (answer).
    """
    qa_pairs: list[dict] = []

    faq_section = soup.find(id="faq")
    if not faq_section:
        for tag in soup.find_all(["div", "section"], class_=True):
            if "faq" in " ".join(tag.get("class", [])).lower():
                faq_section = tag
                break

    if faq_section:
        for item in faq_section.find_all("div", class_="faq-info"):
            title = item.find("div", class_="faq-info-title")
            desc = item.find("div", class_="faq-info-description")
            if not title or not desc:
                continue
            # Skip "Show X more" and "Ask a question" entries
            title_cls = " ".join(title.get("class", []))
            if "faq_show_more" in title_cls:
                continue
            q = title.get_text(strip=True)
            a = desc.get_text(strip=True)
            if q and a and q.lower() != "ask a question":
                qa_pairs.append({"question": q, "answer": a})

        if qa_pairs:
            return qa_pairs

    # Fallback: Schema.org FAQPage structured data
    for script in soup.find_all("script", type="application/ld+json"):
        try:
            ld = json.loads(script.string or "")
            if isinstance(ld, dict) and ld.get("@type") == "FAQPage":
                for item in ld.get("mainEntity", []):
                    q = item.get("name", "")
                    a_obj = item.get("acceptedAnswer", {})
                    a = a_obj.get("text", "") if isinstance(a_obj, dict) else ""
                    if q and a:
                        qa_pairs.append({"question": q, "answer": a})
        except (json.JSONDecodeError, TypeError):
            continue

    return qa_pairs


def _extract_pros(soup: BeautifulSoup) -> list[str]:
    """Extract pros from div.pac-info-item-pros > div.pac-elem elements."""
    pros: list[str] = []
    container = soup.find("div", class_="pac-info-item-pros")
    if container:
        for elem in container.find_all("div", class_="pac-elem"):
            text = elem.get_text(strip=True)
            if text:
                pros.append(text)
    return pros


def _extract_cons(soup: BeautifulSoup) -> list[str]:
    """Extract cons from div.pac-info-item-cons > div.pac-elem elements."""
    cons: list[str] = []
    container = soup.find("div", class_="pac-info-item-cons")
    if container:
        for elem in container.find_all("div", class_="pac-elem"):
            text = elem.get_text(strip=True)
            if text:
                cons.append(text)
    return cons


def _extract_last_updated(soup: BeautifulSoup) -> str | None:
    """Extract the latest release date from the releases section.

    Finds all div.changelog_title elements inside #releases, parses
    the dates, and returns the most recent one as YYYY-MM-DD.
    Returns None if no releases section or no parseable dates exist.
    """
    releases = soup.find(id="releases")
    if not releases:
        return None

    dates = []
    for title_div in releases.find_all("div", class_="changelog_title"):
        text = title_div.get_text(strip=True)
        # Try parsing common date formats: "Feb 11, 2026" or "February 17, 2026"
        for fmt in ("%b %d, %Y", "%B %d, %Y", "%b %d %Y", "%B %d %Y"):
            try:
                dt = datetime.strptime(text, fmt)
                dates.append(dt)
                break
            except ValueError:
                continue

    if dates:
        latest = max(dates)
        return latest.strftime("%Y-%m-%d")

    return None


def _safe_parse_int(text: str) -> int | None:
    """Parse a numeric string like '42,799' to int, returning None on failure."""
    cleaned = text.replace(",", "").strip()
    if cleaned.isdigit() and cleaned:
        return int(cleaned)
    return None


def _extract_traffic_count(soup: BeautifulSoup) -> int | None:
    """Extract the tool's traffic/opens count from the header area.

    The main tool's stats_opens lives inside the header column
    (ancestor of the <h1>), NOT in the breadcrumb dropdown listings.
    """
    h1 = soup.find("h1")
    if h1:
        # Walk up to find the header column container
        for parent in h1.parents:
            cls = " ".join(parent.get("class", []))
            if "header_col" in cls or "title_wrap" in cls:
                el = parent.find("div", class_="stats_opens")
                if el:
                    num = _safe_parse_int(el.get_text(strip=True))
                    if num is not None:
                        return num
    return None


def _extract_leaderboard_score(soup: BeautifulSoup) -> int | None:
    """Extract the leaderboard score from <span class='score'>."""
    el = soup.find("span", class_="score")
    if el:
        return _safe_parse_int(el.get_text(strip=True))
    return None


def _extract_rating(soup: BeautifulSoup) -> tuple[float | None, int | None]:
    """Extract rating value and count."""
    rating = None
    count = None

    a = soup.select_one("a.rating_top")
    if a:
        rating_span = a.select_one(":scope > span:not(.star)")
        if rating_span:
            direct_text = rating_span.find(string=True, recursive=False)
            if direct_text and direct_text.strip():
                try:
                    rating = float(direct_text.strip())
                except ValueError:
                    pass

        count_el = a.select_one(".ratings_count")
        if count_el:
            digits = re.sub(r"\D+", "", count_el.get_text(strip=True))
            if digits:
                try:
                    count = int(digits)
                except ValueError:
                    pass

    if rating is not None or count is not None:
        return (rating or 0.0), (count or 0)
    
    # Look for structured rating data (Schema.org)
    for script in soup.find_all("script", type="application/ld+json"):
        try:
            ld = json.loads(script.string or "")
            if isinstance(ld, dict):
                agg = ld.get("aggregateRating", {})
                if agg:
                    rating = float(agg.get("ratingValue", 0)) or None
                    count = int(agg.get("ratingCount", 0)) or None
                    if rating:
                        return rating, count
        except (json.JSONDecodeError, TypeError, ValueError):
            continue

    # Text-based search for rating patterns
    text = soup.get_text(" ", strip=True)
    match = re.search(r"(\d+\.?\d*)\s*/\s*5(?:\s*\((\d+)\s*(?:rating|review))", text, re.IGNORECASE)
    if match:
        rating = float(match.group(1))
        if match.group(2):
            count = int(match.group(2))
        return rating, count

    return rating, count


def fetch_tools(conn, session: cffi_requests.Session | None = None,
                max_items: int | None = None) -> dict:
    """Run Phase 2: fetch and parse individual tool pages for pending jobs.

    Returns stats dict.
    """
    if session is None:
        session = _make_session()

    limit = max_items or MAX_PER_SESSION
    pending = get_pending_jobs(conn, limit)

    if not pending:
        print("No pending jobs to process.")
        return {"processed": 0, "done": 0, "filtered": 0, "failed": 0, "skipped": 0}

    stats = {"processed": 0, "done": 0, "filtered": 0, "failed": 0, "skipped": 0}
    total = len(pending)

    print(f"Processing {total} pending jobs (session limit: {limit})")

    for i, job in enumerate(pending, 1):
        slug = job["slug"]
        url = job["taaft_url"] or f"{BASE_URL}/ai/{slug}/"

        logger.info(f"Fetching [{i}/{total}]: {slug}")

        html, status_code = _fetch_page(session, url)
        stats["processed"] += 1

        if status_code == 404:
            mark_job(conn, slug, "skipped")
            stats["skipped"] += 1
            logger.info(f"  {slug}: 404 — skipped")
        elif html is None:
            mark_job(conn, slug, "failed")
            stats["failed"] += 1
            logger.error(f"  {slug}: fetch failed")
        else:
            try:
                data = parse_tool_page(html, slug)

                # Verify pricing on detail page
                pricing = data.get("pricing_model")
                if pricing and not is_free_from_detail_page(pricing):
                    mark_job(conn, slug, "filtered")
                    stats["filtered"] += 1
                    logger.info(f"  {slug}: filtered (pricing: {pricing})")
                else:
                    upsert_agent(conn, data)
                    mark_job(conn, slug, "done")
                    stats["done"] += 1
                    is_agent = data.get("is_agent_tagged") or data.get("is_agent_inferred")
                    logger.info(
                        f"  {slug}: done (agent={is_agent}, "
                        f"confidence={data.get('agent_confidence_score')})"
                    )
            except Exception as e:
                mark_job(conn, slug, "failed")
                stats["failed"] += 1
                logger.error(f"  {slug}: parse error — {e}", exc_info=True)

        # Progress output
        print(
            f"\r  Processed {i}/{total} | "
            f"Done: {stats['done']} | "
            f"Filtered: {stats['filtered']} | "
            f"Failed: {stats['failed']} | "
            f"Skipped: {stats['skipped']}",
            end="",
            flush=True,
        )

        if i < total:
            _delay()

    print()  # newline after progress
    remaining = total - stats["processed"]
    print(f"\nFetch complete. Remaining pending: check with 'status' command")

    return stats
