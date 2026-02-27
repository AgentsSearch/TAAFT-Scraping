"""Phase 2: Fetch individual tool pages, parse structured data, and store results."""

import json
import logging
import re
import time
import random
from datetime import datetime, timezone

from bs4 import BeautifulSoup
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
    data["qa_content"] = _extract_qa_content(soup)

    # Saves count
    data["saves_count"] = _extract_saves_count(soup)

    # Rating
    rating, rating_count = _extract_rating(soup)
    data["rating"] = rating
    data["rating_count"] = rating_count

    # Timestamp
    data["scraped_at"] = datetime.now(timezone.utc).isoformat()

    return data


def _extract_description(soup: BeautifulSoup) -> str | None:
    """Extract the main description/overview text."""
    # Try id="ai_overview" or similar
    for selector_id in ("ai_overview", "overview", "description"):
        section = soup.find(id=selector_id)
        if section:
            text = section.get_text(" ", strip=True)
            if len(text) > 20:
                return text

    # Try class-based search
    for tag in soup.find_all(["div", "section", "p"], class_=True):
        classes = " ".join(tag.get("class", []))
        if "overview" in classes.lower() or "description" in classes.lower():
            text = tag.get_text(" ", strip=True)
            if len(text) > 20:
                return text

    # Try meta description
    meta = soup.find("meta", attrs={"name": "description"})
    if meta and meta.get("content"):
        return meta["content"].strip()

    # Try first substantial paragraph after h1
    h1 = soup.find("h1")
    if h1:
        for sibling in h1.find_next_siblings(["p", "div"]):
            text = sibling.get_text(strip=True)
            if len(text) > 40:
                return text

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


def _extract_task_categories(soup: BeautifulSoup) -> list[str]:
    """Extract all task category labels from /task/ links."""
    categories = []
    seen = set()
    for a in soup.find_all("a", href=True):
        if "/task/" in a["href"]:
            text = a.get_text(strip=True)
            if text and text.lower() not in seen:
                seen.add(text.lower())
                categories.append(text)
    return categories


def _extract_qa_content(soup: BeautifulSoup) -> list[dict]:
    """Extract Q&A/FAQ pairs from the page."""
    qa_pairs = []

    # Try FAQ section by id
    faq_section = soup.find(id="faq")
    if not faq_section:
        # Try class-based search
        for tag in soup.find_all(["div", "section"], class_=True):
            classes = " ".join(tag.get("class", []))
            if "faq" in classes.lower():
                faq_section = tag
                break

    if faq_section:
        # Strategy 1: dt/dd pairs
        dts = faq_section.find_all("dt")
        dds = faq_section.find_all("dd")
        for dt, dd in zip(dts, dds):
            q = dt.get_text(strip=True)
            a = dd.get_text(strip=True)
            if q and a:
                qa_pairs.append({"question": q, "answer": a})

        if qa_pairs:
            return qa_pairs

        # Strategy 2: heading + paragraph/div pairs
        headings = faq_section.find_all(["h2", "h3", "h4", "strong"])
        for heading in headings:
            question = heading.get_text(strip=True)
            answer_parts = []
            for sibling in heading.find_next_siblings():
                if sibling.name in ("h2", "h3", "h4", "strong"):
                    break
                text = sibling.get_text(strip=True)
                if text:
                    answer_parts.append(text)
            if question and answer_parts:
                qa_pairs.append({
                    "question": question,
                    "answer": " ".join(answer_parts),
                })

        if qa_pairs:
            return qa_pairs

    # Strategy 3: look for Schema.org FAQPage structured data
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


def _safe_parse_int(text: str) -> int | None:
    """Parse a numeric string like '42,799' to int, returning None on failure."""
    cleaned = text.replace(",", "").strip()
    if cleaned.isdigit() and cleaned:
        return int(cleaned)
    return None


def _extract_saves_count(soup: BeautifulSoup) -> int | None:
    """Extract the saves/bookmark count."""
    # Look for save-related text patterns
    for tag in soup.find_all(string=re.compile(r"\d[\d,]*\s*save", re.IGNORECASE)):
        match = re.search(r"(\d[\d,]*)\s*save", tag, re.IGNORECASE)
        if match:
            num = _safe_parse_int(match.group(1))
            if num is not None:
                return num

    # Look for elements with save-related classes
    for tag in soup.find_all(["span", "div", "button"], class_=True):
        classes = " ".join(tag.get("class", []))
        if "save" in classes.lower() or "bookmark" in classes.lower():
            text = tag.get_text(strip=True)
            match = re.search(r"(\d[\d,]*)", text)
            if match:
                num = _safe_parse_int(match.group(1))
                if num is not None and num > 0:
                    return num

    # Broader search for large numbers near save/bookmark icons
    for tag in soup.find_all(["span", "div"]):
        text = tag.get_text(strip=True)
        if re.match(r"^\d[\d,]*$", text):
            num = _safe_parse_int(text)
            if num is not None and num > 10:
                parent = tag.parent
                if parent:
                    parent_text = parent.get_text(" ", strip=True).lower()
                    if "save" in parent_text or "bookmark" in parent_text:
                        return num

    return None


def _extract_rating(soup: BeautifulSoup) -> tuple[float | None, int | None]:
    """Extract rating value and count."""
    rating = None
    count = None

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
