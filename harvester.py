"""Phase 1: Crawl listing/period pages, collect tool slugs, and populate the job queue."""

import logging
import re
import time
import random
from urllib.parse import urljoin, urlparse

from bs4 import BeautifulSoup
from curl_cffi import requests as cffi_requests

from config import (
    LISTING_URLS,
    BASE_URL,
    FREE_BASE_URL,
    IMPERSONATE_BROWSER,
    REQUEST_TIMEOUT,
    DEFAULT_HEADERS,
    BASE_DELAY,
    BACKOFF_ON_429,
    MAX_RETRIES,
)
from db import upsert_job
from filters import is_free_from_card

logger = logging.getLogger("taaft.harvester")


def _make_session() -> cffi_requests.Session:
    """Create a curl_cffi session with Chrome impersonation."""
    session = cffi_requests.Session(impersonate=IMPERSONATE_BROWSER)
    session.headers.update(DEFAULT_HEADERS)
    return session


def _fetch_page(session: cffi_requests.Session, url: str) -> str | None:
    """Fetch a URL with retries and rate-limit handling. Returns HTML or None."""
    for attempt in range(1, MAX_RETRIES + 1):
        try:
            resp = session.get(url, timeout=REQUEST_TIMEOUT)

            if resp.status_code == 429:
                logger.warning(f"429 rate limit on {url}, sleeping {BACKOFF_ON_429}s")
                time.sleep(BACKOFF_ON_429)
                continue

            if resp.status_code == 404:
                logger.info(f"404 not found: {url}")
                return None

            if resp.status_code >= 400:
                logger.warning(f"HTTP {resp.status_code} on {url} (attempt {attempt})")
                time.sleep(BASE_DELAY[1])
                continue

            return resp.text

        except Exception as e:
            logger.warning(f"Request error on {url} (attempt {attempt}): {e}")
            if attempt < MAX_RETRIES:
                time.sleep(REQUEST_TIMEOUT * 2 if attempt == 2 else BASE_DELAY[1])

    logger.error(f"Failed to fetch {url} after {MAX_RETRIES} attempts")
    return None


def _delay():
    """Random delay between requests."""
    time.sleep(random.uniform(*BASE_DELAY))


def _extract_slug(href: str) -> str | None:
    """Extract tool slug from a URL like /ai/some-tool/."""
    match = re.search(r"/ai/([^/]+)/?", href)
    return match.group(1) if match else None


def _normalize_taaft_url(slug: str) -> str:
    """Build canonical TAAFT URL for a slug."""
    return f"{BASE_URL}/ai/{slug}/"


def _parse_listing_page(html: str, source_url: str) -> tuple[list[dict], list[str]]:
    """Parse a listing page for tool cards and 'View all' links.

    Returns:
        (tools, view_all_links) where tools is a list of dicts with
        slug, taaft_url, pricing_label_raw and view_all_links are URLs to follow.
    """
    soup = BeautifulSoup(html, "lxml")
    tools = []
    view_all_links = []

    # Look for "View all X AIs" links on period pages
    for a in soup.find_all("a", href=True):
        text = a.get_text(strip=True).lower()
        if "view all" in text and "ai" in text:
            link = urljoin(source_url, a["href"])
            view_all_links.append(link)

    # Parse tool cards — they appear as <li> elements or divs with tool links
    # Strategy: find all links matching /ai/slug/ pattern and extract context
    seen_slugs = set()

    # Try finding tool cards in common container patterns
    for card in _find_tool_cards(soup):
        tool = _extract_tool_from_card(card, source_url)
        if tool and tool["slug"] not in seen_slugs:
            seen_slugs.add(tool["slug"])
            tools.append(tool)

    # Fallback: scan all links with /ai/ pattern if no cards found
    if not tools:
        for a in soup.find_all("a", href=True):
            slug = _extract_slug(a["href"])
            if slug and slug not in seen_slugs:
                seen_slugs.add(slug)
                pricing = _find_pricing_near_element(a)
                tools.append({
                    "slug": slug,
                    "taaft_url": _normalize_taaft_url(slug),
                    "pricing_label_raw": pricing or "",
                })

    return tools, view_all_links


def _find_tool_cards(soup: BeautifulSoup) -> list:
    """Find tool card elements using multiple strategies."""
    cards = []

    # Strategy 1: <li> elements containing /ai/ links
    for li in soup.find_all("li"):
        link = li.find("a", href=re.compile(r"/ai/[^/]+/?"))
        if link:
            cards.append(li)

    if cards:
        return cards

    # Strategy 2: divs/articles with tool-related classes
    for tag in soup.find_all(["div", "article", "section"]):
        classes = " ".join(tag.get("class", []))
        if any(kw in classes.lower() for kw in ("tool", "card", "item", "ai-")):
            link = tag.find("a", href=re.compile(r"/ai/[^/]+/?"))
            if link:
                cards.append(tag)

    return cards


def _extract_tool_from_card(card, source_url: str) -> dict | None:
    """Extract tool data from a single card element."""
    link = card.find("a", href=re.compile(r"/ai/[^/]+/?"))
    if not link:
        return None

    slug = _extract_slug(link["href"])
    if not slug:
        return None

    # Get pricing label from card text
    pricing = _find_pricing_in_card(card)

    return {
        "slug": slug,
        "taaft_url": _normalize_taaft_url(slug),
        "pricing_label_raw": pricing or "",
    }


def _find_pricing_in_card(card) -> str:
    """Extract pricing text from a card element."""
    card_text = card.get_text(" ", strip=True).lower()

    # Look for explicit pricing patterns
    pricing_patterns = [
        r"100\s*%\s*free",
        r"free\s*\+\s*from\s*\$[\d.]+",
        r"freemium",
        r"free\s*trial",
        r"paid",
        r"free",
    ]

    for pattern in pricing_patterns:
        match = re.search(pattern, card_text, re.IGNORECASE)
        if match:
            return match.group(0).strip()

    return ""


def _find_pricing_near_element(element) -> str:
    """Try to find pricing text near a link element."""
    parent = element.parent
    if parent:
        return _find_pricing_in_card(parent)
    return ""


def harvest(conn, session: cffi_requests.Session | None = None) -> dict:
    """Run Phase 1: crawl all listing pages and populate the job queue.

    Returns stats dict with counts of queued, filtered, total tools found.
    """
    if session is None:
        session = _make_session()

    stats = {"pages_crawled": 0, "tools_found": 0, "queued": 0, "filtered": 0}
    urls_to_crawl = list(LISTING_URLS)
    crawled_urls = set()

    while urls_to_crawl:
        url = urls_to_crawl.pop(0)

        if url in crawled_urls:
            continue
        crawled_urls.add(url)

        logger.info(f"Harvesting: {url}")
        print(f"  Crawling: {url}")

        html = _fetch_page(session, url)
        if not html:
            logger.warning(f"No content from {url}")
            continue

        stats["pages_crawled"] += 1
        tools, view_all_links = _parse_listing_page(html, url)

        # Queue "View all" links for crawling
        for link in view_all_links:
            if link not in crawled_urls:
                urls_to_crawl.append(link)
                logger.info(f"  Found 'View all' link: {link}")

        # Process discovered tools
        for tool in tools:
            stats["tools_found"] += 1
            slug = tool["slug"]
            pricing_raw = tool["pricing_label_raw"]

            # On the agents page, we might not have pricing — queue anyway
            is_agents_page = "agents" in url.lower()

            if is_free_from_card(pricing_raw) or (is_agents_page and not pricing_raw):
                new = upsert_job(
                    conn,
                    slug=slug,
                    taaft_url=tool["taaft_url"],
                    pricing_label_raw=pricing_raw,
                    status="pending",
                )
                if new:
                    stats["queued"] += 1
            else:
                # Insert as filtered so we track what we've seen
                upsert_job(
                    conn,
                    slug=slug,
                    taaft_url=tool["taaft_url"],
                    pricing_label_raw=pricing_raw,
                    status="filtered",
                )
                stats["filtered"] += 1

        logger.info(
            f"  Found {len(tools)} tools, {len(view_all_links)} 'View all' links"
        )
        print(
            f"    Found {len(tools)} tools | "
            f"Running total — Queued: {stats['queued']} | Filtered: {stats['filtered']}"
        )

        _delay()

    print(f"\nHarvest complete:")
    print(f"  Pages crawled: {stats['pages_crawled']}")
    print(f"  Tools found:   {stats['tools_found']}")
    print(f"  Queued (free): {stats['queued']}")
    print(f"  Filtered out:  {stats['filtered']}")

    return stats
