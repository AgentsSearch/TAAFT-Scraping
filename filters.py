"""Pricing filter logic and agent detection heuristics."""

import re

from config import (
    FREE_PRICING_LABELS,
    DISCARD_PRICING_PATTERNS,
    AGENT_KEYWORDS,
    AGENT_TASK_CATEGORIES,
)


def is_free_from_card(pricing_label: str) -> bool:
    """Check if a listing card's pricing label indicates 100% free.

    Returns True only for genuinely free tools; False for freemium/paid/trial.
    """
    if not pricing_label:
        return False

    normalized = pricing_label.strip().lower()

    # Check for exact free match
    if normalized in FREE_PRICING_LABELS:
        return True

    # Check for discard patterns
    for pattern in DISCARD_PRICING_PATTERNS:
        if pattern in normalized:
            return False

    # If it's just "free" with no qualifier, treat as potentially free
    if normalized == "free":
        return True

    return False


def is_free_from_detail_page(pricing_model: str) -> bool:
    """Check pricing from the individual tool page's structured pricing section.

    Returns True only if pricing model is explicitly "Free".
    """
    if not pricing_model:
        return False

    normalized = pricing_model.strip().lower()

    if normalized == "free":
        return True

    # Reject freemium, paid, etc.
    if any(p in normalized for p in ("freemium", "paid", "trial")):
        return False

    return False


def detect_agent_badge(has_agent_link: bool) -> bool:
    """Check if the page has an explicit [Agent] badge (link to /agents/)."""
    return has_agent_link


def detect_agent_by_categories(task_categories: list[str]) -> bool:
    """Check if any task category indicates an agent."""
    for cat in task_categories:
        if cat.strip().lower() in AGENT_TASK_CATEGORIES:
            return True
    return False


def detect_agent_by_description(description: str) -> bool:
    """Check if description contains 2+ agent-related keywords."""
    if not description:
        return False

    desc_lower = description.lower()
    matches = sum(1 for kw in AGENT_KEYWORDS if kw in desc_lower)
    return matches >= 2


def compute_agent_confidence(
    has_badge: bool,
    category_match: bool,
    description_match: bool,
) -> int:
    """Return 0-3 confidence score based on how many signals matched."""
    return sum([has_badge, category_match, description_match])


def classify_agent(
    has_agent_link: bool,
    task_categories: list[str],
    description: str,
) -> dict:
    """Run all agent detection checks and return classification dict.

    Returns:
        dict with keys: is_agent_tagged, is_agent_inferred, agent_confidence_score
    """
    badge = detect_agent_badge(has_agent_link)
    cat_match = detect_agent_by_categories(task_categories)
    desc_match = detect_agent_by_description(description)

    confidence = compute_agent_confidence(badge, cat_match, desc_match)

    return {
        "is_agent_tagged": badge,
        "is_agent_inferred": cat_match or desc_match,
        "agent_confidence_score": confidence,
    }
