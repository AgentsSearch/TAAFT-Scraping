"""Export agents table to JSON or CSV for the indexing team."""

import csv
import json
import logging
from pathlib import Path

from db import get_all_agents

logger = logging.getLogger("taaft.export")


def export_json(conn, output_path: str, agents_only: bool = False) -> int:
    """Export agents to JSON file.

    Args:
        conn: SQLite connection
        output_path: path for output file
        agents_only: if True, only export tools classified as agents

    Returns:
        Number of records exported.
    """
    agents = get_all_agents(conn)

    if agents_only:
        agents = [a for a in agents if a.get("is_agent")]

    # Shape output to match spec format
    records = []
    for a in agents:
        records.append({
            "slug": a["slug"],
            "name": a["name"],
            "taaft_url": a["taaft_url"],
            "external_url": a["external_url"],
            "description": a["description"],
            "pricing_model": a["pricing_model"],
            "is_agent": a.get("is_agent", False),
            # "agent_confidence_score": a["agent_confidence_score"],
            "task_categories": a["task_categories"],
            # "qa_content": a["qa_content"],
            "pros": a.get("pros", []),
            "cons": a.get("cons", []),
            "traffic_count": a["saves_count"],
            "leaderboard_score": a.get("leaderboard_score"),
            "rating": a["rating"],
            "rating_count": a.get("rating_count"),
            "last_updated": a.get("last_updated"),
            "scraped_at": a["scraped_at"],
        })

    Path(output_path).parent.mkdir(parents=True, exist_ok=True)
    with open(output_path, "w", encoding="utf-8") as f:
        json.dump(records, f, indent=2, ensure_ascii=False)

    logger.info(f"Exported {len(records)} records to {output_path}")
    return len(records)


def export_csv(conn, output_path: str, agents_only: bool = False) -> int:
    """Export agents to CSV file.

    Args:
        conn: SQLite connection
        output_path: path for output file
        agents_only: if True, only export tools classified as agents

    Returns:
        Number of records exported.
    """
    agents = get_all_agents(conn)

    if agents_only:
        agents = [a for a in agents if a.get("is_agent")]

    if not agents:
        print("No records to export.")
        return 0

    fieldnames = [
        "slug", "name", "taaft_url", "external_url", "description",
        "pricing_model", "is_agent", "agent_confidence_score",
        "task_categories", "qa_content", "pros", "cons",
        "traffic_count", "leaderboard_score", "rating", "rating_count",
        "last_updated", "scraped_at",
    ]

    Path(output_path).parent.mkdir(parents=True, exist_ok=True)
    with open(output_path, "w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=fieldnames, extrasaction="ignore")
        writer.writeheader()
        for a in agents:
            row = {
                "slug": a["slug"],
                "name": a["name"],
                "taaft_url": a["taaft_url"],
                "external_url": a["external_url"],
                "description": a["description"],
                "pricing_model": a["pricing_model"],
                "is_agent": a.get("is_agent", False),
                "agent_confidence_score": a["agent_confidence_score"],
                "task_categories": json.dumps(a["task_categories"]),
                # "qa_content": json.dumps(a["qa_content"]),
                "pros": json.dumps(a.get("pros", [])),
                "cons": json.dumps(a.get("cons", [])),
                "traffic_count": a["saves_count"],
                "leaderboard_score": a.get("leaderboard_score"),
                "rating": a["rating"],
                "rating_count": a.get("rating_count"),
                "last_updated": a.get("last_updated"),
                "scraped_at": a["scraped_at"],
            }
            writer.writerow(row)

    logger.info(f"Exported {len(agents)} records to {output_path}")
    return len(agents)
