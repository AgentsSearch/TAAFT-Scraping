#!/usr/bin/env python3
"""TAAFT Scraper — CLI entry point.

Usage:
    python scraper.py harvest          # Phase 1: populate job queue from listings
    python scraper.py fetch            # Phase 2: process pending jobs
    python scraper.py run              # Both phases end to end
    python scraper.py status           # Show queue and agent stats
    python scraper.py export -o FILE   # Export agents to JSON or CSV
    python scraper.py reset-failed     # Re-queue failed jobs
"""

import argparse
import logging
import sys
from pathlib import Path

from config import DB_PATH, LOG_FILE
from db import get_connection, init_db, get_stats, reset_failed_jobs, reset_all_for_refetch
from harvester import harvest
from fetcher import fetch_tools
from export import export_json, export_csv


def setup_logging() -> None:
    """Configure file + console logging."""
    logger = logging.getLogger("taaft")
    logger.setLevel(logging.DEBUG)

    # File handler — detailed
    fh = logging.FileHandler(LOG_FILE, encoding="utf-8")
    fh.setLevel(logging.DEBUG)
    fh.setFormatter(logging.Formatter(
        "%(asctime)s | %(name)s | %(levelname)s | %(message)s"
    ))
    logger.addHandler(fh)

    # Console handler — info only
    ch = logging.StreamHandler()
    ch.setLevel(logging.INFO)
    ch.setFormatter(logging.Formatter("%(levelname)s: %(message)s"))
    logger.addHandler(ch)


def cmd_harvest(args) -> None:
    """Run Phase 1 only."""
    conn = get_connection()
    init_db(conn)
    print("=== Phase 1: Harvesting listing pages ===\n")
    stats = harvest(conn)
    conn.close()


def cmd_fetch(args) -> None:
    """Run Phase 2 only."""
    conn = get_connection()
    init_db(conn)

    if args.refetch:
        count = reset_all_for_refetch(conn)
        print(f"Reset {count} done jobs back to pending for refetch.\n")

    print("=== Phase 2: Fetching individual tool pages ===\n")
    stats = fetch_tools(conn, max_items=args.limit)
    conn.close()


def cmd_run(args) -> None:
    """Run both phases."""
    conn = get_connection()
    init_db(conn)

    if args.refetch:
        count = reset_all_for_refetch(conn)
        print(f"Reset {count} done jobs back to pending for refetch.\n")

    print("=== Phase 1: Harvesting listing pages ===\n")
    harvest(conn)

    print("\n=== Phase 2: Fetching individual tool pages ===\n")
    fetch_tools(conn, max_items=args.limit)

    print("\n=== Final Status ===")
    _print_stats(get_stats(conn))
    conn.close()


def cmd_status(args) -> None:
    """Show queue and agent statistics."""
    conn = get_connection()
    init_db(conn)
    stats = get_stats(conn)
    _print_stats(stats)
    conn.close()


def cmd_export(args) -> None:
    """Export agents to file."""
    conn = get_connection()
    init_db(conn)

    output = args.output
    fmt = args.format

    # Auto-detect format from extension if not specified
    if not fmt:
        if output.endswith(".csv"):
            fmt = "csv"
        else:
            fmt = "json"

    if fmt == "csv":
        count = export_csv(conn, output, agents_only=args.agents_only)
    else:
        count = export_json(conn, output, agents_only=args.agents_only)

    print(f"Exported {count} records to {output}")
    conn.close()


def cmd_reset_failed(args) -> None:
    """Reset failed jobs back to pending."""
    conn = get_connection()
    init_db(conn)
    count = reset_failed_jobs(conn)
    print(f"Reset {count} failed jobs back to pending.")
    conn.close()


def cmd_reset_agents(args) -> None:
    """Clear the agents table and re-queue done jobs for re-fetching."""
    conn = get_connection()
    init_db(conn)
    agents_deleted = conn.execute("SELECT COUNT(*) FROM agents").fetchone()[0]
    conn.execute("DELETE FROM agents")
    jobs_reset = conn.execute(
        "UPDATE jobs SET status = 'pending', scraped_at = NULL WHERE status = 'done'"
    ).rowcount
    conn.commit()
    print(f"Deleted {agents_deleted} agents, reset {jobs_reset} done jobs back to pending.")
    conn.close()


def _print_stats(stats: dict) -> None:
    """Pretty-print statistics."""
    print(f"\n  Job Queue:")
    print(f"    Total jobs:    {stats['jobs_total']}")
    print(f"    Pending:       {stats['jobs_pending']}")
    print(f"    Done:          {stats['jobs_done']}")
    print(f"    Filtered:      {stats['jobs_filtered']}")
    print(f"    Failed:        {stats['jobs_failed']}")
    print(f"    Skipped:       {stats['jobs_skipped']}")
    print(f"\n  Agents Table:")
    print(f"    Total stored:  {stats['agents_total']}")
    print(f"    Agents (confirmed): {stats['agents_confirmed']}")
    print(f"    Non-agent tools:    {stats['agents_non_agent']}")
    print()


def main() -> None:
    parser = argparse.ArgumentParser(
        description="TAAFT Scraper — collect free AI agents from theresanaiforthat.com"
    )
    subparsers = parser.add_subparsers(dest="command", help="Available commands")

    # harvest
    sub_harvest = subparsers.add_parser("harvest", help="Phase 1: crawl listing pages")
    sub_harvest.set_defaults(func=cmd_harvest)

    # fetch
    sub_fetch = subparsers.add_parser("fetch", help="Phase 2: fetch tool pages")
    sub_fetch.add_argument("--limit", type=int, default=None,
                           help="Max tools to process this session")
    sub_fetch.add_argument("--refetch", action="store_true",
                           help="Re-fetch tools already marked done")
    sub_fetch.set_defaults(func=cmd_fetch)

    # run
    sub_run = subparsers.add_parser("run", help="Run both phases")
    sub_run.add_argument("--limit", type=int, default=None,
                         help="Max tools to process in Phase 2")
    sub_run.add_argument("--refetch", action="store_true",
                         help="Re-fetch tools already marked done")
    sub_run.set_defaults(func=cmd_run)

    # status
    sub_status = subparsers.add_parser("status", help="Show queue statistics")
    sub_status.set_defaults(func=cmd_status)

    # export
    sub_export = subparsers.add_parser("export", help="Export agents to file")
    sub_export.add_argument("-o", "--output", required=True,
                            help="Output file path (.json or .csv)")
    sub_export.add_argument("--format", choices=["json", "csv"], default=None,
                            help="Output format (auto-detected from extension)")
    sub_export.add_argument("--agents-only", action="store_true",
                            help="Only export tools classified as agents")
    sub_export.set_defaults(func=cmd_export)

    # reset-failed
    sub_reset = subparsers.add_parser("reset-failed", help="Re-queue failed jobs")
    sub_reset.set_defaults(func=cmd_reset_failed)

    # reset-agents
    sub_reset_agents = subparsers.add_parser(
        "reset-agents", help="Clear agents table and re-queue done jobs"
    )
    sub_reset_agents.set_defaults(func=cmd_reset_agents)

    args = parser.parse_args()

    if not args.command:
        parser.print_help()
        sys.exit(1)

    setup_logging()
    args.func(args)


if __name__ == "__main__":
    main()
