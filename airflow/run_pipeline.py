#!/usr/bin/env python3
"""
Standalone runner for the findDiscord pipeline.
Run this directly without Airflow to test the full workflow:

    python3 airflow/run_pipeline.py "Matt's AI heroes"

This executes the same steps as the Airflow DAG, sequentially.
"""

import sys
import os

# Ensure tasks module is importable
sys.path.insert(0, os.path.dirname(__file__))

from tasks.discord_api import fetch_guilds, find_server, fetch_channels, fetch_all_messages
from tasks.summarizer import build_channel_summary
from tasks.report_generator import generate_markdown_report
from tasks.file_manager import save_summary_to_file, git_commit_and_push


def run_pipeline(server_name: str):
    """
    Execute the full findDiscord pipeline:

    Step 1: fetch_guilds     -> Get all Discord servers
    Step 2: find_server      -> Match target server by name
    Step 3: fetch_channels   -> Get text channels in server
    Step 4: fetch_messages   -> Fetch recent messages (all channels)
    Step 5: summarize        -> Parse, filter AI content, extract tools/links
    Step 6: generate_report  -> Combine into markdown report
    Step 7: save_to_file     -> Write timestamped .md file
    Step 8: git_push         -> Commit & push to GitHub
    """
    print("=" * 60)
    print(f"  findDiscord Pipeline: {server_name}")
    print("=" * 60)

    # Step 1
    print("\n[Step 1/8] Fetching guilds...")
    guilds = fetch_guilds()

    # Step 2
    print("\n[Step 2/8] Finding server...")
    server = find_server(guilds, server_name)
    if not server:
        print(f"ERROR: Server '{server_name}' not found!")
        sys.exit(1)

    # Step 3
    print("\n[Step 3/8] Fetching channels...")
    channels = fetch_channels(server["id"])

    # Step 4
    print("\n[Step 4/8] Fetching messages from all channels...")
    all_messages = fetch_all_messages(channels)

    # Step 5
    print("\n[Step 5/8] Summarizing channels...")
    channel_summaries = []
    for channel_name, messages in all_messages.items():
        summary = build_channel_summary(channel_name, messages)
        channel_summaries.append(summary)

    # Step 6
    print("\n[Step 6/8] Generating report...")
    report = generate_markdown_report(server_name, channel_summaries)

    # Step 7
    print("\n[Step 7/8] Saving to file...")
    filepath = save_summary_to_file(report)

    # Step 8
    print("\n[Step 8/8] Git commit & push...")
    git_commit_and_push(filepath, server_name)

    print("\n" + "=" * 60)
    print(f"  Pipeline complete! Report saved to: {filepath}")
    print("=" * 60)


if __name__ == "__main__":
    name = sys.argv[1] if len(sys.argv) > 1 else "Matt's AI heroes"
    run_pipeline(name)
