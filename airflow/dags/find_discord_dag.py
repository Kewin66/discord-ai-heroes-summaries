"""
=============================================================================
 findDiscord Airflow DAG
=============================================================================

 This DAG models the complete /findDiscord workflow as a data pipeline.
 It runs daily at 5PM and summarizes AI-relevant topics from a Discord server.

 DAG Structure (visual):

   ┌─────────────┐
   │ fetch_guilds │  Step 1: Get all Discord servers
   └──────┬──────┘
          │
   ┌──────▼──────┐
   │ find_server  │  Step 2: Match server by name
   └──────┬──────┘
          │
   ┌──────▼────────┐
   │fetch_channels  │  Step 3: Get all text channels in server
   └──────┬────────┘
          │
          ├─────────────────┬─────────────────┐
          │                 │                 │
   ┌──────▼──────┐  ┌──────▼──────┐  ┌──────▼──────┐
   │fetch_msgs_1 │  │fetch_msgs_2 │  │fetch_msgs_N │  Step 4: Fetch messages
   │ (#ai-hero)  │  │ (#general)  │  │  (#evalite) │  (parallel per channel)
   └──────┬──────┘  └──────┬──────┘  └──────┬──────┘
          │                 │                 │
   ┌──────▼──────┐  ┌──────▼──────┐  ┌──────▼──────┐
   │ summarize_1 │  │ summarize_2 │  │ summarize_N │  Step 5: Parse & filter
   │             │  │             │  │             │  (parallel per channel)
   └──────┬──────┘  └──────┬──────┘  └──────┬──────┘
          │                 │                 │
          └─────────────────┼─────────────────┘
                            │
                   ┌────────▼────────┐
                   │ generate_report │  Step 6: Combine into markdown
                   └────────┬────────┘
                            │
                   ┌────────▼────────┐
                   │  save_to_file   │  Step 7: Write timestamped .md file
                   └────────┬────────┘
                            │
                   ┌────────▼────────┐
                   │ git_push_sync   │  Step 8: Git commit & push to GitHub
                   └─────────────────┘

 Schedule: Daily at 5:00 PM
 Repo: github.com/Kewin66/discord-ai-heroes-summaries
=============================================================================
"""

from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.models import Variable

import sys
import os

# Add the tasks module to path
sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))

from tasks.discord_api import (
    fetch_guilds,
    find_server,
    fetch_channels,
    fetch_all_messages,
)
from tasks.summarizer import build_channel_summary
from tasks.report_generator import generate_markdown_report
from tasks.file_manager import save_summary_to_file, git_commit_and_push


# ─── DAG Configuration ──────────────────────────────────────────────────────

SERVER_NAME = "Matt's AI heroes"

default_args = {
    "owner": "kewin",
    "depends_on_past": False,
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 2,
    "retry_delay": timedelta(minutes=5),
}


# ─── Task Functions (wrappers for XCom communication) ────────────────────────

def task_fetch_guilds(**context):
    """Step 1: Fetch all Discord guilds."""
    guilds = fetch_guilds()
    # Push to XCom so downstream tasks can use the result
    context["ti"].xcom_push(key="guilds", value=guilds)
    return guilds


def task_find_server(**context):
    """Step 2: Find the target server from the guild list."""
    guilds = context["ti"].xcom_pull(task_ids="fetch_guilds", key="guilds")
    server = find_server(guilds, SERVER_NAME)
    if not server:
        raise ValueError(f"Server '{SERVER_NAME}' not found in {len(guilds)} guilds")
    context["ti"].xcom_push(key="server", value=server)
    return server


def task_fetch_channels(**context):
    """Step 3: Fetch all text channels from the target server."""
    server = context["ti"].xcom_pull(task_ids="find_server", key="server")
    channels = fetch_channels(server["id"])
    context["ti"].xcom_push(key="channels", value=channels)
    return channels


def task_fetch_messages(**context):
    """Step 4: Fetch messages from all text channels (parallel-capable)."""
    channels = context["ti"].xcom_pull(task_ids="fetch_channels", key="channels")
    all_messages = fetch_all_messages(channels, limit_per_channel=100)
    context["ti"].xcom_push(key="all_messages", value=all_messages)
    return all_messages


def task_summarize_channels(**context):
    """Step 5: Parse, filter, and extract AI-relevant content per channel."""
    all_messages = context["ti"].xcom_pull(task_ids="fetch_messages", key="all_messages")

    channel_summaries = []
    for channel_name, messages in all_messages.items():
        summary = build_channel_summary(channel_name, messages)
        # Convert dataclass to dict for XCom serialization
        channel_summaries.append({
            "channel_name": summary.channel_name,
            "messages": [
                {
                    "author": m.author,
                    "content": m.content,
                    "timestamp": m.timestamp,
                    "embed_titles": m.embed_titles,
                    "embed_urls": m.embed_urls,
                }
                for m in summary.messages
            ],
            "date_range": summary.date_range,
            "links": summary.links,
            "tools_mentioned": summary.tools_mentioned,
        })

    context["ti"].xcom_push(key="channel_summaries", value=channel_summaries)
    return channel_summaries


def task_generate_report(**context):
    """Step 6: Generate the final markdown report from all channel summaries."""
    from tasks.summarizer import ChannelSummaryData, ParsedMessage

    raw_summaries = context["ti"].xcom_pull(
        task_ids="summarize_channels", key="channel_summaries"
    )

    # Reconstruct dataclasses from dicts
    channel_summaries = []
    for raw in raw_summaries:
        messages = [
            ParsedMessage(
                author=m["author"],
                content=m["content"],
                timestamp=m["timestamp"],
                embed_titles=m["embed_titles"],
                embed_urls=m["embed_urls"],
            )
            for m in raw["messages"]
        ]
        channel_summaries.append(ChannelSummaryData(
            channel_name=raw["channel_name"],
            messages=messages,
            date_range=tuple(raw["date_range"]),
            links=raw["links"],
            tools_mentioned=raw["tools_mentioned"],
        ))

    report = generate_markdown_report(SERVER_NAME, channel_summaries)
    context["ti"].xcom_push(key="report", value=report)
    return report


def task_save_to_file(**context):
    """Step 7: Save the report to a timestamped markdown file."""
    report = context["ti"].xcom_pull(task_ids="generate_report", key="report")
    filepath = save_summary_to_file(report)
    context["ti"].xcom_push(key="filepath", value=filepath)
    return filepath


def task_git_push(**context):
    """Step 8: Git commit and push the new summary to GitHub."""
    filepath = context["ti"].xcom_pull(task_ids="save_to_file", key="filepath")
    success = git_commit_and_push(filepath, SERVER_NAME)
    if not success:
        raise RuntimeError("Git push failed")
    return success


# ─── DAG Definition ──────────────────────────────────────────────────────────

with DAG(
    dag_id="find_discord_ai_heroes",
    default_args=default_args,
    description="Daily summary of AI topics from Matt's AI Heroes Discord",
    schedule_interval="0 17 * * *",  # Every day at 5:00 PM
    start_date=datetime(2026, 3, 23),
    catchup=False,
    tags=["discord", "ai", "summary"],
) as dag:

    # Step 1: Fetch guilds
    t1_fetch_guilds = PythonOperator(
        task_id="fetch_guilds",
        python_callable=task_fetch_guilds,
    )

    # Step 2: Find server
    t2_find_server = PythonOperator(
        task_id="find_server",
        python_callable=task_find_server,
    )

    # Step 3: Fetch channels
    t3_fetch_channels = PythonOperator(
        task_id="fetch_channels",
        python_callable=task_fetch_channels,
    )

    # Step 4: Fetch messages from all channels
    t4_fetch_messages = PythonOperator(
        task_id="fetch_messages",
        python_callable=task_fetch_messages,
    )

    # Step 5: Summarize (parse + filter + extract)
    t5_summarize = PythonOperator(
        task_id="summarize_channels",
        python_callable=task_summarize_channels,
    )

    # Step 6: Generate report
    t6_generate_report = PythonOperator(
        task_id="generate_report",
        python_callable=task_generate_report,
    )

    # Step 7: Save to file
    t7_save_to_file = PythonOperator(
        task_id="save_to_file",
        python_callable=task_save_to_file,
    )

    # Step 8: Git push
    t8_git_push = PythonOperator(
        task_id="git_push_sync",
        python_callable=task_git_push,
    )

    # ─── DAG Dependencies (the execution order) ─────────────────────────────
    #
    # This defines the Directed Acyclic Graph:
    #
    #   fetch_guilds -> find_server -> fetch_channels -> fetch_messages
    #       -> summarize_channels -> generate_report -> save_to_file -> git_push
    #

    (
        t1_fetch_guilds
        >> t2_find_server
        >> t3_fetch_channels
        >> t4_fetch_messages
        >> t5_summarize
        >> t6_generate_report
        >> t7_save_to_file
        >> t8_git_push
    )
