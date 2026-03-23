"""
Task 1 & 2: Discord API interactions
- Fetch all guilds the bot belongs to
- Find the target server and its channels
- Fetch messages from text channels
"""

import os
import requests
from typing import Optional


DISCORD_API = "https://discord.com/api/v10"


def get_headers() -> dict:
    """Build auth headers from DISCORD_TOKEN env var."""
    token = os.environ.get("DISCORD_TOKEN")
    if not token:
        raise ValueError("DISCORD_TOKEN environment variable is not set")
    return {"Authorization": token}


def fetch_guilds() -> list[dict]:
    """
    DAG Step 1: Fetch all Discord servers (guilds) the bot is a member of.

    Returns:
        List of guild objects with 'id' and 'name' fields.

    API: GET /users/@me/guilds
    """
    response = requests.get(f"{DISCORD_API}/users/@me/guilds", headers=get_headers())
    response.raise_for_status()
    guilds = response.json()
    print(f"[fetch_guilds] Found {len(guilds)} guilds:")
    for g in guilds:
        print(f"  - {g['name']} (ID: {g['id']})")
    return guilds


def find_server(guilds: list[dict], server_name: str) -> Optional[dict]:
    """
    DAG Step 2: Find the target server by name (case-insensitive partial match).

    Args:
        guilds: List of guild objects from fetch_guilds()
        server_name: Name to search for (e.g., "Matt's AI heroes")

    Returns:
        Matching guild dict, or None if not found.
    """
    for guild in guilds:
        if server_name.lower() in guild["name"].lower():
            print(f"[find_server] Matched: {guild['name']} (ID: {guild['id']})")
            return guild
    print(f"[find_server] No server matching '{server_name}' found")
    return None


def fetch_channels(guild_id: str) -> list[dict]:
    """
    DAG Step 3: Fetch all channels in a guild.

    Args:
        guild_id: Discord guild ID

    Returns:
        List of channel objects. Text channels have type=0.

    API: GET /guilds/{guild_id}/channels
    """
    response = requests.get(
        f"{DISCORD_API}/guilds/{guild_id}/channels", headers=get_headers()
    )
    response.raise_for_status()
    channels = response.json()

    # Channel type mapping
    type_names = {0: "text", 2: "voice", 4: "category", 5: "announcement", 15: "forum"}
    text_channels = [ch for ch in channels if ch["type"] in (0, 5)]

    print(f"[fetch_channels] Found {len(channels)} total channels, {len(text_channels)} text/announcement:")
    for ch in text_channels:
        print(f"  - #{ch['name']} (ID: {ch['id']}, type: {type_names.get(ch['type'], ch['type'])})")

    return text_channels


def fetch_messages(channel_id: str, limit: int = 100) -> list[dict]:
    """
    DAG Step 4: Fetch recent messages from a single channel.

    Args:
        channel_id: Discord channel ID
        limit: Max messages to fetch (1-100)

    Returns:
        List of message objects with author, content, timestamp, embeds.

    API: GET /channels/{channel_id}/messages?limit={limit}
    """
    response = requests.get(
        f"{DISCORD_API}/channels/{channel_id}/messages",
        headers=get_headers(),
        params={"limit": limit},
    )
    response.raise_for_status()
    messages = response.json()
    print(f"[fetch_messages] Fetched {len(messages)} messages from channel {channel_id}")
    return messages


def fetch_all_messages(channels: list[dict], limit_per_channel: int = 100) -> dict[str, list[dict]]:
    """
    DAG Step 4 (parallel): Fetch messages from ALL text channels.

    Args:
        channels: List of text channel objects
        limit_per_channel: Max messages per channel

    Returns:
        Dict mapping channel_name -> list of messages

    This step runs in parallel in the DAG (one task per channel).
    """
    all_messages = {}
    total = 0
    for ch in channels:
        try:
            msgs = fetch_messages(ch["id"], limit_per_channel)
            if msgs:
                all_messages[ch["name"]] = msgs
                total += len(msgs)
        except requests.HTTPError as e:
            print(f"[fetch_all_messages] Skipping #{ch['name']}: {e}")
            continue

    print(f"[fetch_all_messages] Total: {total} messages from {len(all_messages)} channels")
    return all_messages
