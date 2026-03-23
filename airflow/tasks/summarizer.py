"""
Task 3: Parse and filter messages, extract AI-relevant content.
This is the "transform" stage of the pipeline.
"""

from dataclasses import dataclass, field
from datetime import datetime


@dataclass
class ParsedMessage:
    """A single parsed Discord message."""
    author: str
    content: str
    timestamp: str
    embed_titles: list[str] = field(default_factory=list)
    embed_urls: list[str] = field(default_factory=list)
    attachment_count: int = 0


@dataclass
class ChannelSummaryData:
    """Aggregated data from a single channel, ready for summarization."""
    channel_name: str
    messages: list[ParsedMessage]
    date_range: tuple[str, str]  # (oldest, newest)
    links: list[dict]            # {title, url}
    tools_mentioned: list[str]


# Keywords that indicate AI/tech relevance
AI_KEYWORDS = [
    "ai", "llm", "gpt", "claude", "model", "agent", "prompt", "eval",
    "machine learning", "deep learning", "neural", "transformer", "embedding",
    "rag", "retrieval", "vector", "fine-tune", "finetune", "training",
    "inference", "token", "context window", "mcp", "sdk", "api",
    "openai", "anthropic", "gemini", "mistral", "ollama", "hugging",
    "langchain", "llamaindex", "autogen", "crew", "workflow", "pipeline",
    "tool", "plugin", "framework", "library", "typescript", "python",
    "react", "next", "node", "docker", "sandbox", "skill", "ralph",
    "cohort", "course", "tutorial", "learn",
]


def parse_messages(raw_messages: list[dict]) -> list[ParsedMessage]:
    """
    DAG Step 5a: Parse raw Discord API messages into structured format.

    Args:
        raw_messages: Raw message dicts from Discord API

    Returns:
        List of ParsedMessage objects
    """
    parsed = []
    for msg in raw_messages:
        embed_titles = []
        embed_urls = []
        for embed in msg.get("embeds", []):
            if embed.get("title"):
                embed_titles.append(embed["title"])
            if embed.get("url"):
                embed_urls.append(embed["url"])

        parsed.append(ParsedMessage(
            author=msg["author"]["username"],
            content=msg.get("content", ""),
            timestamp=msg["timestamp"][:10],
            embed_titles=embed_titles,
            embed_urls=embed_urls,
            attachment_count=len(msg.get("attachments", [])),
        ))
    return parsed


def is_ai_relevant(message: ParsedMessage) -> bool:
    """
    DAG Step 5b: Filter — check if a message is AI/tech relevant.

    Uses keyword matching against message content and embed titles.
    Skips very short messages (likely reactions/greetings).
    """
    text = (message.content + " " + " ".join(message.embed_titles)).lower()
    if len(text.strip()) < 15:
        return False
    return any(kw in text for kw in AI_KEYWORDS)


def extract_links(messages: list[ParsedMessage]) -> list[dict]:
    """
    DAG Step 5c: Extract all shared links from messages.

    Returns:
        List of {title, url} dicts
    """
    links = []
    seen_urls = set()
    for msg in messages:
        for i, url in enumerate(msg.embed_urls):
            if url not in seen_urls:
                seen_urls.add(url)
                title = msg.embed_titles[i] if i < len(msg.embed_titles) else url
                links.append({"title": title, "url": url})
    return links


def extract_tools(messages: list[ParsedMessage]) -> list[str]:
    """
    DAG Step 5d: Extract mentioned tools/libraries/frameworks.

    Scans for known tool names in message content.
    """
    known_tools = [
        "Evalite", "Wispr Flow", "Aqua Voice", "VoiceInk", "Manim",
        "chrome-devtools-mcp", "Figma MCP", "Excalidraw", "TLDraw",
        "pickai", "TScanner", "Valora", "Satori", "AgentKit",
        "DSPy", "Workflow DevKit", "OpenRouter", "Playwright",
        "claude-plugins", "Beads", "Sandcastle", "Ralph",
    ]
    found = set()
    for msg in messages:
        text = msg.content + " " + " ".join(msg.embed_titles)
        for tool in known_tools:
            if tool.lower() in text.lower():
                found.add(tool)
    return sorted(found)


def build_channel_summary(channel_name: str, raw_messages: list[dict]) -> ChannelSummaryData:
    """
    DAG Step 5 (combined): Full transform pipeline for one channel.

    Pipeline: raw messages -> parse -> filter -> extract links/tools

    Args:
        channel_name: Name of the Discord channel
        raw_messages: Raw messages from the Discord API

    Returns:
        ChannelSummaryData ready for the summarization step
    """
    # Parse
    all_parsed = parse_messages(raw_messages)

    # Filter to AI-relevant only
    relevant = [m for m in all_parsed if is_ai_relevant(m)]

    # Date range
    if all_parsed:
        dates = sorted(m.timestamp for m in all_parsed)
        date_range = (dates[0], dates[-1])
    else:
        date_range = ("N/A", "N/A")

    # Extract
    links = extract_links(relevant)
    tools = extract_tools(relevant)

    print(f"[build_channel_summary] #{channel_name}: "
          f"{len(all_parsed)} total -> {len(relevant)} relevant, "
          f"{len(links)} links, {len(tools)} tools")

    return ChannelSummaryData(
        channel_name=channel_name,
        messages=relevant,
        date_range=date_range,
        links=links,
        tools_mentioned=tools,
    )
