"""
Task 4: Generate the final markdown summary report.
This is the "load" stage — transforms structured data into a readable report.
"""

from datetime import datetime

from .summarizer import ChannelSummaryData, ParsedMessage


def generate_markdown_report(
    server_name: str,
    channel_summaries: list[ChannelSummaryData],
) -> str:
    """
    DAG Step 6: Generate the final markdown summary report.

    Combines data from all channels into a single cohesive report.

    Args:
        server_name: Name of the Discord server
        channel_summaries: List of per-channel summary data

    Returns:
        Complete markdown string ready to write to file
    """
    # Aggregate stats
    total_messages = sum(len(cs.messages) for cs in channel_summaries)
    all_dates = []
    for cs in channel_summaries:
        if cs.date_range[0] != "N/A":
            all_dates.extend(cs.date_range)
    date_range = (min(all_dates), max(all_dates)) if all_dates else ("N/A", "N/A")

    # Aggregate tools and links across channels
    all_tools = set()
    all_links = []
    seen_urls = set()
    for cs in channel_summaries:
        all_tools.update(cs.tools_mentioned)
        for link in cs.links:
            if link["url"] not in seen_urls:
                seen_urls.add(link["url"])
                all_links.append(link)

    # Build report
    now = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    lines = [
        f"# Discord Summary: {server_name}",
        f"**Generated: {now}**",
        f"**Messages analyzed: {total_messages}** (AI-relevant) across {len(channel_summaries)} channels",
        f"**Date range: {date_range[0]} to {date_range[1]}**",
        "",
        "---",
        "",
    ]

    # Key topics by channel
    lines.append("## Key AI Topics Discussed")
    lines.append("")
    for cs in channel_summaries:
        if not cs.messages:
            continue
        lines.append(f"### #{cs.channel_name}")
        lines.append("")
        # Group messages by rough topic (use first 80 chars as preview)
        for msg in cs.messages[:15]:  # top 15 most relevant
            preview = msg.content[:120].replace("\n", " ")
            if preview:
                lines.append(f"- **{msg.author}** ({msg.timestamp}): {preview}")
        lines.append("")

    # Tools mentioned
    if all_tools:
        lines.append("## Tools / Libraries / Frameworks Mentioned")
        lines.append("")
        for tool in sorted(all_tools):
            lines.append(f"- {tool}")
        lines.append("")

    # Links shared
    if all_links:
        lines.append("## Useful Links Shared")
        lines.append("")
        for link in all_links:
            lines.append(f"- [{link['title']}]({link['url']})")
        lines.append("")

    # Key takeaways placeholder
    lines.append("## Key Takeaways")
    lines.append("")
    lines.append("- Review the topics above and explore linked resources")
    lines.append(f"- {total_messages} AI-relevant messages found across {len(channel_summaries)} channels")
    if all_tools:
        lines.append(f"- Notable tools to explore: {', '.join(sorted(all_tools)[:5])}")
    lines.append("")

    report = "\n".join(lines)
    print(f"[generate_markdown_report] Generated report: {len(lines)} lines, {len(report)} chars")
    return report
