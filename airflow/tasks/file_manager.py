"""
Task 5: Save report to file and sync with git.
This is the final "persist" stage of the pipeline.
"""

import os
import subprocess
from datetime import datetime
from pathlib import Path


OUTPUT_DIR = Path.home() / "projects" / "Discord" / "AI-Heroes"


def save_summary_to_file(report_content: str) -> str:
    """
    DAG Step 7: Save the markdown report to a timestamped file.

    Filename format: summary-YYYY-MM-DD-HHMMSS.md

    Args:
        report_content: The full markdown report string

    Returns:
        Absolute path to the saved file
    """
    OUTPUT_DIR.mkdir(parents=True, exist_ok=True)

    timestamp = datetime.now().strftime("%Y-%m-%d-%H%M%S")
    filename = f"summary-{timestamp}.md"
    filepath = OUTPUT_DIR / filename

    filepath.write_text(report_content, encoding="utf-8")
    print(f"[save_summary_to_file] Saved to {filepath}")
    return str(filepath)


def git_commit_and_push(filepath: str, server_name: str) -> bool:
    """
    DAG Step 8: Git add, commit, and push the new summary file.

    Args:
        filepath: Path to the file to commit
        server_name: Server name for the commit message

    Returns:
        True if push succeeded, False otherwise
    """
    today = datetime.now().strftime("%Y-%m-%d")
    commit_msg = f"Add summary for {server_name} - {today}"

    commands = [
        ["git", "add", "."],
        ["git", "commit", "-m", commit_msg],
        ["git", "push", "origin", "main"],
    ]

    for cmd in commands:
        print(f"[git_commit_and_push] Running: {' '.join(cmd)}")
        result = subprocess.run(
            cmd,
            cwd=str(OUTPUT_DIR),
            capture_output=True,
            text=True,
        )
        if result.returncode != 0:
            print(f"[git_commit_and_push] Error: {result.stderr}")
            # Don't fail on "nothing to commit"
            if "nothing to commit" in result.stdout + result.stderr:
                print("[git_commit_and_push] Nothing to commit, skipping")
                return True
            return False
        print(f"[git_commit_and_push] Output: {result.stdout.strip()}")

    print("[git_commit_and_push] Successfully pushed to remote")
    return True
