#!/bin/bash
# Daily Discord AI-Heroes Summary Script
# Runs claude CLI with the findDiscord skill

export PATH="/opt/homebrew/bin:/usr/local/bin:/usr/bin:/bin:$PATH"

cd ~/projects/Discord/AI-Heroes

# Load token from .env file
if [ -f .env ]; then
    export $(grep -v '^#' .env | xargs)
fi

# Run claude with the findDiscord skill
claude --print "/findDiscord Matt's AI heroes" 2>/dev/null

# The skill handles saving and git push, but as a fallback:
if [ -n "$(git status --porcelain)" ]; then
    git add .
    git commit -m "Add summary - $(date +%Y-%m-%d-%H%M%S)"
    git push origin main
fi
