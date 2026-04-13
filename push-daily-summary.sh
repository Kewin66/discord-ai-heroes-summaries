#!/bin/bash
# Push one daily summary per day, starting Apr 12 -> Mar 20 summary
# Each subsequent day pushes the next date's summary

export PATH="/opt/homebrew/bin:/usr/local/bin:/usr/bin:/bin:$PATH"
cd ~/projects/Discord/AI-Heroes

# Base date: Apr 12, 2026 = day 0 -> pushes 2026-03-20.md
# Apr 13 = day 1 -> pushes 2026-03-21.md, etc.
BASE_EPOCH=$(date -j -f "%Y-%m-%d" "2026-04-12" "+%s")
TODAY_EPOCH=$(date "+%s")
DAYS_DIFF=$(( (TODAY_EPOCH - BASE_EPOCH) / 86400 ))

# Calculate which summary date to push (Mar 20 + DAYS_DIFF)
SUMMARY_EPOCH=$(date -j -f "%Y-%m-%d" "2026-03-20" "+%s")
TARGET_EPOCH=$(( SUMMARY_EPOCH + (DAYS_DIFF * 86400) ))
TARGET_DATE=$(date -j -f "%s" "$TARGET_EPOCH" "+%Y-%m-%d")

FILE="daily/${TARGET_DATE}.md"

if [ ! -f "$FILE" ]; then
    echo "$(date): No summary for $TARGET_DATE, all done!" >> logs/push-daily.log
    exit 0
fi

echo "$(date): Pushing summary for $TARGET_DATE" >> logs/push-daily.log

git add "$FILE"
git commit -m "Add daily summary: $TARGET_DATE (Cohort 003)"
git push origin main

echo "$(date): Pushed $FILE successfully" >> logs/push-daily.log
