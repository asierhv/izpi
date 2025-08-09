#!/bin/bash

# Set working directory to project root
cd "$(dirname "$0")/.."

# Log file path (UTC0 date in ISO 8601 format)
LOGFILE="logs/metadata_updater_$(date -u +'%Y-%m-%dT%H:%M:%SZ').log"

# Run the updater and log output
echo "[$(date -u +'%Y-%m-%dT%H:%M:%SZ')] Running metadata_updater.py..." >> "$LOGFILE"
python3 metadata_updater.py >> "$LOGFILE" 2>&1

# Add and commit changes
echo "[$(date -u +'%Y-%m-%dT%H:%M:%SZ')] Pushing changes to GitHub..." >> "$LOGFILE"
git add . >> "$LOGFILE" 2>&1
git commit -m "Auto-update: $(date -u +'%Y-%m-%dT%H:%M:%SZ')" >> "$LOGFILE" 2>&1
git push >> "$LOGFILE" 2>&1

echo "[$(date -u +'%Y-%m-%dT%H:%M:%SZ')]