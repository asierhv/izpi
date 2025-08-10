#!/bin/bash

# Set working directory to project root
cd "$(dirname "$0")/.."

# Activate virtual environment
if [ -d "venv" ]; then
    source venv/bin/activate
else
    echo "Virtual environment not found. Creating one..."
    python3 -m venv venv
    source venv/bin/activate
    pip install --upgrade pip
    if [ -f "requirements.txt" ]; then
        pip install -r requirements.txt
    fi
fi

# Git config (only if not already set)
git config --global user.email "asierherranzv@gmail.com"
git config --global user.name "asierhv"

# Log file path (UTC0 date in ISO 8601 format)
LOGFILE="logs/metadata_updater_$(date -u +'%Y-%m-%dT%H:%M:%SZ').log"

# Run the updater and log output
{
    echo "[$(date -u +'%Y-%m-%dT%H:%M:%SZ')] Running metadata_updater.py..."
    python scripts/metadata_updater.py

    echo "[$(date -u +'%Y-%m-%dT%H:%M:%SZ')] Pushing changes to GitHub..."
    git add .
    git commit -m "Auto-update: $(date -u +'%Y-%m-%dT%H:%M:%SZ')"
    git push
} >> "$LOGFILE" 2>&1

echo "[$(date -u +'%Y-%m-%dT%H:%M:%SZ')] Done."
