#!/bin/bash

# Set working directory to project root
cd "$(dirname "$0")/.."

# Activate virtual environment
source ../venvs/venv_izpi/bin/activate

# Git config (only if not already set)
git config --global user.email "asierherranzv@gmail.com"
git config --global user.name "asierhv"

# Log file path (UTC0 date in ISO 8601 format)
LOGFILE="logs/metadata_scrapper_$(date -u +'%Y-%m-%d_%H:%M:%S_UTC').log"

# Run the updater and log output
{
    echo "[$(date -u +'%Y-%m-%d %H:%M:%S UTC')] Running metadata_scrapper.py..."
    python scripts/metadata_scrapper.py
    echo "[$(date -u +'%Y-%m-%d %H:%M:%S UTC')] Pushing changes to GitHub..."
} 2>&1 | tee -a "$LOGFILE"

git add .
git commit -m "Scrapper-update: $(date -u +'%Y-%m-%d %H:%M:%S UTC')"
git push -u origin main

echo "[$(date -u +'%Y-%m-%d %H:%M:%S UTC')] Done."