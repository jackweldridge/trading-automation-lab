#!/usr/bin/env bash
#
# Backup template (Module 8).
#
# Exports your n8n workflows and dumps the database, then commits and pushes them
# to a PRIVATE git repo OF YOUR OWN.
#
#   >>> Point BACKUP_REPO at your own private repo — NEVER the public course repo. <<<
#   Workflow exports and database dumps are yours and must stay private.
#
# Usage (e.g. from cron, once a day):
#   BACKUP_REPO="$HOME/my-private-backup" ./backup.sh
#
set -euo pipefail

BACKUP_REPO="${BACKUP_REPO:?Set BACKUP_REPO to your own private backup repo path}"
cd "$BACKUP_REPO"
mkdir -p n8n

# 1. Export all n8n workflows to JSON. n8n stores credentials separately, so the
#    export should not contain them — but scan it before trusting that.
docker exec n8n-n8n-1 n8n export:workflow --all --output=/tmp/workflows.json
docker cp n8n-n8n-1:/tmp/workflows.json n8n/workflows.json

# 2. Dump the market_data database.
docker exec market-postgres pg_dump -U screener market_data > market_data.sql

# 3. Commit and push. Make sure .env and any secret files are gitignored HERE.
git add -A
git commit -m "backup $(date -u +%Y-%m-%dT%H:%MZ)" || echo "nothing new to back up"
git push
