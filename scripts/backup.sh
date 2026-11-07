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
# Docker note: this calls `docker` directly, so run it as root (e.g. a root cron
# entry) or with sudo — your `trader` user is deliberately NOT in the docker group.
#
# Usage (e.g. from cron, once a day):
#   BACKUP_REPO="$HOME/my-private-backup" ./backup.sh
#
set -euo pipefail

BACKUP_REPO="${BACKUP_REPO:?Set BACKUP_REPO to your own private backup repo path}"
# Container names (override if yours differ — check with `docker ps`):
N8N_CONTAINER="${N8N_CONTAINER:-n8n}"
PG_CONTAINER="${PG_CONTAINER:-market-postgres}"

cd "$BACKUP_REPO"
mkdir -p n8n

# 1. Export all n8n workflows to JSON. n8n stores credentials separately, so the
#    export should not contain them — but scan it before trusting that.
docker exec "$N8N_CONTAINER" n8n export:workflow --all --output=/tmp/workflows.json
docker cp "$N8N_CONTAINER":/tmp/workflows.json n8n/workflows.json

# 2. Dump the market_data database.
docker exec "$PG_CONTAINER" pg_dump -U screener market_data > market_data.sql

# 3. Commit and push. Make sure .env and any secret files are gitignored HERE.
git add -A
git commit -m "backup $(date -u +%Y-%m-%dT%H:%MZ)" || echo "nothing new to back up"
git push
