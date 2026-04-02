#!/usr/bin/env bash
# Deploy script for edunabazar (Docker Compose on server)
# Usage: ssh user@server 'bash -s' < deploy/deploy.sh
#   or:  copy project to server, then run: bash deploy/deploy.sh

set -euo pipefail

APP_DIR="${APP_DIR:-/opt/edunabazar}"
BRANCH="${BRANCH:-main}"
REPO="${REPO:-https://github.com/RuKi81/edunabazar.git}"
COMPOSE_FILE="docker-compose.prod.yml"

echo "=== Deploy started at $(date) ==="

# Clone or pull
if [ -d "$APP_DIR/.git" ]; then
    echo "--- Pulling latest code ---"
    cd "$APP_DIR"
    git fetch origin
    git reset --hard "origin/$BRANCH"
else
    echo "--- Cloning repository ---"
    git clone -b "$BRANCH" "$REPO" "$APP_DIR"
    cd "$APP_DIR"
fi

# Ensure .env exists
if [ ! -f "$APP_DIR/.env" ]; then
    echo "ERROR: .env file not found at $APP_DIR/.env"
    echo "Copy .env.example to .env and fill in production values:"
    echo "  cp .env.example .env && nano .env"
    exit 1
fi

# Build and restart
echo "--- Building containers ---"
docker compose -f "$COMPOSE_FILE" build --no-cache web

echo "--- Starting services ---"
docker compose -f "$COMPOSE_FILE" up -d

echo "--- Running migrations ---"
docker compose -f "$COMPOSE_FILE" exec web python manage.py migrate --noinput

echo "--- Collecting static files ---"
docker compose -f "$COMPOSE_FILE" exec web python manage.py collectstatic --noinput

echo "--- Cleaning up old images ---"
docker image prune -f

# Setup cron for daily news fetch at 07:00 Moscow time
CRON_CMD='CRON_TZ=Europe/Moscow
0 7 * * * cd /opt/edunabazar && docker compose -f docker-compose.prod.yml exec -T web python manage.py fetch_news --count 3 >> /var/log/fetch_news.log 2>&1'
( crontab -l 2>/dev/null | grep -v 'fetch_news' | grep -v 'CRON_TZ' ; echo "$CRON_CMD" ) | crontab -
echo "--- Cron job for fetch_news installed ---"

echo "=== Deploy finished at $(date) ==="
echo "--- Service status ---"
docker compose -f "$COMPOSE_FILE" ps
