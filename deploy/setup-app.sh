#!/usr/bin/env bash
# First-time setup for VM1 (App server: 195.47.196.46)
# Run: ssh root@195.47.196.46 'bash -s' < deploy/setup-app.sh

set -euo pipefail

echo "=== Setting up App server ==="

# Install Docker if not present
if ! command -v docker &> /dev/null; then
    echo "--- Installing Docker ---"
    curl -fsSL https://get.docker.com | sh
fi

# Install Git if not present
if ! command -v git &> /dev/null; then
    echo "--- Installing Git ---"
    apt-get update && apt-get install -y git
fi

# Clone repo
APP_DIR="/opt/edunabazar"
REPO="https://github.com/RuKi81/edunabazar.git"

if [ ! -d "$APP_DIR/.git" ]; then
    echo "--- Cloning repository ---"
    git clone "$REPO" "$APP_DIR"
else
    echo "--- Repository already exists ---"
fi

cd "$APP_DIR"

# Create .env
if [ ! -f .env ]; then
    cp .env.example .env
    echo "IMPORTANT: Edit $APP_DIR/.env and set all production values!"
fi

echo ""
echo "=== Next steps ==="
echo "1. Edit $APP_DIR/.env:"
echo "   - DJANGO_SECRET_KEY  (generate a random 50+ char string)"
echo "   - DJANGO_DEBUG=0"
echo "   - DB_HOST=93.95.98.209"
echo "   - DB_USER=enb_app"
echo "   - DB_PASSWORD=<same as on DB server>"
echo "   - DJANGO_ALLOWED_HOSTS=195.47.196.46 your-domain.ru"
echo ""
echo "2. Start the app:"
echo "   cd $APP_DIR"
echo "   docker compose -f deploy/app/docker-compose.yml up -d"
echo ""
echo "3. Run initial migrations:"
echo "   docker compose -f deploy/app/docker-compose.yml exec web python manage.py migrate"
echo ""
