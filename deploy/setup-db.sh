#!/usr/bin/env bash
# First-time setup for VM2 (Database server: 93.95.98.209)
# Run: ssh root@93.95.98.209 'bash -s' < deploy/setup-db.sh

set -euo pipefail

echo "=== Setting up DB server ==="

# Install Docker if not present
if ! command -v docker &> /dev/null; then
    echo "--- Installing Docker ---"
    curl -fsSL https://get.docker.com | sh
fi

# Create directory
mkdir -p /opt/edunabazar-db
cd /opt/edunabazar-db

echo "--- Creating .env ---"
if [ ! -f .env ]; then
    cat > .env <<'EOF'
DB_NAME=enb_DB
DB_USER=enb_app
DB_PASSWORD=CHANGE_ME_strong_password_here
EOF
    echo "IMPORTANT: Edit /opt/edunabazar-db/.env and set a strong DB_PASSWORD!"
fi

echo "--- Configuring firewall (allow PostgreSQL only from app server) ---"
if command -v ufw &> /dev/null; then
    ufw allow from 195.47.196.46 to any port 5432 proto tcp comment 'PostgreSQL from app VM'
    ufw deny 5432
    echo "UFW rules added."
fi

echo ""
echo "=== Next steps ==="
echo "1. Edit /opt/edunabazar-db/.env — set DB_PASSWORD"
echo "2. Copy docker-compose.yml and pg_hba_custom.conf to /opt/edunabazar-db/"
echo "3. Run: cd /opt/edunabazar-db && docker compose up -d"
echo ""
