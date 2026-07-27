#!/bin/bash
# ============================================
# Rubaih VPS Production Setup
# ============================================
set -euo pipefail

echo "Rubaih VPS Setup (CoinDCX production)"
echo "======================================"

if [ "${EUID:-$(id -u)}" -ne 0 ]; then
    echo "Please run as root or with sudo"
    exit 1
fi

VPS_IP=$(curl -fsS ifconfig.me || curl -fsS icanhazip.com || hostname -I | awk '{print $1}')
echo "VPS IP: $VPS_IP"

echo "Updating nginx.conf server_name..."
sed -i "s/YOUR_VPS_IP_OR_DOMAIN/$VPS_IP/g" nginx.conf

if [ ! -f .env ]; then
    echo "Creating .env from .env.example..."
    cp .env.example .env
    TOKEN=$(openssl rand -hex 32)
    DBPASS=$(openssl rand -hex 16)
    sed -i "s/^DB_PASSWORD=.*/DB_PASSWORD=$DBPASS/" .env
    sed -i "s/^RUBAIH_API_TOKEN=.*/RUBAIH_API_TOKEN=$TOKEN/" .env
    echo ""
    echo "Generated DB_PASSWORD and RUBAIH_API_TOKEN in .env"
    echo "NOW edit .env and set:"
    echo "  COINDCX_API_KEY / COINDCX_API_SECRET"
    echo "  LIVE_TRADING=true   (only when ready for real orders)"
    echo "  OPENROUTER_API_KEY  (optional)"
    echo ""
    echo "Then re-run: sudo bash setup-vps.sh"
    echo "Token for mobile/config.js:"
    grep '^RUBAIH_API_TOKEN=' .env
    exit 0
fi

# Validate required secrets
set -a
# shellcheck disable=SC1091
source .env
set +a

if [ -z "${COINDCX_API_KEY:-}" ] || [ -z "${COINDCX_API_SECRET:-}" ]; then
    echo "ERROR: Set COINDCX_API_KEY and COINDCX_API_SECRET in .env"
    exit 1
fi
if [ -z "${RUBAIH_API_TOKEN:-}" ] || [ "${#RUBAIH_API_TOKEN}" -lt 16 ]; then
    echo "ERROR: RUBAIH_API_TOKEN must be set (>=16 chars)"
    exit 1
fi
if [ -z "${DB_PASSWORD:-}" ]; then
    echo "ERROR: DB_PASSWORD must be set"
    exit 1
fi

# Patch mobile config for APK builds on this machine / CI checkout
if [ -f mobile/config.js ]; then
    echo "Patching mobile/config.js with VPS IP + API token..."
    python3 - <<PY
from pathlib import Path
p = Path("mobile/config.js")
text = p.read_text()
text = text.replace("http://YOUR_VPS_IP", "http://${VPS_IP}")
text = text.replace("YOUR_RUBAIH_API_TOKEN", """${RUBAIH_API_TOKEN}""")
p.write_text(text)
print("mobile/config.js updated")
PY
fi

if ! command -v docker >/dev/null 2>&1; then
    echo "Installing Docker..."
    curl -fsSL https://get.docker.com | sh
    systemctl enable docker
    systemctl start docker
fi

if ! command -v docker-compose >/dev/null 2>&1 && ! docker compose version >/dev/null 2>&1; then
    echo "Installing Docker Compose plugin fallback..."
    curl -fsSL "https://github.com/docker/compose/releases/latest/download/docker-compose-$(uname -s)-$(uname -m)" \
      -o /usr/local/bin/docker-compose
    chmod +x /usr/local/bin/docker-compose
fi

COMPOSE="docker compose"
if ! docker compose version >/dev/null 2>&1; then
    COMPOSE="docker-compose"
fi

mkdir -p logs

echo "Building and starting services..."
$COMPOSE build
$COMPOSE up -d

echo "Waiting for API health..."
ok=0
for i in $(seq 1 30); do
    if curl -fsS "http://127.0.0.1:8010/api/health" >/dev/null 2>&1; then
        ok=1
        break
    fi
    sleep 2
done

echo ""
if [ "$ok" -eq 1 ]; then
    echo "API healthy"
else
    echo "API not healthy yet — check: $COMPOSE logs -f rubaih_api"
fi

echo ""
echo "Deployed"
echo "========"
echo "Public API:  http://$VPS_IP/api"
echo "WebSocket:   ws://$VPS_IP/ws?token=***"
echo "Local API:   http://127.0.0.1:8010/api"
echo ""
echo "LIVE_TRADING=${LIVE_TRADING:-false}"
if [ "${LIVE_TRADING:-false}" != "true" ]; then
    echo "DRY-RUN mode — no real CoinDCX orders until LIVE_TRADING=true"
fi
echo ""
echo "Mobile: Settings → Edit Connection → VPS IP ($VPS_IP) + RUBAIH_API_TOKEN"
echo ""
echo "Useful:"
echo "  $COMPOSE logs -f rubaih_engine"
echo "  $COMPOSE logs -f rubaih_api"
echo "  $COMPOSE ps"
