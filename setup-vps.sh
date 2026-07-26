#!/bin/bash
# ============================================
# Rubaih VPS Deployment Script
# ============================================
# Run this on your VPS after cloning the repo

set -e

echo "🤖 Rubaih VPS Setup"
echo "===================="

# Check if running as root
if [ "$EUID" -ne 0 ]; then 
    echo "⚠️  Please run as root or with sudo"
    exit 1
fi

# Get VPS IP
VPS_IP=$(curl -s ifconfig.me || curl -s icanhazip.com || hostname -I | awk '{print $1}')
echo "📡 Your VPS IP: $VPS_IP"

# Update nginx.conf with actual IP
echo "📝 Updating nginx.conf with your VPS IP..."
sed -i "s/YOUR_VPS_IP_OR_DOMAIN/$VPS_IP/g" nginx.conf

# Install Docker if not present
if ! command -v docker &> /dev/null; then
    echo "🐳 Installing Docker..."
    curl -fsSL https://get.docker.com | sh
    systemctl enable docker
    systemctl start docker
fi

# Install Docker Compose if not present
if ! command -v docker-compose &> /dev/null; then
    echo "🐳 Installing Docker Compose..."
    curl -L "https://github.com/docker/compose/releases/latest/download/docker-compose-$(uname -s)-$(uname -m)" -o /usr/local/bin/docker-compose
    chmod +x /usr/local/bin/docker-compose
fi

# Create .env if not exists
if [ ! -f .env ]; then
    echo "⚙️  Creating .env file..."
    cp .env.example .env
    echo "⚠️  IMPORTANT: Edit .env and add your Delta Exchange API keys!"
    echo "   nano .env"
fi

# Create required directories
mkdir -p logs certbot/conf certbot/www

# Pull and build
echo "🏗️  Building Rubaih..."
docker-compose pull
docker-compose build

# Start services
echo "🚀 Starting Rubaih services..."
docker-compose up -d

# Wait for services
echo "⏳ Waiting for services to start..."
sleep 10

# Check health
echo "🏥 Health check..."
curl -s http://localhost:8000/api/health || echo "⚠️  API not responding yet (may need more time)"

echo ""
echo "✅ Rubaih deployed!"
echo "===================="
echo "API URL:    http://$VPS_IP:8000/api"
echo "WebSocket:  ws://$VPS_IP:8000/ws"
echo "Nginx:      http://$VPS_IP (port 80)"
echo ""
echo "📱 Update your mobile app API_URL to: http://$VPS_IP:8000/api"
echo "📱 Update your mobile app WS_URL to:  ws://$VPS_IP:8000/ws"
echo ""
echo "📋 Useful commands:"
echo "   docker-compose logs -f rubaih_engine    # Watch trading bot"
echo "   docker-compose logs -f rubaih_api       # Watch API"
echo "   docker-compose ps                       # Check status"
echo "   docker-compose down                     # Stop all"
echo "   docker-compose up -d --build            # Rebuild & restart"
