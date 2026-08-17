#!/bin/bash
set -e

echo "=========================================="
echo " Installing Python Restreamer Lite"
echo "=========================================="

echo "[1/4] Installing system dependencies..."
sudo apt update
sudo apt install -y python3 python3-pip python3-venv ffmpeg

echo "[2/4] Setting up Python virtual environment..."
cd "$(dirname "$0")"
python3 -m venv venv
source venv/bin/activate
pip install -r requirements.txt

echo "[2.5/4] Setting up Configuration..."
if [ ! -f .env ]; then
    cp .env.example .env
    echo "Generated .env file with default settings."
fi

# Load port from .env
APP_PORT=$(grep '^APP_PORT=' .env | cut -d '=' -f2 || echo 8000)
if [ -z "$APP_PORT" ]; then APP_PORT=8000; fi

echo "[3/4] Creating Systemd service (Port: $APP_PORT)..."
SERVICE_FILE="/etc/systemd/system/python-restreamer.service"
APP_DIR=$(pwd)
CURRENT_USER=$(whoami)

sudo bash -c "cat > $SERVICE_FILE" << EOF
[Unit]
Description=Python Restreamer Lite
After=network.target

[Service]
User=$CURRENT_USER
WorkingDirectory=$APP_DIR
Environment="PATH=$APP_DIR/venv/bin"
ExecStart=$APP_DIR/venv/bin/uvicorn main:app --host 0.0.0.0 --port $APP_PORT
Restart=always

[Install]
WantedBy=multi-user.target
EOF

sudo systemctl daemon-reload
sudo systemctl enable python-restreamer.service
sudo systemctl start python-restreamer.service

echo "[4/4] Installation Complete!"
echo "=========================================="
echo "Restreamer Lite is now configured for port $APP_PORT."
echo "Access it at: http://<your-server-ip>:$APP_PORT"
echo "To check logs: sudo journalctl -u python-restreamer -f"
echo "To stop: sudo systemctl stop python-restreamer"
echo "To start: sudo systemctl start python-restreamer"
