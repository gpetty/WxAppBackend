#!/bin/bash
# Apply memory limits to wxapi and wxingest systemd services.
# Run with: sudo bash scripts/apply_service_limits.sh

set -e

cat > /etc/systemd/system/wxapi.service << 'EOF'
[Unit]
Description=Weather Window FastAPI
After=network.target

[Service]
User=gpetty
WorkingDirectory=/home/gpetty/WxApp
Environment=DATA_DIR=/12TB2/NBM
ExecStart=/home/gpetty/WxApp/.venv/bin/gunicorn backend.app.main:app \
    -k uvicorn.workers.UvicornWorker \
    -w 1 \
    --bind 127.0.0.1:8001 \
    --root-path /wxapp \
    --timeout 30 \
    --worker-tmp-dir /dev/shm
Restart=always
RestartSec=5
StartLimitIntervalSec=300
StartLimitBurst=5
MemoryHigh=70G
MemoryMax=85G
MemorySwapMax=0

[Install]
WantedBy=multi-user.target
EOF

cat > /etc/systemd/system/wxingest.service << 'EOF'
# /etc/systemd/system/wxingest.service
[Unit]
Description=Weather Window NBM Ingestion
After=network.target

[Service]
Type=oneshot
User=gpetty
WorkingDirectory=/home/gpetty/WxApp
Environment=DATA_DIR=/12TB2/NBM
Environment=AWS_NO_SIGN_REQUEST=yes
ExecStart=/home/gpetty/WxApp/.venv/bin/python -m backend.app.ingest --postprocess
ExecStartPost=-/usr/bin/curl -s --max-time 30 -X POST http://127.0.0.1:8001/admin/reload
StandardOutput=journal
StandardError=journal
MemoryHigh=20G
MemoryMax=25G
MemorySwapMax=0
EOF

systemctl daemon-reload
systemctl restart wxapi.service

echo "Done. Verifying limits:"
systemctl show wxapi.service | grep -E "MemoryHigh|MemoryMax|MemorySwapMax"
systemctl show wxingest.service | grep -E "MemoryHigh|MemoryMax|MemorySwapMax"
