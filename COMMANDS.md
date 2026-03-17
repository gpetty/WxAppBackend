# Quick Reference — Manual Commands

## Services

| Service | Port | Description |
|---|---|---|
| `wxapi` | 8001 | NBM FastAPI (gunicorn) |
| `wxndfdapi` | 8002 | NDFD FastAPI (gunicorn) |
| `wxblendapi` | 8004 | Blend API (gunicorn) |
| `wxingest` | — | NBM ingest (oneshot) |
| `wxndfdingest` | — | NDFD ingest (oneshot) |

---

## Restart APIs

```bash
# Restart all three APIs
sudo systemctl restart wxapi wxndfdapi wxblendapi

# Restart one
sudo systemctl restart wxapi
sudo systemctl restart wxndfdapi
sudo systemctl restart wxblendapi
```

Hot-reload data store without restarting gunicorn (ingest services do this automatically):
```bash
curl -s -X POST http://127.0.0.1:8001/admin/reload   # NBM
curl -s -X POST http://127.0.0.1:8002/admin/reload   # NDFD
curl -s -X POST http://127.0.0.1:8004/admin/reload   # Blend
```

---

## Run Ingest Manually

Triggers the full ingest pipeline (download + postprocess + API reload), same as the timer:
```bash
sudo systemctl start wxingest        # NBM
sudo systemctl start wxndfdingest    # NDFD
```

Watch progress:
```bash
journalctl -f -u wxingest
journalctl -f -u wxndfdingest
```

Force re-download of the current cycle (bypasses idempotency check):
```bash
cd /home/gpetty/WxApp
DATA_DIR=/12TB2/NBM AWS_NO_SIGN_REQUEST=yes \
  .venv/bin/python -m backend.app.ingest --postprocess --force

NDFD_DATA_DIR=/12TB2/NDFD AWS_NO_SIGN_REQUEST=yes \
  .venv/bin/python -m backend.app.ingest.ndfd_ingest --postprocess --force
```

---

## Status and Logs

```bash
# One-line status for all wx services
systemctl status wxapi wxndfdapi wxblendapi wxingest wxndfdingest

# Recent logs
journalctl -u wxapi -n 50
journalctl -u wxndfdapi -n 50
journalctl -u wxblendapi -n 50

# Timer schedules and last/next fire times
systemctl list-timers wxingest.timer wxndfdingest.timer
```

---

## Deploy Updated systemd Units

Run after editing any `.service` or `.timer` file in `systemd/`:
```bash
sudo cp systemd/wxapi.service        /etc/systemd/system/
sudo cp systemd/wxndfdapi.service    /etc/systemd/system/
sudo cp systemd/wxblendapi.service   /etc/systemd/system/
sudo cp systemd/wxingest.service     /etc/systemd/system/
sudo cp systemd/wxingest.timer       /etc/systemd/system/
sudo cp systemd/wxndfdingest.service /etc/systemd/system/
sudo cp systemd/wxndfdingest.timer   /etc/systemd/system/
sudo systemctl daemon-reload
```
