#!/bin/bash
# wx_alert.sh — send email when a systemd service fails.
# Usage: wx_alert.sh <service-name>
# Called via OnFailure= in systemd service units.

SERVICE="${1:-unknown}"
HOST="$(hostname -s)"
SUBJECT="[${HOST}] FAILED: ${SERVICE}"
BODY="$(printf 'Service:  %s\nHost:     %s\nTime:     %s\n\n--- Status ---\n' \
    "${SERVICE}" "${HOST}" "$(date)")
$(systemctl status "${SERVICE}" --no-pager 2>&1)

--- Last 40 journal lines ---
$(journalctl -u "${SERVICE}" -n 40 --no-pager 2>&1)"

echo "$BODY" | mail -s "$SUBJECT" grantwp3@gmail.com
