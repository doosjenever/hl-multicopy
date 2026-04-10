#!/bin/bash
# Fallback watchdog — runs every 2 minutes via cron.
# Pure bash, no Python, no AI dependency.
# Last-resort safety net: if total equity drops too far, kill everything.

set -euo pipefail

# Config
STATE_DIR="/data/.openclaw/workspace/hl-multicopy/state"
KILL_THRESHOLD_PCT=25   # Kill all if aggregate DD exceeds this % (buffer above risk_checker's 20%)
TELEGRAM_BOT_TOKEN="${TELEGRAM_BOT_TOKEN:-}"
TELEGRAM_CHAT_ID="${TELEGRAM_CHAT_ID:-}"

send_alert() {
    local msg="$1"
    echo "[WATCHDOG] $msg"
    if [ -n "$TELEGRAM_BOT_TOKEN" ] && [ -n "$TELEGRAM_CHAT_ID" ]; then
        curl -s -X POST "https://api.telegram.org/bot${TELEGRAM_BOT_TOKEN}/sendMessage" \
            -d chat_id="$TELEGRAM_CHAT_ID" \
            -d text="[WATCHDOG] $msg" \
            -d parse_mode="HTML" > /dev/null 2>&1 || true
    fi
}

kill_all() {
    send_alert "<b>EMERGENCY KILL</b>: Stopping all multicopy services!"
    systemctl stop 'hl-multicopy@*' 2>/dev/null || true
    # List and stop individually as fallback
    for unit in $(systemctl list-units --type=service --state=running --no-legend | grep 'hl-multicopy@' | awk '{print $1}'); do
        systemctl stop "$unit" 2>/dev/null || true
        send_alert "Stopped $unit"
    done
}

# Check if state directory exists
if [ ! -d "$STATE_DIR" ]; then
    echo "State dir not found: $STATE_DIR"
    exit 0
fi

# Read all state files and sum equity
total_equity=0
total_original=0
count=0

for state_file in "$STATE_DIR"/*.json; do
    [ -f "$state_file" ] || continue

    # Parse JSON with jq (lightweight, no Python needed)
    equity=$(jq -r '.equity // 0' "$state_file" 2>/dev/null || echo 0)
    original=$(jq -r '.original_start_equity // 0' "$state_file" 2>/dev/null || echo 0)
    name=$(jq -r '.target // "unknown"' "$state_file" 2>/dev/null || echo "unknown")
    timestamp=$(jq -r '.timestamp // 0' "$state_file" 2>/dev/null || echo 0)

    # Check staleness (> 10 min = very stale)
    now=$(date +%s)
    age=$(echo "$now - ${timestamp%.*}" | bc 2>/dev/null || echo 0)
    if [ "$age" -gt 600 ] 2>/dev/null; then
        send_alert "[$name] VERY STALE: last update ${age}s ago"
    fi

    total_equity=$(echo "$total_equity + $equity" | bc 2>/dev/null || echo "$total_equity")
    total_original=$(echo "$total_original + $original" | bc 2>/dev/null || echo "$total_original")
    count=$((count + 1))
done

if [ "$count" -eq 0 ]; then
    echo "No state files found"
    exit 0
fi

# Calculate aggregate drawdown
if [ "$(echo "$total_original > 0" | bc 2>/dev/null)" = "1" ]; then
    dd_pct=$(echo "scale=1; ($total_original - $total_equity) / $total_original * 100" | bc 2>/dev/null || echo 0)

    if [ "$(echo "$dd_pct >= $KILL_THRESHOLD_PCT" | bc 2>/dev/null)" = "1" ]; then
        send_alert "Aggregate DD: ${dd_pct}% >= ${KILL_THRESHOLD_PCT}% threshold!"
        kill_all
        exit 1
    fi

    echo "OK: $count leaders, total \$${total_equity}, DD ${dd_pct}%"
else
    echo "OK: $count leaders, total \$${total_equity} (no original equity data)"
fi
