#!/bin/bash
# Ensure supervisor is running — called by OpenClaw cron every minute.
# If supervisor is not running, starts it in the background.

BASEDIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PIDFILE="$BASEDIR/state/.supervisor.pid"
LOGFILE="$BASEDIR/logs/supervisor.log"

# Check if supervisor is alive
if [ -f "$PIDFILE" ]; then
    PID=$(cat "$PIDFILE")
    if kill -0 "$PID" 2>/dev/null; then
        exit 0  # Running, nothing to do
    fi
fi

# Not running — start it
echo "$(date -u '+%Y-%m-%d %H:%M:%S') Supervisor not running, starting..." >> "$LOGFILE"
cd "$BASEDIR"
nohup python3 -u supervisor.py >> "$LOGFILE" 2>&1 &
echo $! > "$PIDFILE"
echo "$(date -u '+%Y-%m-%d %H:%M:%S') Supervisor started (PID $!)" >> "$LOGFILE"
