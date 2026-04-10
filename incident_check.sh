#!/bin/bash
# Incident check — runs every 15 min.
# If any bot is unhealthy, outputs the briefing for Krabje to analyze.
# If everything is healthy, exits silently (exit 0 = no incident).

BASEDIR="/data/.openclaw/workspace/hl-multicopy"
COPYDIR="/data/.openclaw/workspace/hl-copytrader"

ISSUES=""

# Check vossen supervisor
PIDFILE="$BASEDIR/state/.supervisor.pid"
if [ -f "$PIDFILE" ]; then
    PID=$(cat "$PIDFILE")
    if ! kill -0 "$PID" 2>/dev/null; then
        ISSUES="$ISSUES\n- Vossen supervisor DEAD"
    fi
else
    ISSUES="$ISSUES\n- Vossen supervisor PID file missing"
fi

# Check NEET watchdog
PIDFILE="$COPYDIR/state/.watchdog.pid"
if [ -f "$PIDFILE" ]; then
    PID=$(cat "$PIDFILE")
    if ! kill -0 "$PID" 2>/dev/null; then
        ISSUES="$ISSUES\n- NEET watchdog DEAD"
    fi
else
    ISSUES="$ISSUES\n- NEET watchdog PID file missing"
fi

# Check NEET bot
PIDFILE="$COPYDIR/state/.bot.pid"
if [ -f "$PIDFILE" ]; then
    PID=$(cat "$PIDFILE")
    if ! kill -0 "$PID" 2>/dev/null; then
        ISSUES="$ISSUES\n- NEET bot process DEAD"
    fi
fi

# Check NEET state freshness
if [ -f "$COPYDIR/state.json" ]; then
    STATE_AGE=$(( $(date +%s) - $(stat -c %Y "$COPYDIR/state.json") ))
    if [ "$STATE_AGE" -gt 300 ]; then
        ISSUES="$ISSUES\n- NEET state STALE (${STATE_AGE}s old)"
    fi
fi

# Check vossen state freshness (any stale = issue)
for f in "$BASEDIR/state/"*.json; do
    [ -f "$f" ] || continue
    name=$(basename "$f" .json)
    [[ "$name" == .* ]] && continue
    STATE_AGE=$(( $(date +%s) - $(stat -c %Y "$f") ))
    if [ "$STATE_AGE" -gt 300 ]; then
        ISSUES="$ISSUES\n- Vos $name state STALE (${STATE_AGE}s old)"
    fi
done

# If no issues, exit cleanly
if [ -z "$ISSUES" ]; then
    exit 0
fi

# Issues found — output for Krabje
echo "INCIDENT DETECTED:"
echo -e "$ISSUES"
echo ""
echo "Full briefing:"
cd "$BASEDIR"
python3 trading_briefing.py --incident
exit 1
