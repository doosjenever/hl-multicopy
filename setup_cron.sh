#!/bin/bash
# Setup cron jobs for Krabje's monitoring scripts.
# Run once on VPS after deployment.

set -euo pipefail

WORKDIR="/data/.openclaw/workspace/hl-multicopy"
PYTHON="/usr/bin/python3"
LOGDIR="${WORKDIR}/logs"

mkdir -p "$LOGDIR"

# Build crontab entries
CRON_ENTRIES="
# ── hl-multicopy monitoring (Krabje) ──────────────────────────
# Risk checker: every 3 minutes
*/3 * * * * cd ${WORKDIR} && ${PYTHON} risk_checker.py >> ${LOGDIR}/risk_checker.log 2>&1

# Portfolio tracker: every 30 minutes
*/30 * * * * cd ${WORKDIR} && ${PYTHON} portfolio_tracker.py >> ${LOGDIR}/portfolio_tracker.log 2>&1

# Leader scorer: Sunday 06:00 UTC
0 6 * * 0 cd ${WORKDIR} && ${PYTHON} leader_scorer.py >> ${LOGDIR}/leader_scorer.log 2>&1

# Rotation proposer: Sunday 06:30 UTC (30 min after scorer)
30 6 * * 0 cd ${WORKDIR} && ${PYTHON} rotation_proposer.py >> ${LOGDIR}/rotation_proposer.log 2>&1

# Fallback watchdog: every 2 minutes
*/2 * * * * cd ${WORKDIR} && bash fallback_watchdog.sh >> ${LOGDIR}/watchdog.log 2>&1
"

echo "The following cron entries will be added:"
echo "$CRON_ENTRIES"
echo ""
read -p "Add these to crontab? [y/N] " confirm

if [[ "${confirm,,}" == "y" ]]; then
    # Append to existing crontab (preserve other entries)
    (crontab -l 2>/dev/null || true; echo "$CRON_ENTRIES") | crontab -
    echo "Cron jobs installed. Verify with: crontab -l"
else
    echo "Cancelled. You can manually add these entries with: crontab -e"
fi
