#!/usr/bin/env python3
"""Health check — independent monitoring script.

Can be called by Docker HEALTHCHECK, external monitoring, or cron.
Checks supervisor, bot processes, and state freshness.

Exit codes:
    0 = healthy
    1 = unhealthy (details on stderr)

Usage:
    python3 healthcheck.py            # Check and exit
    python3 healthcheck.py --alert    # Check and send Telegram alert if unhealthy
    python3 healthcheck.py --json     # Output JSON status
"""

import json
import os
import sys
import time
from pathlib import Path

import httpx

BASE_DIR = Path(__file__).parent
STATE_DIR = BASE_DIR / "state"
CONFIGS_DIR = BASE_DIR / "configs"
HEARTBEAT_FILE = STATE_DIR / ".supervisor_heartbeat"
PID_FILE = STATE_DIR / ".supervisor.pid"
ALERT_DEDUP_FILE = STATE_DIR / ".last_health_alert"

TELEGRAM_BOT_TOKEN = os.environ.get("TELEGRAM_BOT_TOKEN", "")
TELEGRAM_CHAT_ID = os.environ.get("TELEGRAM_CHAT_ID", "")

MAX_HEARTBEAT_AGE_S = 120       # Supervisor heartbeat must be < 2 min old
MAX_STATE_AGE_S = 300           # Bot state files must be < 5 min old
ALERT_DEDUP_S = 600             # Max 1 alert per 10 min


def check_supervisor() -> tuple[bool, str]:
    """Check if supervisor process is alive."""
    # PID check skipped because this script runs inside Docker and cannot see host PIDs
    
    # Check heartbeat freshness
    if not HEARTBEAT_FILE.exists():
        return False, "Supervisor heartbeat file missing"

    try:
        hb = json.loads(HEARTBEAT_FILE.read_text())
        age = time.time() - hb.get("timestamp", 0)
        if age > MAX_HEARTBEAT_AGE_S:
            return False, f"Supervisor heartbeat stale ({age:.0f}s old)"
    except Exception as e:
        return False, f"Supervisor heartbeat unreadable: {e}"

    return True, "OK"


def check_bots() -> tuple[bool, list[str]]:
    """Check that all expected bots are running and state is fresh."""
    issues = []

    # Expected bots from configs
    expected = set()
    for f in CONFIGS_DIR.glob("*.yaml"):
        if f.name in ("example.yaml", "alice.yaml", "bob.yaml", "carol.yaml"):
            continue
        expected.add(f.stem)

    if not expected:
        return True, []

    # Check heartbeat for bot status
    if HEARTBEAT_FILE.exists():
        try:
            hb = json.loads(HEARTBEAT_FILE.read_text())
            bots = hb.get("bots", {})

            for name in expected:
                if name not in bots:
                    issues.append(f"{name}: not tracked by supervisor")
                    continue
                bot = bots[name]
                if bot.get("halted"):
                    issues.append(f"{name}: HALTED (too many crashes)")
                elif not bot.get("running"):
                    if not bot.get("intentional_stop"):
                        issues.append(f"{name}: not running")
        except Exception:
            issues.append("Cannot read supervisor heartbeat")

    # Check state file freshness
    for name in expected:
        state_file = STATE_DIR / f"{name}.json"
        if not state_file.exists():
            continue  # New bot, no state yet
        age = time.time() - state_file.stat().st_mtime
        if age > MAX_STATE_AGE_S:
            issues.append(f"{name}: state stale ({age:.0f}s old)")

    return len(issues) == 0, issues


def send_alert(message: str):
    """Send Telegram alert with dedup."""
    if not TELEGRAM_BOT_TOKEN or not TELEGRAM_CHAT_ID:
        return

    # Dedup check
    if ALERT_DEDUP_FILE.exists():
        try:
            last = float(ALERT_DEDUP_FILE.read_text().strip())
            if time.time() - last < ALERT_DEDUP_S:
                return
        except Exception:
            pass

    try:
        httpx.post(
            f"https://api.telegram.org/bot{TELEGRAM_BOT_TOKEN}/sendMessage",
            json={
                "chat_id": TELEGRAM_CHAT_ID,
                "text": message,
                "parse_mode": "HTML",
            },
            timeout=10,
        )
        ALERT_DEDUP_FILE.write_text(str(time.time()))
    except Exception:
        pass


def main():
    alert_mode = "--alert" in sys.argv
    json_mode = "--json" in sys.argv

    sup_ok, sup_msg = check_supervisor()
    bots_ok, bot_issues = check_bots()
    healthy = sup_ok and bots_ok

    if json_mode:
        result = {
            "healthy": healthy,
            "timestamp": time.time(),
            "supervisor": {"ok": sup_ok, "message": sup_msg},
            "bots": {"ok": bots_ok, "issues": bot_issues},
        }
        print(json.dumps(result, indent=2))
    else:
        if healthy:
            print("HEALTHY")
        else:
            issues = []
            if not sup_ok:
                issues.append(f"Supervisor: {sup_msg}")
            issues.extend(bot_issues)
            print("UNHEALTHY")
            for issue in issues:
                print(f"  - {issue}", file=sys.stderr)

    if not healthy and alert_mode:
        lines = ["🦊 <b>[HEALTH CHECK FAILED]</b>"]
        if not sup_ok:
            lines.append(f"Supervisor: {sup_msg}")
        for issue in bot_issues:
            lines.append(f"• {issue}")
        send_alert("\n".join(lines))

    sys.exit(0 if healthy else 1)


if __name__ == "__main__":
    main()
