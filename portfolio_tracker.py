#!/usr/bin/env python3
"""Portfolio tracker — runs every 30 minutes via cron.

Responsibilities:
1. Append equity snapshots to equity_history.jsonl
2. Performance comparison between leaders
3. Profit-taking: skim profits to main wallet when thresholds hit

Profit-taking triggers (from plan):
- After 7+ days active AND 20%+ profit -> skim 50% of profit
- After 14+ days active AND 30%+ profit -> skim 50%
- At 50%+ profit -> ALWAYS skim 50% (regardless of duration)
- Baseline = original_start_equity (NEVER reset after withdrawal)
"""

import json
import logging
import logging.handlers
import os
import time
from pathlib import Path

import httpx

# ── Config ──────────────────────────────────────────────────────

BASE_DIR = Path(__file__).parent
STATE_DIR = BASE_DIR / "state"
EQUITY_LOG = BASE_DIR / "equity_history.jsonl"
TELEGRAM_BOT_TOKEN = os.environ.get("TELEGRAM_BOT_TOKEN", "")
TELEGRAM_CHAT_ID = os.environ.get("TELEGRAM_CHAT_ID", "")

# Profit-taking thresholds
PROFIT_TAKE_RULES = [
    {"min_days": 0,  "min_profit_pct": 50.0, "skim_pct": 50.0},  # 50%+ profit -> always skim
    {"min_days": 14, "min_profit_pct": 30.0, "skim_pct": 50.0},  # 14d + 30% -> skim
    {"min_days": 7,  "min_profit_pct": 20.0, "skim_pct": 50.0},  # 7d + 20% -> skim
]

# Persistent tracker state
TRACKER_STATE_FILE = BASE_DIR / "tracker_state.json"

# Hyperliquid API
HL_API_URL = "https://api.hyperliquid.xyz"


def send_telegram(message: str):
    if not TELEGRAM_BOT_TOKEN or not TELEGRAM_CHAT_ID:
        print(f"[ALERT] {message}")
        return
    try:
        httpx.post(
            f"https://api.telegram.org/bot{TELEGRAM_BOT_TOKEN}/sendMessage",
            json={
                "chat_id": TELEGRAM_CHAT_ID,
                "text": message,
                "parse_mode": "HTML",
                "disable_web_page_preview": True,
            },
            timeout=10,
        )
    except Exception as e:
        print(f"Telegram send failed: {e}")


def load_tracker_state() -> dict:
    if TRACKER_STATE_FILE.exists():
        try:
            return json.loads(TRACKER_STATE_FILE.read_text())
        except Exception:
            pass
    return {}


def save_tracker_state(state: dict):
    tmp = TRACKER_STATE_FILE.with_suffix(".tmp")
    tmp.write_text(json.dumps(state, indent=2))
    os.replace(str(tmp), str(TRACKER_STATE_FILE))


def check_profit_taking(name: str, equity: float, original_equity: float,
                        start_ts: float, tracker: dict) -> dict | None:
    """Check if profit-taking should trigger. Returns skim details or None."""
    if original_equity <= 0:
        return None

    # Use baseline_equity (raised after each skim) to measure NEW profit only
    # This prevents re-skimming already-taken profit
    baseline = tracker.get(name, {}).get("baseline_equity", original_equity)
    profit = equity - baseline
    # H5 fix: measure profit% against baseline (not original) after skim
    profit_pct = (profit / baseline) * 100

    if profit_pct <= 0:
        return None

    days_active = (time.time() - start_ts) / 86400

    # Check rules in order (most aggressive first)
    for rule in PROFIT_TAKE_RULES:
        if days_active >= rule["min_days"] and profit_pct >= rule["min_profit_pct"]:
            skim_amount = profit * (rule["skim_pct"] / 100.0)

            # Don't skim if we already skimmed recently (within 24h)
            last_skim = tracker.get(name, {}).get("last_skim_ts", 0)
            if time.time() - last_skim < 86400:
                return None

            # L5 fix: Cap skim to max 30% of equity to prevent liquidation
            # (margin is needed for open positions)
            max_safe_skim = equity * 0.30
            safe_skim = min(skim_amount, max_safe_skim)
            if safe_skim < 1.0:
                return None  # Too small to skim

            return {
                "amount": safe_skim,
                "profit_pct": profit_pct,
                "rule": f"{rule['min_days']}d+{rule['min_profit_pct']}%",
                "days_active": days_active,
                "capped": safe_skim < skim_amount,
            }

    return None


def execute_profit_skim(name: str, amount: float, sub_account: str):
    """Write a skim request for the orchestrator to execute.

    Krabje writes JSON request files, the host orchestrator
    picks them up via 'orchestrator.py process-transfers' (cron every 5min).
    """
    request_dir = STATE_DIR / "transfer_requests"
    request_dir.mkdir(exist_ok=True)

    request = {
        "type": "skim",
        "name": name,
        "amount": round(amount, 2),
        "sub_account": sub_account,
        "timestamp": time.time(),
    }

    request_file = request_dir / f"skim_{name}_{int(time.time())}.json"
    tmp = request_file.with_suffix(".tmp")
    tmp.write_text(json.dumps(request, indent=2))
    os.replace(str(tmp), str(request_file))
    print(f"PROFIT SKIM REQUEST: [{name}] ${amount:,.2f} -> {request_file.name}")


def main():
    log_path = BASE_DIR / "logs" / "portfolio_tracker.log"
    log_path.parent.mkdir(exist_ok=True)
    logging.basicConfig(
        handlers=[logging.handlers.RotatingFileHandler(
            log_path, maxBytes=2 * 1024 * 1024, backupCount=2, encoding="utf-8"
        )],
        level=logging.INFO,
        format="%(asctime)s %(message)s",
    )

    from shared_state import read_all_states, append_equity_log

    states = read_all_states(STATE_DIR)
    if not states:
        print("No state files found")
        return

    tracker = load_tracker_state()
    reports = []

    # ── Log equity snapshots ───────────────────────────────────

    for s in states:
        append_equity_log(
            EQUITY_LOG,
            name=s.name,
            equity=s.equity,
            drawdown_pct=s.drawdown_pct,
            positions=s.position_count,
        )

    # ── Performance comparison ─────────────────────────────────

    sorted_by_pnl = sorted(states, key=lambda s: s.pnl_pct, reverse=True)
    for rank, s in enumerate(sorted_by_pnl, 1):
        dd_str = f"DD={abs(s.drawdown_pct):.1f}%" if s.drawdown_pct < 0 else "DD=0%"
        halt_str = " [HALTED]" if s.halted else ""
        reports.append(
            f"{rank}. [{s.name}] ${s.equity:,.2f} "
            f"({s.pnl_pct:+.1f}%) {dd_str} "
            f"{s.position_count}pos {s.trade_count}trades{halt_str}"
        )

    # ── Profit taking check ────────────────────────────────────

    skim_alerts = []
    for s in states:
        if s.halted or s.mode != "live":
            continue

        # Estimate start timestamp from first equity log entry
        start_ts = tracker.get(s.name, {}).get("start_ts", s.timestamp)
        if s.name not in tracker:
            tracker[s.name] = {"start_ts": s.timestamp}

        skim = check_profit_taking(
            s.name, s.equity, s.original_start_equity, start_ts, tracker
        )
        if skim:
            skim_alerts.append(
                f"[{s.name}] PROFIT TAKE: ${skim['amount']:,.2f} "
                f"(+{skim['profit_pct']:.1f}%, {skim['days_active']:.0f}d, rule={skim['rule']})"
            )
            # Record skim
            if s.name not in tracker:
                tracker[s.name] = {}
            tracker[s.name]["last_skim_ts"] = time.time()
            tracker[s.name]["last_skim_amount"] = skim["amount"]
            tracker[s.name]["total_skimmed"] = tracker[s.name].get("total_skimmed", 0) + skim["amount"]
            # H6 fix: Raise baseline so future profit is measured from post-skim equity
            # Use max() to prevent baseline from dropping below original (corruption guard)
            new_baseline = s.equity - skim["amount"]
            tracker[s.name]["baseline_equity"] = max(new_baseline, s.original_start_equity)

            # Write skim request for orchestrator to execute
            sub = s.positions  # We need sub_account from config, not positions
            # Load config to get sub_account
            config_path = BASE_DIR / "configs" / f"{s.name}.yaml"
            sub_account = ""
            if config_path.exists():
                try:
                    import yaml
                    cfg = yaml.safe_load(config_path.read_text())
                    sub_account = cfg.get("leader", {}).get("sub_account", "")
                except Exception:
                    pass
            if sub_account:
                execute_profit_skim(s.name, skim["amount"], sub_account)

    save_tracker_state(tracker)

    # ── Report ─────────────────────────────────────────────────

    total_equity = sum(s.equity for s in states)
    total_original = sum(s.original_start_equity for s in states)
    total_pnl_pct = ((total_equity - total_original) / total_original * 100) if total_original > 0 else 0

    header = (
        f"<b>Portfolio Update</b>\n"
        f"Total: ${total_equity:,.2f} ({total_pnl_pct:+.1f}%)\n\n"
    )
    body = "\n".join(reports)

    message = header + body
    if skim_alerts:
        message += "\n\n<b>Profit Taking:</b>\n" + "\n".join(skim_alerts)

    send_telegram(message)
    print(f"Portfolio: ${total_equity:,.2f} ({total_pnl_pct:+.1f}%), {len(states)} leaders")


if __name__ == "__main__":
    main()
