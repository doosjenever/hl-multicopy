#!/usr/bin/env python3
"""Risk checker — runs every 3 minutes via cron.

Checks:
1. Individual drawdown per leader (warn/freeze/deactivate)
2. Aggregate portfolio drawdown (kill switch)
3. Coin correlation across leaders (concentration alert)
4. Liveness (stale heartbeat detection)

Sends Telegram alerts on threshold breaches.
"""

import json
import logging
import logging.handlers
import os
import subprocess
import sys
import time
from pathlib import Path

import httpx

# ── Config ──────────────────────────────────────────────────────

BASE_DIR = Path(__file__).parent
STATE_DIR = BASE_DIR / "state"
TELEGRAM_BOT_TOKEN = os.environ.get("TELEGRAM_BOT_TOKEN", "")
TELEGRAM_CHAT_ID = os.environ.get("TELEGRAM_CHAT_ID", "")

# Risk thresholds (matching plan)
DD_WARN_PCT = 20.0              # Telegram warning
DD_SOFT_FREEZE_PCT = 25.0       # Bot handles this (Layer 1)
DD_AUTO_FREEZE_DAYS = 7         # Days at -30% before auto-freeze
DD_AUTO_DEACTIVATE_DAYS = 14    # Days at -30% before auto-deactivate
DD_HARD_PCT = 30.0              # Threshold for time-based escalation
AGGREGATE_KILL_PCT = 20.0       # Kill switch for total portfolio
CORRELATION_THRESHOLD = 0.40    # Alert if >40% exposure in 1 coin
STALE_HEARTBEAT_S = 300         # 5 min without state update = stale
DD_RECOVERY_PCT = 15.0          # Only reset escalation timer when DD drops below this

# Persistent state for time-based escalation
RISK_STATE_FILE = BASE_DIR / "risk_state.json"
AGGREGATE_RISK_FILE = STATE_DIR / "aggregate_risk.json"


def send_telegram(message: str):
    """Send alert via Telegram."""
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


def load_risk_state() -> dict:
    """Load persistent risk state (tracks how long leaders have been in DD)."""
    if RISK_STATE_FILE.exists():
        try:
            return json.loads(RISK_STATE_FILE.read_text())
        except Exception:
            pass
    return {}


def save_risk_state(state: dict):
    tmp = RISK_STATE_FILE.with_suffix(".tmp")
    tmp.write_text(json.dumps(state, indent=2))
    os.replace(str(tmp), str(RISK_STATE_FILE))


def _write_withdraw_request(name: str, equity: float):
    """Write a withdraw request for the orchestrator to execute."""
    request_dir = STATE_DIR / "transfer_requests"
    request_dir.mkdir(exist_ok=True)

    # Check if we already wrote one for this leader (avoid duplicates)
    # Only check active .json files, not .failed/.done/.processing (H7 fix)
    existing = list(request_dir.glob(f"withdraw_{name}_*.json"))
    active = [f for f in existing if f.suffix == ".json"]
    if active:
        return

    request = {
        "type": "withdraw",
        "name": name,
        "amount": round(equity, 2),
        "sub_account": "",  # orchestrator looks up from config
        "timestamp": time.time(),
        "reason": "auto-deactivate",
    }

    request_file = request_dir / f"withdraw_{name}_{int(time.time())}.json"
    tmp = request_file.with_suffix(".tmp")
    tmp.write_text(json.dumps(request, indent=2))
    os.replace(str(tmp), str(request_file))
    print(f"WITHDRAW REQUEST: [{name}] ${equity:,.2f}")


def freeze_leader(name: str):
    """Stop a leader via supervisor command file."""
    cmd_file = STATE_DIR / ".supervisor_cmd"
    try:
        tmp = cmd_file.with_suffix(".tmp")
        tmp.write_text(f"stop:{name}\n")
        os.replace(str(tmp), str(cmd_file))
        send_telegram(f"<b>AUTO-FREEZE</b>\n[{name}] Stopped by risk checker")
    except Exception as e:
        send_telegram(f"<b>FREEZE FAILED</b>\n[{name}] Could not send stop command: {e}")


def main():
    log_path = BASE_DIR / "logs" / "risk_checker.log"
    log_path.parent.mkdir(exist_ok=True)
    logging.basicConfig(
        handlers=[logging.handlers.RotatingFileHandler(
            log_path, maxBytes=2 * 1024 * 1024, backupCount=2, encoding="utf-8"
        )],
        level=logging.INFO,
        format="%(asctime)s %(message)s",
    )

    from shared_state import read_all_states

    states = read_all_states(STATE_DIR)
    if not states:
        print("No state files found, nothing to check")
        return

    risk_state = load_risk_state()
    now = time.time()
    alerts = []

    # ── Per-leader checks ──────────────────────────────────────

    total_equity = 0.0
    total_original = 0.0
    all_coins: dict[str, float] = {}  # coin -> total notional across all leaders

    for s in states:
        total_equity += s.equity
        total_original += s.original_start_equity
        leader_key = s.name

        # Liveness check
        age = now - s.timestamp
        if age > STALE_HEARTBEAT_S and s.is_alive:
            alerts.append(f"[{s.name}] STALE: last update {age:.0f}s ago")

        # Individual drawdown (absolute value)
        dd = abs(s.drawdown_pct)

        # Warning at -20%
        if dd >= DD_WARN_PCT:
            alerts.append(f"[{s.name}] DD WARNING: {dd:.1f}% (equity ${s.equity:,.2f})")

        # Time-based escalation at -30%
        if dd >= DD_HARD_PCT:
            if leader_key not in risk_state:
                risk_state[leader_key] = {"dd_start": now}
                alerts.append(
                    f"[{s.name}] 🔴 DD CRITICAL: {dd:.1f}% — tracking for escalation"
                )
            else:
                days_in_dd = (now - risk_state[leader_key]["dd_start"]) / 86400
                # Always alert when DD >= 30% (every check, not just at thresholds)
                if days_in_dd < DD_AUTO_FREEZE_DAYS:
                    alerts.append(
                        f"[{s.name}] DD CRITICAL: {dd:.1f}% for {days_in_dd:.1f}d "
                        f"(freeze at {DD_AUTO_FREEZE_DAYS}d, equity ${s.equity:,.2f})"
                    )

                if days_in_dd >= DD_AUTO_DEACTIVATE_DAYS:
                    if not s.halted:
                        freeze_leader(s.name)
                    # K5 fix: write PENDING deactivate instead of immediate withdraw
                    pending_dir = STATE_DIR / "transfer_requests"
                    pending_dir.mkdir(exist_ok=True)
                    pending_files = list(pending_dir.glob(f"pending_deactivate_{s.name}_*.json"))
                    if not pending_files:
                        pending = {
                            "type": "pending_deactivate",
                            "name": s.name,
                            "amount": round(s.equity, 2),
                            "timestamp": now,
                            "execute_after": now + 86400,  # 24h grace period
                            "reason": f"DD {dd:.1f}% for {days_in_dd:.1f} days",
                        }
                        pf = pending_dir / f"pending_deactivate_{s.name}_{int(now)}.json"
                        tmp = pf.with_suffix(".tmp")
                        tmp.write_text(json.dumps(pending, indent=2))
                        os.replace(str(tmp), str(pf))
                        alerts.append(
                            f"[{s.name}] 🔴 PENDING DEACTIVATE: {dd:.1f}% DD for "
                            f"{days_in_dd:.1f} days. Auto-withdraw in 24h unless cancelled."
                        )
                    else:
                        alerts.append(
                            f"[{s.name}] AUTO-DEACTIVATE pending ({dd:.1f}% DD, {days_in_dd:.1f}d)"
                        )
                elif days_in_dd >= DD_AUTO_FREEZE_DAYS:
                    if not s.halted:
                        alerts.append(
                            f"[{s.name}] AUTO-FREEZE: {dd:.1f}% DD for {days_in_dd:.1f} days"
                        )
                        freeze_leader(s.name)
                    else:
                        alerts.append(
                            f"[{s.name}] DD HELD: {dd:.1f}% for {days_in_dd:.1f} days (already frozen)"
                        )
        else:
            # K4 fix: Only clear escalation timer on REAL recovery (below DD_RECOVERY_PCT)
            # This prevents timer reset from DD oscillation around the threshold
            if leader_key in risk_state and dd < DD_RECOVERY_PCT:
                del risk_state[leader_key]
                alerts.append(f"[{s.name}] DD recovered to {dd:.1f}% — escalation timer cleared")

        # H9 fix: Coin exposure tracking using current price (not entry_price)
        for coin, pos in s.positions.items():
            size = abs(pos.get("size", 0))
            # Prefer current_price from state, fall back to entry_price
            price = pos.get("current_price", pos.get("entry_price", 0))
            notional = size * price
            all_coins[coin] = all_coins.get(coin, 0) + notional

    # ── Aggregate portfolio check (K7 fix: track real portfolio peak) ──

    # Load/update real aggregate peak (not sum of individual peaks)
    agg_risk = {}
    if AGGREGATE_RISK_FILE.exists():
        try:
            agg_risk = json.loads(AGGREGATE_RISK_FILE.read_text())
        except Exception:
            pass

    aggregate_peak = max(
        agg_risk.get("aggregate_peak_equity", total_equity),
        total_equity,
    )
    agg_risk["aggregate_peak_equity"] = aggregate_peak
    agg_risk["last_total_equity"] = total_equity
    agg_risk["last_check_ts"] = now

    try:
        tmp = AGGREGATE_RISK_FILE.with_suffix(".tmp")
        tmp.write_text(json.dumps(agg_risk, indent=2))
        os.replace(str(tmp), str(AGGREGATE_RISK_FILE))
    except Exception:
        pass

    if aggregate_peak > 0:
        aggregate_dd = (aggregate_peak - total_equity) / aggregate_peak * 100
        if aggregate_dd >= AGGREGATE_KILL_PCT:
            alerts.append(
                f"KILL SWITCH: Aggregate DD {aggregate_dd:.1f}% >= {AGGREGATE_KILL_PCT}%! "
                f"Total equity: ${total_equity:,.2f} (peak: ${aggregate_peak:,.2f})"
            )
            # Freeze ALL leaders
            for s in states:
                if not s.halted:
                    freeze_leader(s.name)

    # ── Correlation check (H10 fix: group HIP-3 @NNN with base coin) ──

    # Group coins by underlying: "@123" spot variants map to their perp
    # e.g. "@4" (BTC spot on HIP-3) and "BTC" perp are the same underlying
    grouped_coins: dict[str, float] = {}
    coin_to_group: dict[str, str] = {}
    for coin, notional in all_coins.items():
        # HIP-3 coins use "dex:SYMBOL" format (e.g. "xyz:BRENTOIL")
        # or "@NNN" format for spot. Group "xyz:X" with "X" if both exist.
        if ":" in coin:
            underlying = coin.split(":")[-1]  # "xyz:BRENTOIL" -> "BRENTOIL"
        elif coin.startswith("@"):
            underlying = coin  # Keep @NNN as-is (no matching perp symbol)
        else:
            underlying = coin
        coin_to_group[coin] = underlying
        grouped_coins[underlying] = grouped_coins.get(underlying, 0) + notional

    total_exposure = sum(grouped_coins.values())
    if total_exposure > 0:
        for underlying, notional in grouped_coins.items():
            concentration = notional / total_exposure
            if concentration >= CORRELATION_THRESHOLD:
                leaders_with_coin = [
                    s.name for s in states
                    if any(coin_to_group.get(c) == underlying for c in s.positions)
                ]
                # List the actual coins in this group
                actual_coins = [c for c, g in coin_to_group.items() if g == underlying]
                coin_label = "/".join(actual_coins) if len(actual_coins) > 1 else underlying
                alerts.append(
                    f"CONCENTRATION: {coin_label} = {concentration:.0%} of total exposure "
                    f"(${notional:,.0f}/${total_exposure:,.0f}) "
                    f"across [{', '.join(leaders_with_coin)}]"
                )

    # ── Report ─────────────────────────────────────────────────

    save_risk_state(risk_state)

    if alerts:
        header = f"<b>Risk Check</b> ({len(states)} leaders)\n"
        message = header + "\n".join(alerts)
        send_telegram(message)
        for a in alerts:
            print(a)
    else:
        print(f"OK: {len(states)} leaders, total equity ${total_equity:,.2f}")


if __name__ == "__main__":
    main()
