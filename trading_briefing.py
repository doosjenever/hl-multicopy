#!/usr/bin/env python3
"""Trading Briefing — collects unified state for Krabje's AI analysis.

Gathers data from:
1. NEET copy trader (hl-copytrader/state.json)
2. All vossen (hl-multicopy/state/*.json)
3. Equity history (equity_history.jsonl)
4. Risk state

Outputs structured text that Krabje can reason about.

Usage:
    python3 trading_briefing.py              # Full briefing
    python3 trading_briefing.py --incident   # Incident mode (more detail)
"""

import json
import os
import sys
import time
from pathlib import Path

MULTICOPY_DIR = Path(__file__).parent
COPYTRADER_DIR = MULTICOPY_DIR.parent / "hl-copytrader"
STATE_DIR = MULTICOPY_DIR / "state"
EQUITY_LOG = MULTICOPY_DIR / "equity_history.jsonl"
RISK_STATE = MULTICOPY_DIR / "risk_state.json"


def load_neet_state() -> dict | None:
    """Load NEET copy trader state."""
    state_file = COPYTRADER_DIR / "state.json"
    if not state_file.exists():
        return None
    try:
        return json.loads(state_file.read_text())
    except Exception:
        return None


def load_neet_health() -> dict | None:
    """Check NEET bot health via PID files."""
    watchdog_pid = COPYTRADER_DIR / "state" / ".watchdog.pid"
    bot_pid = COPYTRADER_DIR / "state" / ".bot.pid"
    result = {"watchdog_alive": False, "bot_alive": False}

    for label, pid_file in [("watchdog_alive", watchdog_pid), ("bot_alive", bot_pid)]:
        if pid_file.exists():
            try:
                pid = int(pid_file.read_text().strip())
                os.kill(pid, 0)
                result[label] = True
            except (ValueError, ProcessLookupError, PermissionError):
                pass
    return result


def load_vossen_states() -> list[dict]:
    """Load all vossen state files."""
    states = []
    for f in sorted(STATE_DIR.glob("*.json")):
        if f.name.startswith("."):
            continue
        try:
            states.append(json.loads(f.read_text()))
        except Exception:
            states.append({"name": f.stem, "error": "corrupt state file"})
    return states


def load_supervisor_health() -> dict | None:
    """Load supervisor heartbeat."""
    hb_file = STATE_DIR / ".supervisor_heartbeat"
    if not hb_file.exists():
        return None
    try:
        return json.loads(hb_file.read_text())
    except Exception:
        return None


def load_risk_state() -> dict:
    """Load risk escalation state."""
    if RISK_STATE.exists():
        try:
            return json.loads(RISK_STATE.read_text())
        except Exception:
            pass
    return {}


def load_recent_equity(hours: int = 24) -> list[dict]:
    """Load recent equity log entries."""
    if not EQUITY_LOG.exists():
        return []
    cutoff = time.time() - (hours * 3600)
    entries = []
    try:
        with open(EQUITY_LOG) as f:
            for line in f:
                line = line.strip()
                if not line:
                    continue
                try:
                    entry = json.loads(line)
                    if entry.get("timestamp", 0) >= cutoff:
                        entries.append(entry)
                except Exception:
                    continue
    except Exception:
        pass
    return entries


def format_briefing(incident: bool = False) -> str:
    """Generate the full briefing text."""
    now = time.time()
    lines = []
    lines.append("=" * 60)
    lines.append("  TRADING BRIEFING — NEET + VOSSEN")
    lines.append(f"  Timestamp: {time.strftime('%Y-%m-%d %H:%M UTC', time.gmtime(now))}")
    lines.append("=" * 60)

    # ── NEET Copy Trader ──────────────────────────────────────
    lines.append("\n## NEET COPY TRADER (LIVE)")
    neet = load_neet_state()
    neet_health = load_neet_health()

    if neet_health:
        wd = "ALIVE" if neet_health["watchdog_alive"] else "DEAD"
        bot = "ALIVE" if neet_health["bot_alive"] else "DEAD"
        lines.append(f"  Process: watchdog={wd}, bot={bot}")

    if neet:
        age = now - neet.get("timestamp", 0)
        equity = neet.get("our_equity", 0)
        starting = neet.get("starting_equity", 0)
        target_eq = neet.get("target_equity", 0)
        pnl = neet.get("pnl", 0)
        pnl_pct = neet.get("pnl_pct", 0)
        trades = neet.get("trade_count", 0)
        positions = neet.get("positions", {})

        lines.append(f"  Mode: {neet.get('mode', '?')}")
        lines.append(f"  Equity: ${equity:,.2f} (start: ${starting:,.2f})")
        lines.append(f"  PnL: ${pnl:+,.2f} ({pnl_pct:+.1f}%)")
        lines.append(f"  Target equity: ${target_eq:,.2f}")
        lines.append(f"  Trades: {trades}")
        lines.append(f"  State age: {age:.0f}s")
        lines.append(f"  Positions ({len(positions)}):")
        for coin, pos in positions.items():
            lev = pos.get("leverage", {})
            lev_str = f" {lev.get('value', '?')}x" if lev else ""
            lines.append(f"    {coin}: {pos['side']} {pos['size']:.6g}@${pos['entry_price']:,.2f}{lev_str}")
    else:
        lines.append("  ⚠️ No state file found!")

    # ── Vossen (Paper Trading) ────────────────────────────────
    lines.append("\n## VOSSEN COPY TRADERS (PAPER)")
    vossen = load_vossen_states()
    sup = load_supervisor_health()

    if sup:
        sup_age = now - sup.get("timestamp", 0)
        lines.append(f"  Supervisor: uptime={sup.get('uptime_s', 0):.0f}s, heartbeat_age={sup_age:.0f}s")

    if not vossen:
        lines.append("  ⚠️ No vossen state files found!")
    else:
        total_equity = 0
        total_original = 0
        all_coins: dict[str, list] = {}  # coin -> list of (vos, side, notional)

        for v in vossen:
            if "error" in v:
                lines.append(f"  {v['name']}: ⚠️ {v['error']}")
                continue

            name = v.get("name", v.get("target", "?"))
            equity = v.get("equity", 0)
            original = v.get("original_start_equity", 0)
            dd = v.get("drawdown_pct", 0)
            trades = v.get("trade_count", 0)
            halted = v.get("halted", False)
            positions = v.get("positions", {})
            age = now - v.get("timestamp", 0)

            total_equity += equity
            total_original += original

            pnl_pct = ((equity - original) / original * 100) if original > 0 else 0
            status = "HALTED" if halted else ("STALE" if age > 300 else "OK")

            lines.append(f"  {name}: ${equity:,.2f} ({pnl_pct:+.1f}%) DD={abs(dd):.1f}% "
                         f"trades={trades} pos={len(positions)} [{status}]")

            # Track coin exposure across vossen
            for coin, pos in positions.items():
                notional = abs(pos.get("size", 0) * pos.get("entry_price", 0))
                side = pos.get("side", "?")
                if coin not in all_coins:
                    all_coins[coin] = []
                all_coins[coin].append((name, side, notional))

        if total_original > 0:
            total_pnl = ((total_equity - total_original) / total_original * 100)
            lines.append(f"\n  TOTAL: ${total_equity:,.2f} ({total_pnl:+.1f}%) from ${total_original:,.2f}")

        # ── Cross-correlation: NEET + Vossen ──────────────────
        lines.append("\n## EXPOSURE ANALYSIS (ALL BOTS)")

        # Add NEET positions to the mix
        if neet:
            for coin, pos in neet.get("positions", {}).items():
                notional = abs(pos.get("size", 0) * pos.get("entry_price", 0))
                side = pos.get("side", "?")
                if coin not in all_coins:
                    all_coins[coin] = []
                all_coins[coin].append(("NEET", side, notional))

        if all_coins:
            total_exposure = sum(n for entries in all_coins.values() for _, _, n in entries)
            for coin, entries in sorted(all_coins.items(), key=lambda x: sum(n for _, _, n in x[1]), reverse=True):
                coin_total = sum(n for _, _, n in entries)
                pct = (coin_total / total_exposure * 100) if total_exposure > 0 else 0
                bots = ", ".join(f"{name}({side})" for name, side, _ in entries)
                flag = " ⚠️ CONCENTRATED" if pct >= 40 else ""
                lines.append(f"  {coin}: ${coin_total:,.0f} ({pct:.0f}%) — {bots}{flag}")

            # Check for conflicting positions (one bot long, another short)
            for coin, entries in all_coins.items():
                sides = set(side for _, side, _ in entries)
                if len(sides) > 1:
                    lines.append(f"  ⚠️ CONFLICT: {coin} has both long and short across bots!")

    # ── Risk State ────────────────────────────────────────────
    risk = load_risk_state()
    if risk:
        lines.append("\n## RISK ESCALATION")
        for leader, state in risk.items():
            days = (now - state.get("dd_start", now)) / 86400
            lines.append(f"  {leader}: in drawdown for {days:.1f} days (since {time.strftime('%Y-%m-%d', time.gmtime(state.get('dd_start', 0)))})")

    # ── Equity Trend (last 24h) ──────────────────────────────
    if incident:
        equity_entries = load_recent_equity(24)
        if equity_entries:
            lines.append(f"\n## EQUITY HISTORY (last 24h, {len(equity_entries)} entries)")
            # Group by name, show first and last
            by_name: dict[str, list] = {}
            for e in equity_entries:
                n = e.get("name", "?")
                if n not in by_name:
                    by_name[n] = []
                by_name[n].append(e)
            for name, entries in sorted(by_name.items()):
                first = entries[0]
                last = entries[-1]
                delta = last.get("equity", 0) - first.get("equity", 0)
                lines.append(f"  {name}: ${first.get('equity', 0):,.2f} → ${last.get('equity', 0):,.2f} (Δ${delta:+,.2f})")

    lines.append("\n" + "=" * 60)
    return "\n".join(lines)


def main():
    incident = "--incident" in sys.argv
    print(format_briefing(incident=incident))


if __name__ == "__main__":
    main()
