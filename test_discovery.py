#!/usr/bin/env python3
"""Test discovery script — replicate ApexLiquid-style filtering using raw HL API.

Filters (matching ApexLiquid screenshot):
  - Perps Balance >= $50K
  - Trade Time >= 30 days
  - 1D ROE >= 1%
  - 7D ROE >= 10%
  - 30D ROE >= 25%
  - All ROE >= 200%
  - MAX Drawdown <= 40%
  - Last Trade <= 2 days ago
  - Win Rate: shown but not filtered

Usage:
  python test_discovery.py [--top N] [--hip3]
"""

import argparse
import math
import time
import sys
from datetime import datetime, timezone

import httpx

# ── Endpoints ──────────────────────────────────────────────────
LEADERBOARD_URL = "https://stats-data.hyperliquid.xyz/Mainnet/leaderboard"
HL_API_URL = "https://api.hyperliquid.xyz"

# ── Filter defaults (match ApexLiquid screenshot) ─────────────
FILTERS = {
    "min_perps_balance": 25_000,
    "min_trade_days": 30,
    "min_1d_roe": 0.5,       # %  — just needs to be green today
    "min_7d_roe": 5.0,       # %  — solid week, not insane
    "min_30d_roe": 15.0,     # %  — consistent monthly performer
    "min_all_roe": 100.0,    # %  — doubled their account lifetime
    "max_drawdown": 75.0,    # %  — loose, bot has own DD protection
    "max_last_trade_days": 2,
}

# ── Rate limiting ─────────────────────────────────────────────
API_DELAY_S = 1.0
API_RETRY_429 = 3
API_BACKOFF_S = 10


def api_post(client: httpx.Client, payload: dict) -> dict | list | None:
    """POST to HL API with retry on 429."""
    for attempt in range(API_RETRY_429 + 1):
        try:
            resp = client.post(f"{HL_API_URL}/info", json=payload, timeout=15)
            if resp.status_code == 429:
                wait = API_BACKOFF_S * (attempt + 1)
                print(f"  429 rate limited, waiting {wait}s")
                time.sleep(wait)
                continue
            resp.raise_for_status()
            return resp.json()
        except httpx.HTTPStatusError:
            raise
        except Exception as e:
            print(f"  API error: {e}")
            return None
    return None


def fetch_leaderboard(client: httpx.Client) -> list[dict]:
    """Fetch full leaderboard."""
    resp = client.get(LEADERBOARD_URL, timeout=30)
    resp.raise_for_status()
    data = resp.json()
    rows = data.get("leaderboardRows", data) if isinstance(data, dict) else data
    print(f"Leaderboard: {len(rows)} traders")
    return rows


def extract_window(row: dict, window: str) -> dict:
    """Extract pnl/roi from windowPerformances."""
    for entry in row.get("windowPerformances", []):
        if isinstance(entry, list) and len(entry) == 2 and entry[0] == window:
            return entry[1]
    return {}


def pre_filter(rows: list[dict]) -> list[dict]:
    """Fast pre-filter on leaderboard data.

    Only filters on data available in leaderboard response:
    account_value and all-time ROI.
    """
    candidates = []
    for row in rows:
        try:
            account_value = float(row.get("accountValue", 0))
            perf_all = extract_window(row, "allTime")
            roi_all = float(perf_all.get("roi", 0))
            pnl_all = float(perf_all.get("pnl", 0))

            # Quick filters from leaderboard data
            if account_value < FILTERS["min_perps_balance"]:
                continue
            if roi_all * 100 < FILTERS["min_all_roe"]:
                continue
            if pnl_all <= 0:
                continue

            # Extract day/week/month ROI from leaderboard (if available)
            perf_day = extract_window(row, "day")
            perf_week = extract_window(row, "week")
            perf_month = extract_window(row, "month")

            candidates.append({
                "address": row.get("ethAddress", ""),
                "display_name": row.get("displayName") or "",
                "account_value": account_value,
                "roi_all": roi_all * 100,
                "pnl_all": pnl_all,
                # Leaderboard ROI per window (may be 0 if not available)
                "roi_day": float(perf_day.get("roi", 0)) * 100,
                "roi_week": float(perf_week.get("roi", 0)) * 100,
                "roi_month": float(perf_month.get("roi", 0)) * 100,
            })
        except (ValueError, TypeError):
            continue

    # Pre-filter on leaderboard window ROIs
    filtered = []
    for c in candidates:
        if c["roi_day"] < FILTERS["min_1d_roe"]:
            continue
        if c["roi_week"] < FILTERS["min_7d_roe"]:
            continue
        if c["roi_month"] < FILTERS["min_30d_roe"]:
            continue
        filtered.append(c)

    # Sort by 30D ROE descending
    filtered.sort(key=lambda x: x["roi_month"], reverse=True)
    print(f"Pre-filtered: {len(filtered)} candidates (from {len(candidates)} with AV+ROI)")
    return filtered


def deep_scan(client: httpx.Client, address: str) -> dict | None:
    """Deep scan: portfolio (drawdown, ROE verification) + fills (last trade, win rate)."""

    # 1. Portfolio — drawdown + ROE per window
    portfolio = api_post(client, {"type": "portfolio", "user": address})
    if portfolio is None:
        return None

    # Parse all windows
    windows = {}
    if isinstance(portfolio, list):
        for item in portfolio:
            if isinstance(item, list) and len(item) == 2:
                windows[item[0]] = item[1]

    all_time = windows.get("allTime")
    if not all_time:
        return None

    av_history = all_time.get("accountValueHistory", [])
    if len(av_history) < 10:
        return None

    # Calculate max drawdown: both all-time and 30D
    # All-time DD
    peak = 0.0
    max_dd_alltime = 0.0
    all_vals = []
    for point in av_history:
        val = float(point[1] if isinstance(point, list) else point.get("accountValue", 0))
        ts = point[0] if isinstance(point, list) else point.get("time", 0)
        all_vals.append((ts, val))
        if val > peak:
            peak = val
        if peak > 0:
            dd = (peak - val) / peak * 100
            max_dd_alltime = max(max_dd_alltime, dd)

    # 30D DD (from current peak within last 30 days)
    now_ms = int(time.time() * 1000)
    thirty_days_ms = 30 * 86400 * 1000
    recent_vals = [v for ts, v in all_vals if ts >= now_ms - thirty_days_ms]
    max_dd_30d = 0.0
    if recent_vals:
        peak_30d = recent_vals[0]
        for val in recent_vals:
            if val > peak_30d:
                peak_30d = val
            if peak_30d > 0:
                dd = (peak_30d - val) / peak_30d * 100
                max_dd_30d = max(max_dd_30d, dd)

    # Use all-time DD for filtering (user intended MAX DD = 40%, not MIN)
    max_dd = max_dd_alltime

    # Calculate trade time (days since first entry)
    first_ts = av_history[0][0] if isinstance(av_history[0], list) else av_history[0].get("time", 0)
    trade_days = (now_ms - first_ts) / (86400 * 1000)

    # Calculate ROE per window from portfolio pnlHistory
    # ROE = period_pnl / start_equity_at_period_start * 100
    roe_data = {}
    for window_name, period_key in [("day", "perpDay"), ("week", "perpWeek"),
                                      ("month", "perpMonth"), ("allTime", "perpAllTime")]:
        w = windows.get(period_key, {})
        pnl_hist = w.get("pnlHistory", [])
        av_hist = w.get("accountValueHistory", [])
        if pnl_hist and av_hist:
            start_av = float(av_hist[0][1] if isinstance(av_hist[0], list) else av_hist[0].get("accountValue", 0))
            end_pnl = float(pnl_hist[-1][1] if isinstance(pnl_hist[-1], list) else pnl_hist[-1].get("pnl", 0))
            start_pnl = float(pnl_hist[0][1] if isinstance(pnl_hist[0], list) else pnl_hist[0].get("pnl", 0))
            period_pnl = end_pnl - start_pnl
            # Guard against tiny start equity causing absurd ROE
            if start_av >= 100:
                roe_data[window_name] = period_pnl / start_av * 100

    time.sleep(API_DELAY_S)

    # 2. Recent fills — last trade + win rate
    fills = api_post(client, {"type": "userFills", "user": address})
    if fills is None:
        return None

    last_trade_ts = 0
    wins = 0
    losses = 0
    total_profit = 0.0
    total_loss = 0.0
    hip3_coins = set()
    crypto_coins = set()

    for fill in fills:
        ts = fill.get("time", 0)
        if ts > last_trade_ts:
            last_trade_ts = ts

        coin = fill.get("coin", "")
        # Categorize
        if coin.startswith("@") or ":" in coin:
            hip3_coins.add(coin)
        elif coin and coin != "USDC":
            crypto_coins.add(coin)

        closed_pnl = float(fill.get("closedPnl", "0"))
        if closed_pnl > 0:
            wins += 1
            total_profit += closed_pnl
        elif closed_pnl < 0:
            losses += 1
            total_loss += abs(closed_pnl)

    last_trade_days = (now_ms - last_trade_ts) / (86400 * 1000) if last_trade_ts > 0 else 999

    closed_trades = wins + losses
    win_rate = (wins / closed_trades * 100) if closed_trades > 0 else 0
    profit_factor = (total_profit / total_loss) if total_loss > 0 else (float("inf") if total_profit > 0 else 0)

    # Direction bias from recent fills
    longs = sum(1 for f in fills if "Long" in f.get("dir", ""))
    shorts = sum(1 for f in fills if "Short" in f.get("dir", ""))
    long_pct = (longs / (longs + shorts) * 100) if (longs + shorts) > 0 else 50

    time.sleep(API_DELAY_S)

    return {
        "max_dd": max_dd,
        "max_dd_alltime": max_dd_alltime,
        "max_dd_30d": max_dd_30d,
        "trade_days": trade_days,
        "last_trade_days": last_trade_days,
        "roe_day": roe_data.get("day", 0),
        "roe_week": roe_data.get("week", 0),
        "roe_month": roe_data.get("month", 0),
        "roe_all": roe_data.get("allTime", 0),
        "win_rate": win_rate,
        "profit_factor": profit_factor,
        "closed_trades": closed_trades,
        "long_pct": long_pct,
        "hip3_coins": sorted(hip3_coins),
        "crypto_coins": sorted(crypto_coins)[:10],
        "is_hip3": len(hip3_coins) > len(crypto_coins),
    }


def format_pnl(val: float) -> str:
    """Format PnL with sign and abbreviation."""
    if abs(val) >= 1_000_000:
        return f"${val/1_000_000:+,.1f}M"
    elif abs(val) >= 1_000:
        return f"${val/1_000:+,.0f}K"
    return f"${val:+,.0f}"


def main():
    parser = argparse.ArgumentParser(description="Test trader discovery with ApexLiquid-style filters")
    parser.add_argument("--top", type=int, default=50, help="How many pre-filtered candidates to deep scan")
    parser.add_argument("--hip3", action="store_true", help="Focus on HIP-3 traders")
    parser.add_argument("--relaxed", action="store_true", help="Use relaxed filters (lower ROE thresholds)")
    args = parser.parse_args()

    if args.relaxed:
        FILTERS["min_1d_roe"] = 0.5
        FILTERS["min_7d_roe"] = 5.0
        FILTERS["min_30d_roe"] = 10.0
        FILTERS["min_all_roe"] = 100.0
        FILTERS["max_drawdown"] = 50.0

    print(f"=== Trader Discovery Test ===")
    print(f"Time: {datetime.now(timezone.utc).strftime('%Y-%m-%d %H:%M UTC')}")
    print(f"Filters: Balance>=${FILTERS['min_perps_balance']:,} | "
          f"1D>={FILTERS['min_1d_roe']}% | 7D>={FILTERS['min_7d_roe']}% | "
          f"30D>={FILTERS['min_30d_roe']}% | All>={FILTERS['min_all_roe']}% | "
          f"DD<={FILTERS['max_drawdown']}% | LastTrade<={FILTERS['max_last_trade_days']}d")
    print()

    client = httpx.Client(timeout=30)

    try:
        # Step 1: Leaderboard pre-filter
        rows = fetch_leaderboard(client)
        candidates = pre_filter(rows)

        if not candidates:
            print("No candidates passed pre-filter!")
            return

        # Step 2: Deep scan top N
        scan_count = min(len(candidates), args.top)
        print(f"\nDeep scanning top {scan_count} candidates...")
        print()

        qualified = []
        for i, c in enumerate(candidates[:scan_count]):
            addr = c["address"]
            short_addr = f"{addr[:6]}..{addr[-4:]}"

            scan = deep_scan(client, addr)
            if scan is None:
                print(f"  [{i+1}/{scan_count}] {short_addr} — scan failed")
                continue

            # Apply deep filters
            reasons = []
            if scan["max_dd"] > FILTERS["max_drawdown"]:
                reasons.append(f"DD={scan['max_dd']:.0f}%")
            if scan["trade_days"] < FILTERS["min_trade_days"]:
                reasons.append(f"days={scan['trade_days']:.0f}")
            if scan["last_trade_days"] > FILTERS["max_last_trade_days"]:
                reasons.append(f"last_trade={scan['last_trade_days']:.0f}d ago")

            if reasons:
                print(f"  [{i+1}/{scan_count}] {short_addr} — REJECTED: {', '.join(reasons)}")
                continue

            # Passed all filters
            entry = {**c, **scan}
            qualified.append(entry)
            tag = " [HIP-3]" if scan["is_hip3"] else ""
            print(f"  [{i+1}/{scan_count}] {short_addr} — PASSED{tag} | "
                  f"1D={scan['roe_day']:.1f}% 7D={scan['roe_week']:.1f}% "
                  f"30D={scan['roe_month']:.1f}% | DD={scan['max_dd']:.1f}% | "
                  f"WR={scan['win_rate']:.0f}% PF={min(scan['profit_factor'], 999):.1f}")

        # Step 3: Results
        print(f"\n{'='*80}")
        print(f"RESULTS: {len(qualified)} traders passed all filters")
        print(f"{'='*80}\n")

        if not qualified:
            print("No traders passed all filters. Try --relaxed for looser thresholds.")
            return

        # Sort by 30D ROE
        qualified.sort(key=lambda x: x["roe_month"], reverse=True)

        # Print table
        header = f"{'Trader':<16} {'Balance':>12} {'Trade Time':>10} {'1D ROE':>8} {'7D ROE':>8} {'30D ROE':>9} {'All ROE':>9} {'PNL':>10} {'DD30':>6} {'DDall':>6} {'WR':>5} {'Type':>6}"
        print(header)
        print("-" * len(header))

        crypto_traders = []
        hip3_traders = []

        for t in qualified:
            addr = f"{t['address'][:6]}..{t['address'][-4:]}"
            name = t.get("display_name", "")
            label = f"{addr}" + (f" {name[:8]}" if name else "")
            trade_time = f"{int(t['trade_days'])}d"
            trader_type = "HIP-3" if t["is_hip3"] else "Crypto"

            print(f"{label:<16} ${t['account_value']:>10,.0f} {trade_time:>10} "
                  f"{t['roe_day']:>7.1f}% {t['roe_week']:>7.1f}% {t['roe_month']:>8.1f}% "
                  f"{t['roi_all']:>8.1f}% {format_pnl(t['pnl_all']):>10} "
                  f"{t['max_dd_30d']:>5.1f}% {t['max_dd_alltime']:>5.1f}% {t['win_rate']:>4.0f}% {trader_type:>6}")

            if t["is_hip3"]:
                hip3_traders.append(t)
            else:
                crypto_traders.append(t)

        # Summary
        print(f"\n--- Crypto traders: {len(crypto_traders)} | HIP-3 traders: {len(hip3_traders)} ---")

        if crypto_traders:
            print(f"\nTop 5 Crypto:")
            for i, t in enumerate(crypto_traders[:5]):
                coins = ", ".join(t["crypto_coins"][:5])
                print(f"  {i+1}. {t['address'][:10]}.. — 30D={t['roe_month']:.0f}% DD={t['max_dd']:.0f}% "
                      f"PNL={format_pnl(t['pnl_all'])} | {coins}")

        if hip3_traders:
            print(f"\nTop 3 HIP-3:")
            for i, t in enumerate(hip3_traders[:3]):
                coins = ", ".join(t["hip3_coins"][:5])
                print(f"  {i+1}. {t['address'][:10]}.. — 30D={t['roe_month']:.0f}% DD={t['max_dd']:.0f}% "
                      f"PNL={format_pnl(t['pnl_all'])} | {coins}")

    except httpx.HTTPStatusError as e:
        print(f"HTTP error: {e}")
        sys.exit(1)
    except KeyboardInterrupt:
        print("\nInterrupted")
    finally:
        client.close()


if __name__ == "__main__":
    main()
