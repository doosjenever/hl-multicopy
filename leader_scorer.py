#!/usr/bin/env python3
"""Leader scorer — runs weekly (Sunday 06:00 UTC) via cron.

Two-step pipeline:
1. Pre-filter: Leaderboard API (1 call) -> filter on AV, ROI, recent ROE
2. Deep scan: Top N candidates -> portfolio history + fills for scoring

Output: leaders.yaml with ranked candidates + Telegram summary.
"""

import argparse
import json
import math
import os
import time
import sys
from datetime import datetime, timezone
from pathlib import Path

import httpx
import yaml

# ── Config ──────────────────────────────────────────────────────

BASE_DIR = Path(__file__).parent
OUTPUT_FILE = BASE_DIR / "leaders.yaml"
TELEGRAM_BOT_TOKEN = os.environ.get("TELEGRAM_BOT_TOKEN", "")
TELEGRAM_CHAT_ID = os.environ.get("TELEGRAM_CHAT_ID", "")

# API endpoints
LEADERBOARD_URL = "https://stats-data.hyperliquid.xyz/Mainnet/leaderboard"
HL_API_URL = "https://api.hyperliquid.xyz"

# ── Filters (tested to match ApexLiquid-quality output) ────────

# Pre-filter (leaderboard data, no extra API calls)
MIN_PERPS_BALANCE = 25_000       # $25K minimum AV
MIN_ALL_ROE = 100.0              # 100% all-time ROI (doubled account)
MIN_1D_ROE = 0.5                 # 0.5% today (just green)
MIN_7D_ROE = 5.0                 # 5% this week
MIN_30D_ROE = 15.0               # 15% this month

# Deep scan filters
MAX_DRAWDOWN = 75.0              # 75% max all-time DD (bot has own DD protection)
MIN_TRADE_DAYS = 30              # 30 days minimum active
MAX_LAST_TRADE_DAYS = 2          # Most recent voluntary trade must be < 2d ago
MIN_CLOSED_TRADES = 20           # Minimum closed trades
MIN_TRADES_LAST_7D = 5           # Must have at least 5 voluntary trades in last 7d
MIN_ACTIVE_DAYS_LAST_14D = 3     # Must have traded on at least 3 distinct days in last 14d

# Rate limiting
API_DELAY_S = 1.0
API_RETRY_429 = 3
API_BACKOFF_S = 10
DEEP_SCAN_COUNT = 80             # How many pre-filtered candidates to deep scan


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


def _api_post(client: httpx.Client, payload: dict) -> dict | list | None:
    """POST to HL API with retry on 429."""
    for attempt in range(API_RETRY_429 + 1):
        try:
            resp = client.post(f"{HL_API_URL}/info", json=payload, timeout=15)
            if resp.status_code == 429:
                wait = API_BACKOFF_S * (attempt + 1)
                print(f"  429 rate limited, waiting {wait}s (attempt {attempt+1})")
                time.sleep(wait)
                continue
            resp.raise_for_status()
            return resp.json()
        except httpx.HTTPStatusError:
            raise
        except Exception as e:
            print(f"  API error: {e}")
            return None
    print(f"  Gave up after {API_RETRY_429} retries")
    return None


def fetch_leaderboard(client: httpx.Client) -> list[dict]:
    """Fetch full leaderboard from stats endpoint."""
    resp = client.get(LEADERBOARD_URL, timeout=30)
    resp.raise_for_status()
    data = resp.json()
    rows = data.get("leaderboardRows", data) if isinstance(data, dict) else data
    print(f"Leaderboard: {len(rows)} traders")
    return rows


def _extract_window(row: dict, window: str) -> dict:
    """Extract pnl/roi from windowPerformances."""
    for entry in row.get("windowPerformances", []):
        if isinstance(entry, list) and len(entry) == 2 and entry[0] == window:
            return entry[1]
    return {}


def pre_filter(rows: list[dict]) -> list[dict]:
    """Fast pre-filter on leaderboard data (no extra API calls)."""
    av_pass = 0
    candidates = []
    for row in rows:
        try:
            account_value = float(row.get("accountValue", 0))
            perf_all = _extract_window(row, "allTime")
            roi_all = float(perf_all.get("roi", 0)) * 100
            pnl_all = float(perf_all.get("pnl", 0))

            if account_value < MIN_PERPS_BALANCE:
                continue
            if roi_all < MIN_ALL_ROE:
                continue
            if pnl_all <= 0:
                continue
            av_pass += 1

            # Window ROIs from leaderboard
            perf_day = _extract_window(row, "day")
            perf_week = _extract_window(row, "week")
            perf_month = _extract_window(row, "month")

            roi_day = float(perf_day.get("roi", 0)) * 100
            roi_week = float(perf_week.get("roi", 0)) * 100
            roi_month = float(perf_month.get("roi", 0)) * 100

            if roi_day < MIN_1D_ROE:
                continue
            if roi_week < MIN_7D_ROE:
                continue
            if roi_month < MIN_30D_ROE:
                continue

            candidates.append({
                "address": row.get("ethAddress", ""),
                "display_name": row.get("displayName") or "",
                "account_value": account_value,
                "roi_all": roi_all,
                "pnl_all": pnl_all,
                "roi_day": roi_day,
                "roi_week": roi_week,
                "roi_month": roi_month,
            })
        except (ValueError, TypeError):
            continue

    candidates.sort(key=lambda x: x["roi_month"], reverse=True)
    print(f"Pre-filtered: {len(candidates)} candidates (from {av_pass} with AV+ROI)")
    return candidates


def deep_scan_trader(client: httpx.Client, address: str) -> dict | None:
    """Deep scan: portfolio (drawdown, ROE) + fills (last trade, win rate)."""

    # 1. Portfolio — drawdown + ROE per window
    portfolio = _api_post(client, {"type": "portfolio", "user": address})
    if portfolio is None:
        return None

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

    # Calculate max drawdown (all-time) and 30D
    now_ms = int(time.time() * 1000)
    thirty_days_ms = 30 * 86400 * 1000

    peak = 0.0
    max_dd_alltime = 0.0
    recent_vals = []
    for point in av_history:
        ts = point[0] if isinstance(point, list) else point.get("time", 0)
        val = float(point[1] if isinstance(point, list) else point.get("accountValue", 0))
        if val > peak:
            peak = val
        if peak > 0:
            dd = (peak - val) / peak * 100
            max_dd_alltime = max(max_dd_alltime, dd)
        if ts >= now_ms - thirty_days_ms:
            recent_vals.append(val)

    max_dd_30d = 0.0
    if recent_vals:
        peak_30d = recent_vals[0]
        for val in recent_vals:
            if val > peak_30d:
                peak_30d = val
            if peak_30d > 0:
                dd = (peak_30d - val) / peak_30d * 100
                max_dd_30d = max(max_dd_30d, dd)

    # Trade time (days since first entry)
    first_ts = av_history[0][0] if isinstance(av_history[0], list) else av_history[0].get("time", 0)
    trade_days = (now_ms - first_ts) / (86400 * 1000)

    # ROE per window from portfolio pnlHistory
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
            if start_av >= 100:
                roe_data[window_name] = (end_pnl - start_pnl) / start_av * 100

    time.sleep(API_DELAY_S)

    # 2. Recent fills — last trade + win rate + coin categorization
    fills = _api_post(client, {"type": "userFills", "user": address})
    if fills is None:
        return None

    last_trade_ts = 0
    wins = 0
    losses = 0
    total_profit = 0.0
    total_loss = 0.0
    hip3_coins = set()
    crypto_coins = set()

    # Calculate True PF, WinRate and Sharpe from Portfolio equity curve (pnlHistory) instead of partial fills
    pnl_hist_30d = windows.get("perpMonth", {}).get("pnlHistory", [])
    true_wins = 0
    true_losses = 0
    true_profit = 0.0
    true_loss = 0.0
    returns = []
    
    if len(pnl_hist_30d) > 1:
        prev_pnl = float(pnl_hist_30d[0][1] if isinstance(pnl_hist_30d[0], list) else pnl_hist_30d[0].get("pnl", 0))
        for point in pnl_hist_30d[1:]:
            curr_pnl = float(point[1] if isinstance(point, list) else point.get("pnl", 0))
            delta = curr_pnl - prev_pnl
            if delta > 1.0:  # Filter dust
                true_wins += 1
                true_profit += delta
            elif delta < -1.0:
                true_losses += 1
                true_loss += abs(delta)
            returns.append(delta)
            prev_pnl = curr_pnl

    sharpe = 0.0
    if len(returns) > 2:
        mean_ret = sum(returns) / len(returns)
        variance = sum((r - mean_ret) ** 2 for r in returns) / len(returns)
        std_dev = math.sqrt(variance)
        if std_dev > 0:
            sharpe = (mean_ret / std_dev) * math.sqrt(len(returns))

    # Activity windows: only count VOLUNTARY trades (exclude liquidations).
    # A dead wallet can still have recent fills via liquidations or stale
    # limit-order partial fills, which falsely passes a "last trade < 2d" check.
    seven_days_ms = 7 * 86400 * 1000
    fourteen_days_ms = 14 * 86400 * 1000
    trades_last_7d = 0
    active_day_buckets: set = set()

    for fill in fills:
        ts = fill.get("time", 0)
        direction = fill.get("dir", "")
        is_voluntary = direction in ("Open Long", "Close Long", "Open Short", "Close Short")

        if is_voluntary and ts > last_trade_ts:
            last_trade_ts = ts
        if is_voluntary and ts >= now_ms - seven_days_ms:
            trades_last_7d += 1
        if is_voluntary and ts >= now_ms - fourteen_days_ms:
            active_day_buckets.add(ts // (86400 * 1000))

        coin = fill.get("coin", "")
        if coin.startswith("@") or ":" in coin:
            hip3_coins.add(coin)
        elif coin and coin != "USDC":
            crypto_coins.add(coin)

    active_days_last_14d = len(active_day_buckets)
    last_trade_days = (now_ms - last_trade_ts) / (86400 * 1000) if last_trade_ts > 0 else 999
    closed_trades = true_wins + true_losses
    win_rate = (true_wins / closed_trades * 100) if closed_trades > 0 else 0

    if true_loss > 0:
        profit_factor = true_profit / true_loss
    elif true_profit > 0:
        profit_factor = float("inf")
    else:
        profit_factor = 0.0

    # Direction bias
    longs = sum(1 for f in fills if "Long" in f.get("dir", ""))
    shorts = sum(1 for f in fills if "Short" in f.get("dir", ""))
    long_pct = (longs / (longs + shorts) * 100) if (longs + shorts) > 0 else 50

    time.sleep(API_DELAY_S)

    return {
        "max_dd_alltime": max_dd_alltime,
        "max_dd_30d": max_dd_30d,
        "trade_days": trade_days,
        "last_trade_days": last_trade_days,
        "trades_last_7d": trades_last_7d,
        "active_days_last_14d": active_days_last_14d,
        "roe_day": roe_data.get("day", 0),
        "roe_week": roe_data.get("week", 0),
        "roe_month": roe_data.get("month", 0),
        "roe_all": roe_data.get("allTime", 0),
        "win_rate": win_rate,
        "sharpe": sharpe,
        "profit_factor": profit_factor,
        "closed_trades": closed_trades,
        "long_pct": long_pct,
        "hip3_coins": sorted(hip3_coins),
        "crypto_coins": sorted(crypto_coins)[:10],
        "is_hip3": len(hip3_coins) > len(crypto_coins),
    }


def score_trader(candidate: dict, scan: dict) -> float:
    """Calculate composite score for a trader."""
    # 30D ROE score (capped at 500%)
    roe_30d_score = min(scan["roe_month"] / 500, 1.0) if scan["roe_month"] > 0 else 0

    # DD score: lower is better (0% = 1.0, 75% = 0.0)
    dd_score = max(0, 1 - scan["max_dd_alltime"] / MAX_DRAWDOWN)

    # PF score (capped at 10 for true PF)
    pf = scan["profit_factor"] if not math.isinf(scan["profit_factor"]) else 10
    pf_score = min(pf / 10, 1.0)

    # Win rate (0-100% -> 0-1)
    wr_score = scan["win_rate"] / 100.0

    # Sharpe score (capped at 5.0)
    sharpe = scan.get("sharpe", 0.0)
    sharpe_score = min(max(sharpe, 0) / 5.0, 1.0)

    # 7D ROE momentum (capped at 100%)
    momentum = min(scan["roe_week"] / 100, 1.0) if scan["roe_week"] > 0 else 0

    # Confidence: more trades + more days = higher
    trade_confidence = min(scan["closed_trades"] / 200, 1.0)
    time_confidence = min(scan["trade_days"] / 180, 1.0)
    confidence = (trade_confidence + time_confidence) / 2

    # New HFT Quant Weights
    score = (
        0.25 * roe_30d_score +
        0.20 * dd_score +
        0.15 * sharpe_score +
        0.10 * pf_score +
        0.10 * wr_score +
        0.10 * momentum +
        0.10 * confidence
    )
    return round(score, 4)


def format_pnl(val: float) -> str:
    if abs(val) >= 1_000_000:
        return f"${val/1_000_000:+,.1f}M"
    elif abs(val) >= 1_000:
        return f"${val/1_000:+,.0f}K"
    return f"${val:+,.0f}"


def main():
    parser = argparse.ArgumentParser(description="Leader scorer — weekly trader discovery")
    parser.add_argument("--top", type=int, default=DEEP_SCAN_COUNT,
                        help=f"How many pre-filtered to deep scan (default {DEEP_SCAN_COUNT})")
    parser.add_argument("--dry-run", action="store_true", help="Don't save to leaders.yaml")
    args = parser.parse_args()

    print(f"Leader Scorer starting at {time.strftime('%Y-%m-%d %H:%M UTC')}")
    print(f"Filters: Balance>=${MIN_PERPS_BALANCE:,} | "
          f"1D>={MIN_1D_ROE}% | 7D>={MIN_7D_ROE}% | 30D>={MIN_30D_ROE}% | "
          f"All>={MIN_ALL_ROE}% | DD<={MAX_DRAWDOWN}% | LastTrade<={MAX_LAST_TRADE_DAYS}d")
    print()

    client = httpx.Client(timeout=30)

    try:
        # Step 1: Pre-filter
        rows = fetch_leaderboard(client)
        candidates = pre_filter(rows)

        if not candidates:
            print("No candidates passed pre-filter!")
            send_telegram("<b>Leader Scorer:</b> 0 candidates passed pre-filter")
            return

        # Step 2: Deep scan top N
        scan_count = min(len(candidates), args.top)
        print(f"\nDeep scanning top {scan_count} candidates...")

        qualified = []

        for i, c in enumerate(candidates[:scan_count]):
            addr = c["address"]
            short_addr = f"{addr[:6]}..{addr[-4:]}"

            scan = deep_scan_trader(client, addr)
            if scan is None:
                print(f"  [{i+1}/{scan_count}] {short_addr} — scan failed")
                continue

            # Apply deep filters
            reasons = []
            if scan["max_dd_alltime"] > MAX_DRAWDOWN:
                reasons.append(f"DD={scan['max_dd_alltime']:.0f}%")
            if scan["trade_days"] < MIN_TRADE_DAYS:
                reasons.append(f"days={scan['trade_days']:.0f}")
            if scan["last_trade_days"] > MAX_LAST_TRADE_DAYS:
                reasons.append(f"last_trade={scan['last_trade_days']:.0f}d ago")
            if scan["closed_trades"] < MIN_CLOSED_TRADES:
                reasons.append(f"trades={scan['closed_trades']}")
            if scan["trades_last_7d"] < MIN_TRADES_LAST_7D:
                reasons.append(f"trades_7d={scan['trades_last_7d']}")
            if scan["active_days_last_14d"] < MIN_ACTIVE_DAYS_LAST_14D:
                reasons.append(f"active_days_14d={scan['active_days_last_14d']}")

            if reasons:
                print(f"  [{i+1}/{scan_count}] {short_addr} — REJECTED: {', '.join(reasons)}")
                continue

            # Score and add
            score = score_trader(c, scan)
            tag = " [HIP-3]" if scan["is_hip3"] else ""
            print(f"  [{i+1}/{scan_count}] {short_addr} — PASSED{tag} score={score:.3f} | "
                  f"30D={scan['roe_month']:.0f}% DD={scan['max_dd_alltime']:.0f}% "
                  f"WR={scan['win_rate']:.0f}% PF={min(scan['profit_factor'], 999):.1f}")

            qualified.append({
                "address": addr,
                "display_name": c.get("display_name", ""),
                "score": score,
                "account_value": round(c["account_value"], 2),
                "roi_all_pct": round(c["roi_all"], 1),
                "pnl_all": round(c["pnl_all"], 2),
                "roe_day_pct": round(scan["roe_day"], 1),
                "roe_week_pct": round(scan["roe_week"], 1),
                "roe_month_pct": round(scan["roe_month"], 1),
                "dd_alltime_pct": round(scan["max_dd_alltime"], 1),
                "dd_30d_pct": round(scan["max_dd_30d"], 1),
                "profit_factor": round(min(scan["profit_factor"], 9999), 2),
                "win_rate_pct": round(scan["win_rate"], 1),
                "closed_trades": scan["closed_trades"],
                "trade_days": int(scan["trade_days"]),
                "last_trade_days": round(scan["last_trade_days"], 1),
                "trades_last_7d": scan["trades_last_7d"],
                "active_days_last_14d": scan["active_days_last_14d"],
                "long_pct": round(scan["long_pct"], 1),
                "is_hip3": scan["is_hip3"],
                "hip3_coins": scan["hip3_coins"][:10],
                "crypto_coins": scan["crypto_coins"][:10],
            })

        # Sort by score
        qualified.sort(key=lambda x: x["score"], reverse=True)

        # Results
        print(f"\n{'='*80}")
        print(f"RESULTS: {len(qualified)} traders passed all filters")
        print(f"{'='*80}\n")

        crypto = [t for t in qualified if not t["is_hip3"]]
        hip3 = [t for t in qualified if t["is_hip3"]]

        if crypto:
            print(f"Top Crypto ({len(crypto)}):")
            for i, t in enumerate(crypto[:10]):
                coins = ", ".join(t["crypto_coins"][:5])
                print(f"  {i+1}. {t['address'][:10]}.. S={t['score']:.3f} "
                      f"30D={t['roe_month_pct']:.0f}% DD={t['dd_alltime_pct']:.0f}% "
                      f"PNL={format_pnl(t['pnl_all'])} | {coins}")

        if hip3:
            print(f"\nTop HIP-3 ({len(hip3)}):")
            for i, t in enumerate(hip3[:5]):
                coins = ", ".join(t["hip3_coins"][:5])
                print(f"  {i+1}. {t['address'][:10]}.. S={t['score']:.3f} "
                      f"30D={t['roe_month_pct']:.0f}% DD={t['dd_alltime_pct']:.0f}% "
                      f"PNL={format_pnl(t['pnl_all'])} | {coins}")

        # Step 3: Save to YAML
        if args.dry_run:
            print("\n[DRY RUN] Not saving to leaders.yaml")
        else:
            prev_leaders = set()
            if OUTPUT_FILE.exists():
                try:
                    prev = yaml.safe_load(OUTPUT_FILE.read_text()) or {}
                    prev_leaders = {t["address"] for t in prev.get("candidates", [])}
                except Exception:
                    pass

            output = {
                "generated": time.strftime("%Y-%m-%d %H:%M UTC"),
                "filters": {
                    "min_perps_balance": MIN_PERPS_BALANCE,
                    "min_all_roe": MIN_ALL_ROE,
                    "min_1d_roe": MIN_1D_ROE,
                    "min_7d_roe": MIN_7D_ROE,
                    "min_30d_roe": MIN_30D_ROE,
                    "max_drawdown": MAX_DRAWDOWN,
                    "max_last_trade_days": MAX_LAST_TRADE_DAYS,
                    "min_trade_days": MIN_TRADE_DAYS,
                    "min_closed_trades": MIN_CLOSED_TRADES,
                    "min_trades_last_7d": MIN_TRADES_LAST_7D,
                    "min_active_days_last_14d": MIN_ACTIVE_DAYS_LAST_14D,
                },
                "stats": {
                    "total_leaderboard": len(rows),
                    "pre_filtered": len(candidates),
                    "deep_scanned": scan_count,
                    "qualified": len(qualified),
                },
                "candidates": qualified,
            }

            OUTPUT_FILE.write_text(yaml.dump(output, default_flow_style=False, sort_keys=False))
            print(f"\nResults saved to {OUTPUT_FILE}")

            # Step 4: Telegram summary
            new_leaders = {t["address"] for t in qualified} - prev_leaders
            lost_leaders = prev_leaders - {t["address"] for t in qualified}

            lines = [
                f"<b>Weekly Leader Scan</b>",
                f"Scanned: {len(rows)} -> {len(candidates)} -> {scan_count} -> {len(qualified)}",
                "",
            ]

            if crypto[:5]:
                lines.append("<b>Top Crypto:</b>")
                for t in crypto[:5]:
                    lines.append(
                        f"  {t['address'][:10]}.. "
                        f"S={t['score']:.2f} 30D={t['roe_month_pct']:.0f}% "
                        f"DD={t['dd_alltime_pct']:.0f}% PNL={format_pnl(t['pnl_all'])}"
                    )

            if hip3[:3]:
                lines.append("<b>Top HIP-3:</b>")
                for t in hip3[:3]:
                    coins = ", ".join(t["hip3_coins"][:3])
                    lines.append(
                        f"  {t['address'][:10]}.. "
                        f"S={t['score']:.2f} 30D={t['roe_month_pct']:.0f}% ({coins})"
                    )

            if new_leaders:
                lines.append(f"\nNew: {len(new_leaders)} traders entered")
            if lost_leaders:
                lines.append(f"Dropped: {len(lost_leaders)} traders fell off")

            send_telegram("\n".join(lines))

    except httpx.HTTPStatusError as e:
        if e.response.status_code == 429:
            print("Rate limited! Waiting 60s...")
            time.sleep(60)
        else:
            print(f"HTTP error: {e}")
            sys.exit(1)
    except Exception as e:
        print(f"Fatal error: {e}")
        send_telegram(f"<b>Leader Scorer FAILED</b>\n{e}")
        sys.exit(1)
    finally:
        client.close()


if __name__ == "__main__":
    main()
