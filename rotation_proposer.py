#!/usr/bin/env python3
"""Rotation proposer — runs Sunday 06:30 UTC (30 min after scorer).

Compares active vossen with leaders.yaml candidates and proposes swaps.
Writes rotation_proposal.yaml + sends Telegram summary to the owner.

Rules:
- Only propose swapping leaders with MIN_TENURE_DAYS+ tenure
- Only swap if new candidate scores significantly higher (delta > SCORE_DELTA)
- Maximum MAX_SWAPS_PER_WEEK swaps per rotation
- Never propose swapping a leader that's still in the top candidates
"""

import json
import os
import time
from pathlib import Path

import httpx
import yaml

# ── Config ──────────────────────────────────────────────────────

BASE_DIR = Path(__file__).parent
CONFIGS_DIR = BASE_DIR / "configs"
STATE_DIR = BASE_DIR / "state"
LEADERS_FILE = BASE_DIR / "leaders.yaml"
PROPOSAL_FILE = BASE_DIR / "rotation_proposal.yaml"

TELEGRAM_BOT_TOKEN = os.environ.get("TELEGRAM_BOT_TOKEN", "")
TELEGRAM_CHAT_ID = os.environ.get("TELEGRAM_CHAT_ID", "")

# Rotation constraints
MIN_TENURE_DAYS = 7              # Minimum days before rotation eligible
SCORE_DELTA = 0.05               # New candidate must score this much higher
MAX_SWAPS_PER_WEEK = 2           # Don't churn the whole portfolio


def send_telegram(message: str):
    if not TELEGRAM_BOT_TOKEN or not TELEGRAM_CHAT_ID:
        print(f"[ALERT] {message}")
        return
    try:
        httpx.post(
            f"https://api.telegram.org/bot{TELEGRAM_BOT_TOKEN}/sendMessage",
            json={"chat_id": TELEGRAM_CHAT_ID, "text": message,
                  "parse_mode": "HTML", "disable_web_page_preview": True},
            timeout=10,
        )
    except Exception as e:
        print(f"Telegram send failed: {e}")


def get_current_leaders() -> dict[str, dict]:
    """Load all active leader configs. Returns {name: config}."""
    leaders = {}
    for f in CONFIGS_DIR.glob("*.yaml"):
        if f.name == "example.yaml":
            continue
        try:
            config = yaml.safe_load(f.read_text())
            leader = config.get("leader", {})
            wallet = leader.get("target_wallet", "")
            if wallet:
                leaders[f.stem] = {
                    "name": f.stem,
                    "address": wallet,
                    "config": config,
                }
        except Exception:
            continue
    return leaders


def get_leader_tenure(name: str) -> float:
    """Get days since leader was assigned (from state file timestamp or equity log)."""
    state_path = STATE_DIR / f"{name}.json"
    if state_path.exists():
        try:
            state = json.loads(state_path.read_text())
            ts = state.get("timestamp", 0)
            if ts > 0:
                return (time.time() - ts) / 86400
        except Exception:
            pass

    # Check for pre_swap backups to estimate tenure
    backups = sorted(STATE_DIR.glob(f"{name}.json.pre_swap_*"))
    if backups:
        # Latest swap timestamp from filename
        try:
            swap_ts = int(backups[-1].suffix.split("_")[-1])
            return (time.time() - swap_ts) / 86400
        except Exception:
            pass

    # Config file modification time as fallback
    config_path = CONFIGS_DIR / f"{name}.yaml"
    if config_path.exists():
        return (time.time() - config_path.stat().st_mtime) / 86400

    return 0


def main():
    print(f"Rotation Proposer — {time.strftime('%Y-%m-%d %H:%M UTC')}")

    # Load new candidates
    if not LEADERS_FILE.exists():
        print(f"No {LEADERS_FILE} found. Run leader_scorer.py first.")
        return

    leaders_data = yaml.safe_load(LEADERS_FILE.read_text())
    candidates = leaders_data.get("candidates", [])
    if not candidates:
        print("No candidates in leaders.yaml")
        return

    print(f"Candidates: {len(candidates)} from scorer")

    # Load current vossen
    current = get_current_leaders()
    if not current:
        print("No active leaders configured")
        return

    print(f"Active leaders: {', '.join(current.keys())}")

    # Build candidate lookup by address
    candidate_by_addr = {c["address"]: c for c in candidates}
    candidate_addrs = set(candidate_by_addr.keys())
    current_addrs = {v["address"] for v in current.values()}

    # Build active coins set for correlation penalty
    active_coins = set()
    for info in current.values():
        addr = info["address"]
        if addr in candidate_by_addr:
            c = candidate_by_addr[addr]
            active_coins.update(c.get("crypto_coins", [])[:3])
            active_coins.update(c.get("hip3_coins", [])[:3])

    # Evaluate each current leader
    proposed_swaps = []
    retained = []

    for name, info in current.items():
        addr = info["address"]
        tenure = get_leader_tenure(name)

        # Still in candidates? Keep
        if addr in candidate_addrs:
            retained.append({
                "name": name,
                "reason": f"still ranked (score={candidate_by_addr[addr].get('score', '?')})",
            })
            continue

        # Too new to rotate?
        if tenure < MIN_TENURE_DAYS:
            retained.append({
                "name": name,
                "reason": f"tenure {tenure:.0f}d < {MIN_TENURE_DAYS}d minimum",
            })
            continue

        # Find best replacement (not already in use)
        best_replacement = None
        for c in candidates:
            if c["address"] in current_addrs:
                continue  # Already following this one
            if c["address"] in {s["new_address"] for s in proposed_swaps}:
                continue  # Already proposed for another swap
            
            current_score = candidate_by_addr.get(addr, {}).get("score", 0)
            
            # P4: Correlation penalty (-0.02 per overlapping top coin)
            cand_coins = set(c.get("crypto_coins", [])[:3] + c.get("hip3_coins", [])[:3])
            overlap = len(cand_coins.intersection(active_coins))
            adjusted_score = c.get("score", 0) - (overlap * 0.02)
            
            # P2: Score Delta Check
            if adjusted_score <= current_score + SCORE_DELTA:
                continue # Not significantly better after correlation penalty

            best_replacement = c
            active_coins.update(cand_coins)
            break

        if best_replacement:
            proposed_swaps.append({
                "current_name": name,
                "current_address": addr,
                "new_address": best_replacement["address"],
                "new_display_name": best_replacement.get("display_name", ""),
                "new_score": best_replacement.get("score", 0),
                "reason": f"replaced by higher scorer (adj_score={adjusted_score:.3f} > current={current_score:.3f} + {SCORE_DELTA})",
                "tenure_days": round(tenure, 1),
            })
        else:
            retained.append({
                "name": name,
                "reason": "not in candidates, but no replacement available",
            })

    # Cap at MAX_SWAPS_PER_WEEK
    if len(proposed_swaps) > MAX_SWAPS_PER_WEEK:
        # Keep the ones with longest tenure (most overdue for rotation)
        proposed_swaps.sort(key=lambda s: s["tenure_days"], reverse=True)
        deferred = proposed_swaps[MAX_SWAPS_PER_WEEK:]
        proposed_swaps = proposed_swaps[:MAX_SWAPS_PER_WEEK]
        for d in deferred:
            retained.append({
                "name": d["current_name"],
                "reason": f"deferred (max {MAX_SWAPS_PER_WEEK} swaps/week)",
            })

    # Write proposal
    proposal = {
        "generated": time.strftime("%Y-%m-%d %H:%M UTC"),
        "proposed_swaps": proposed_swaps,
        "retain": retained,
    }

    PROPOSAL_FILE.write_text(yaml.dump(proposal, default_flow_style=False, sort_keys=False))
    print(f"\nProposal written to {PROPOSAL_FILE}")

    # Summary
    print(f"\nProposed swaps: {len(proposed_swaps)}")
    for s in proposed_swaps:
        print(f"  {s['current_name']}: {s['current_address'][:10]}... -> "
              f"{s['new_address'][:10]}... (score={s['new_score']:.3f}, tenure={s['tenure_days']:.0f}d)")

    print(f"\nRetained: {len(retained)}")
    for r in retained:
        print(f"  {r['name']}: {r['reason']}")

    # Telegram
    lines = [f"<b>Weekly Rotation Proposal</b>"]

    if proposed_swaps:
        lines.append(f"\n<b>Proposed swaps ({len(proposed_swaps)}):</b>")
        for s in proposed_swaps:
            lines.append(
                f"  {s['current_name']}: {s['current_address'][:10]}... → "
                f"{s['new_address'][:10]}... (score={s['new_score']:.2f})"
            )
        lines.append(f"\nApprove: <code>python orchestrator.py approve-rotation</code>")
    else:
        lines.append("\nNo swaps needed — all leaders still ranked.")

    if retained:
        lines.append(f"\n<b>Retained ({len(retained)}):</b>")
        for r in retained:
            lines.append(f"  {r['name']}: {r['reason']}")

    send_telegram("\n".join(lines))


if __name__ == "__main__":
    main()
