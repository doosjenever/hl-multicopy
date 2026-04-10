#!/usr/bin/env python3
"""Full discovery pipeline: scan → select → generate configs → restart bots.

Usage:
    python3 run_discovery.py              # Full run: scan + deploy + restart
    python3 run_discovery.py --scan-only  # Only scan, don't touch bots
    python3 run_discovery.py --deploy     # Skip scan, deploy from existing leaders.yaml

Krabje can run this directly on the VPS.
"""

import os
import signal
import subprocess
import sys
import time
from pathlib import Path

import yaml

BASE_DIR = Path(__file__).parent
CONFIGS_DIR = BASE_DIR / "configs"
LEADERS_FILE = BASE_DIR / "leaders.yaml"
LOGS_DIR = BASE_DIR / "logs"
STATE_DIR = BASE_DIR / "state"

# ── Vos names ────────────────────────────────────────────────
# 5 crypto + 2 mixed/HIP-3 slots
CRYPTO_NAMES = ["sluipvos", "nachtvos", "vuurvos", "poolvos", "woestijnvos"]
MIXED_NAMES = ["stadsvos", "sneeuwvos"]
ALL_NAMES = CRYPTO_NAMES + MIXED_NAMES

MAX_CRYPTO = len(CRYPTO_NAMES)
MAX_MIXED = len(MIXED_NAMES)

# ── Config template ──────────────────────────────────────────
CONFIG_TEMPLATE = """\
# {name} — {description}
# Score: {score} | PF: {pf} | DD90: {dd}% | WR: {wr}% | Perps eq: ${perps_eq}

leader:
  name: "{name}"
  target_wallet: "{address}"
  sub_account: ""
  starting_equity: 200.0
  paper_mode: true
  slippage_cap_pct: 0.5
  debounce_ms: 80
  reconciler_interval_s: 30
  max_drawdown_pct: 25.0
  daily_loss_limit_pct: 10.0

global:
  log_level: "INFO"
"""


def load_leaders() -> list[dict]:
    """Load qualified candidates from leaders.yaml."""
    if not LEADERS_FILE.exists():
        print(f"ERROR: {LEADERS_FILE} not found. Run with --scan first.")
        sys.exit(1)

    data = yaml.safe_load(LEADERS_FILE.read_text()) or {}
    candidates = data.get("candidates", [])
    print(f"Loaded {len(candidates)} candidates from {LEADERS_FILE}")
    return candidates


def select_leaders(candidates: list[dict]) -> dict[str, dict]:
    """Select top crypto and mixed/HIP-3 leaders, assign vos names."""
    # Split into crypto and mixed/HIP-3
    crypto = [c for c in candidates if not c.get("is_hip3", False)]
    mixed = [c for c in candidates if c.get("is_hip3", False)]

    # Already sorted by score from leader_scorer
    selected_crypto = crypto[:MAX_CRYPTO]
    selected_mixed = mixed[:MAX_MIXED]

    # If not enough mixed, fill with more crypto
    if len(selected_mixed) < MAX_MIXED:
        extra_crypto = crypto[MAX_CRYPTO:MAX_CRYPTO + (MAX_MIXED - len(selected_mixed))]
        selected_mixed.extend(extra_crypto)

    assignments = {}
    for name, candidate in zip(CRYPTO_NAMES, selected_crypto):
        assignments[name] = candidate
    for name, candidate in zip(MIXED_NAMES, selected_mixed):
        assignments[name] = candidate

    print(f"\nSelected {len(assignments)} leaders:")
    for name, c in assignments.items():
        coins = ", ".join(c.get("crypto_coins", [])[:3]) or ", ".join(c.get("hip3_coins", [])[:3])
        print(f"  {name:12s} -> {c['address'][:12]}.. "
              f"S={c['score']:.2f} PF={c['profit_factor']} "
              f"DD={c['dd90_pct']}% perps=${c.get('perps_equity', 0):,.0f} "
              f"({coins})")

    return assignments


def generate_configs(assignments: dict[str, dict]):
    """Write YAML config files for each assigned leader."""
    CONFIGS_DIR.mkdir(exist_ok=True)

    for name, c in assignments.items():
        coins = ", ".join(c.get("crypto_coins", [])[:3]) or ", ".join(c.get("hip3_coins", [])[:3])
        description = f"{c.get('display_name') or coins}"

        config_content = CONFIG_TEMPLATE.format(
            name=name,
            description=description,
            score=c["score"],
            pf=c["profit_factor"],
            dd=c["dd90_pct"],
            wr=c["win_rate_pct"],
            perps_eq=f"{c.get('perps_equity', 0):,.0f}",
            address=c["address"],
        )

        config_path = CONFIGS_DIR / f"{name}.yaml"
        config_path.write_text(config_content)
        print(f"  Written: {config_path.name}")

    print(f"Generated {len(assignments)} configs in {CONFIGS_DIR}")


def _supervisor_running() -> bool:
    """Check if supervisor is managing the bots."""
    pid_file = STATE_DIR / ".supervisor.pid"
    if not pid_file.exists():
        return False
    try:
        pid = int(pid_file.read_text().strip())
        os.kill(pid, 0)  # Check if alive
        return True
    except (ValueError, ProcessLookupError, PermissionError):
        return False


def stop_bots():
    """Stop all running multicopy bots — via supervisor if available, direct kill otherwise."""
    print("\nStopping running bots...")

    if _supervisor_running():
        # Tell supervisor to stop all bots
        cmd_file = STATE_DIR / ".supervisor_cmd"
        cmd_file.write_text("stop-all\n")
        print("  Sent stop-all to supervisor")
        # Wait for supervisor to process
        for _ in range(20):
            time.sleep(0.5)
            if not cmd_file.exists():
                break
        time.sleep(2)
        return

    # No supervisor — direct process management
    try:
        result = subprocess.run(
            ["pgrep", "-f", "main.py --config configs/"],
            capture_output=True, text=True
        )
        pids = result.stdout.strip().split("\n")
        pids = [p for p in pids if p]
        if pids:
            # SIGTERM first (graceful)
            for pid in pids:
                try:
                    os.kill(int(pid), signal.SIGTERM)
                except ProcessLookupError:
                    pass
            print(f"  Sent SIGTERM to {len(pids)} processes")

            # Wait up to 10s for graceful exit
            deadline = time.time() + 10
            while time.time() < deadline:
                result = subprocess.run(
                    ["pgrep", "-f", "main.py --config configs/"],
                    capture_output=True, text=True
                )
                remaining = [p for p in result.stdout.strip().split("\n") if p]
                if not remaining:
                    break
                time.sleep(0.5)
            else:
                # Force kill stragglers
                for pid in remaining:
                    try:
                        os.kill(int(pid), signal.SIGKILL)
                    except ProcessLookupError:
                        pass
                print(f"  Force killed {len(remaining)} remaining processes")

            print(f"  All bots stopped")
        else:
            print("  No running bots found")
    except Exception as e:
        print(f"  Error stopping bots: {e}")


def clear_state():
    """Remove all state files for clean start."""
    STATE_DIR.mkdir(exist_ok=True)
    removed = 0
    for f in STATE_DIR.glob("*.json"):
        f.unlink()
        removed += 1
    if removed:
        print(f"  Cleared {removed} state files")


def start_bots(assignments: dict[str, dict]):
    """Start bots — via supervisor command if running, direct otherwise."""
    LOGS_DIR.mkdir(exist_ok=True)

    print("\nStarting bots...")

    if _supervisor_running():
        # Tell supervisor to restart all (it will pick up new configs)
        cmd_file = STATE_DIR / ".supervisor_cmd"
        cmd_file.write_text("restart-all\n")
        print("  Sent restart-all to supervisor")
        for _ in range(20):
            time.sleep(0.5)
            if not cmd_file.exists():
                break
        print(f"  Supervisor restarting {len(assignments)} bots")
        return

    # No supervisor — start directly
    started = 0
    for name in assignments:
        config_path = CONFIGS_DIR / f"{name}.yaml"
        log_path = LOGS_DIR / f"{name}.log"

        if not config_path.exists():
            print(f"  SKIP {name}: config not found")
            continue

        with open(log_path, "a") as logfile:
            subprocess.Popen(
                [sys.executable, "-u", "main.py", "--config", str(config_path)],
                cwd=str(BASE_DIR),
                stdout=logfile,
                stderr=subprocess.STDOUT,
            )
        started += 1
        print(f"  Started: {name}")

    print(f"\n{started} bots started in paper mode")


def run_scan():
    """Run leader_scorer.py discovery scan."""
    print("Running leader discovery scan...\n")
    result = subprocess.run(
        [sys.executable, "leader_scorer.py"],
        cwd=str(BASE_DIR),
    )
    if result.returncode != 0:
        print("ERROR: Discovery scan failed")
        sys.exit(1)
    print()


def main():
    scan_only = "--scan-only" in sys.argv
    deploy_only = "--deploy" in sys.argv

    print("=" * 60)
    print("  Multi-Target Copy Trader — Discovery Pipeline")
    print(f"  Mode: {'scan only' if scan_only else 'deploy only' if deploy_only else 'full run'}")
    print(f"  Time: {time.strftime('%Y-%m-%d %H:%M UTC')}")
    print("=" * 60)

    # Step 1: Scan
    if not deploy_only:
        run_scan()

    if scan_only:
        print("Scan complete. Use --deploy to generate configs and start bots.")
        return

    # Step 2: Select
    candidates = load_leaders()
    if not candidates:
        print("ERROR: No qualified candidates found")
        sys.exit(1)

    assignments = select_leaders(candidates)
    if not assignments:
        print("ERROR: No leaders selected")
        sys.exit(1)

    # Step 3: Generate configs
    generate_configs(assignments)

    # Step 4: Stop old bots, clear state, start new
    stop_bots()
    clear_state()
    start_bots(assignments)

    # Step 5: Wait and verify
    print("\nWaiting 15s for bots to initialize...")
    time.sleep(15)

    print("\n" + "=" * 60)
    print("  STATUS CHECK")
    print("=" * 60)
    for name in assignments:
        log_path = LOGS_DIR / f"{name}.log"
        if not log_path.exists():
            print(f"  {name}: NO LOG")
            continue
        # Find last STATUS line
        status = ""
        for line in log_path.read_text().splitlines():
            if "STATUS:" in line:
                status = line.split("STATUS: ")[-1]
        if status:
            print(f"  {status}")
        else:
            print(f"  {name}: no status yet")

    print("\nDone.")


if __name__ == "__main__":
    main()
