#!/usr/bin/env python3
"""Orchestrator — manages the lifecycle of multi-target copy traders.

Responsibilities:
1. Generate leader configs from leaders.yaml (discovery output)
2. Start/stop/restart via supervisor
3. Leader swap (change target wallet, reconciler handles orphan positions)
4. Approve-rotation (execute weekly rotation proposals)
5. Create sub-accounts + capital transfers (live mode)
6. Process transfer requests from portfolio_tracker (profit-taking)
7. Withdraw capital from sub-accounts

Usage:
    python orchestrator.py status                          # Show all leaders
    python orchestrator.py add <address> <name>            # Add new leader
    python orchestrator.py remove <name>                   # Remove leader
    python orchestrator.py start <name>                    # Start leader
    python orchestrator.py stop <name>                     # Stop leader
    python orchestrator.py start-all / stop-all
    python orchestrator.py generate                        # From leaders.yaml
    python orchestrator.py swap <name> <new_address>       # Swap leader target
    python orchestrator.py approve-rotation [--proposal X] # Execute rotation
    python orchestrator.py setup-subaccount <name>         # Create sub-account
    python orchestrator.py process-transfers               # Execute skim requests
    python orchestrator.py withdraw <name>                 # Withdraw capital
    python orchestrator.py rebalance                       # Show capital balance
"""

import argparse
import json
import os
import subprocess
import sys
import time
from pathlib import Path

import httpx
import yaml

# ── Config ──────────────────────────────────────────────────────

BASE_DIR = Path(__file__).parent
CONFIGS_DIR = BASE_DIR / "configs"
STATE_DIR = BASE_DIR / "state"
LEADERS_FILE = BASE_DIR / "leaders.yaml"
ALLOCATION_FILE = BASE_DIR / "allocation.yaml"

# Defaults for new leaders
DEFAULT_EQUITY = 200.0           # Paper mode starting equity ($200 per leader)
DEFAULT_PAPER_MODE = True
DEFAULT_SLIPPAGE = 0.5
DEFAULT_DEBOUNCE_MS = 80
DEFAULT_RECONCILER_S = 30
DEFAULT_MAX_DD = 25.0
DEFAULT_DAILY_LOSS = 10.0

# Allocation constraints
MAX_LEADERS = 10                 # Maximum simultaneous leaders
MIN_ALLOCATION_PCT = 5.0         # Floor: min 5% of total capital per leader
MAX_ALLOCATION_PCT = 30.0        # Ceiling: max 30% per leader
MIN_TENURE_DAYS = 7              # Minimum days before a leader can be rotated out
SWITCH_COST_PCT = 0.5            # Don't rotate if fees > 0.5% of allocation

TRANSFER_DIR = STATE_DIR / "transfer_requests"
ROTATION_FILE = BASE_DIR / "rotation_proposal.yaml"

HEARTBEAT_FILE = STATE_DIR / ".supervisor_heartbeat"
CMD_FILE = STATE_DIR / ".supervisor_cmd"
PID_FILE = STATE_DIR / ".supervisor.pid"

# Telegram
TELEGRAM_BOT_TOKEN = os.environ.get("TELEGRAM_BOT_TOKEN", "")
TELEGRAM_CHAT_ID = os.environ.get("TELEGRAM_CHAT_ID", "")

HL_API_URL = "https://api.hyperliquid.xyz"


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


def _init_exchange(account_address: str = None):
    """Initialize HL SDK Exchange for orchestrator operations.

    Args:
        account_address: Sub-account address to trade on behalf of.
                         Required for market_close on sub-accounts.
    """
    from eth_account import Account
    from hyperliquid.exchange import Exchange

    private_key = os.environ.get("HL_PRIVATE_KEY", "")
    if not private_key:
        raise RuntimeError("HL_PRIVATE_KEY not set — cannot execute live operations")

    wallet = Account.from_key(private_key)
    return Exchange(wallet=wallet, base_url=HL_API_URL, account_address=account_address)


def _supervisor_running() -> bool:
    """Check if the supervisor is alive."""
    if not PID_FILE.exists():
        return False
    try:
        pid = int(PID_FILE.read_text().strip())
        os.kill(pid, 0)
        return True
    except (ValueError, ProcessLookupError, PermissionError):
        return False


def _send_supervisor_cmd(cmd: str) -> bool:
    """Send a command to the supervisor via command file."""
    if not _supervisor_running():
        print("  Supervisor not running")
        return False
    try:
        tmp = CMD_FILE.with_suffix(".tmp")
        tmp.write_text(cmd + "\n")
        os.replace(str(tmp), str(CMD_FILE))
        # Wait for processing
        for _ in range(20):
            time.sleep(0.5)
            if not CMD_FILE.exists():
                return True
        return True  # Sent, but may not be processed yet
    except Exception as e:
        print(f"  Command error: {e}")
        return False


def is_running(name: str) -> bool:
    """Check if a leader's bot is running (via supervisor heartbeat)."""
    if not HEARTBEAT_FILE.exists():
        return False
    try:
        hb = json.loads(HEARTBEAT_FILE.read_text())
        bot = hb.get("bots", {}).get(name, {})
        return bot.get("running", False)
    except Exception:
        return False


def get_configured_leaders() -> list[str]:
    """List all leader names that have config files."""
    leaders = []
    for f in CONFIGS_DIR.glob("*.yaml"):
        if f.name == "example.yaml":
            continue
        leaders.append(f.stem)
    return sorted(leaders)


def load_leader_config(name: str) -> dict | None:
    """Load a leader's YAML config."""
    path = CONFIGS_DIR / f"{name}.yaml"
    if not path.exists():
        return None
    try:
        return yaml.safe_load(path.read_text())
    except Exception:
        return None


def load_leader_state(name: str) -> dict | None:
    """Load a leader's current state."""
    path = STATE_DIR / f"{name}.json"
    if not path.exists():
        return None
    try:
        return json.loads(path.read_text())
    except Exception:
        return None


def load_allocation() -> dict:
    """Load capital allocation config."""
    if ALLOCATION_FILE.exists():
        try:
            return yaml.safe_load(ALLOCATION_FILE.read_text()) or {}
        except Exception:
            pass
    return {}


def save_allocation(alloc: dict):
    ALLOCATION_FILE.write_text(yaml.dump(alloc, default_flow_style=False))


# ── Commands ────────────────────────────────────────────────────

def cmd_status():
    """Show status of all configured leaders."""
    leaders = get_configured_leaders()
    if not leaders:
        print("No leaders configured. Use 'orchestrator.py add <address> <name>' to add one.")
        return

    print(f"{'Name':<12} {'Mode':<6} {'Running':<8} {'Equity':>10} {'PnL%':>8} {'DD%':>6} {'Trades':>7} {'Halted'}")
    print("-" * 75)

    for name in leaders:
        config = load_leader_config(name)
        state = load_leader_state(name)
        running = is_running(name)

        mode = "paper" if config and config.get("leader", {}).get("paper_mode", True) else "live"
        running_str = "YES" if running else "no"

        if state:
            equity = f"${state.get('equity', 0):,.2f}"
            orig = state.get("original_start_equity", 0)
            eq = state.get("equity", 0)
            pnl_pct = f"{((eq - orig) / orig * 100):+.1f}%" if orig > 0 else "n/a"
            dd = f"{abs(state.get('drawdown_pct', 0)):.1f}%"
            trades = str(state.get("trade_count", 0))
            halted = "YES" if state.get("halted") else ""
        else:
            equity = "—"
            pnl_pct = "—"
            dd = "—"
            trades = "—"
            halted = ""

        print(f"{name:<12} {mode:<6} {running_str:<8} {equity:>10} {pnl_pct:>8} {dd:>6} {trades:>7} {halted}")


def cmd_add(address: str, name: str, equity: float = DEFAULT_EQUITY):
    """Add a new leader with a config file."""
    config_path = CONFIGS_DIR / f"{name}.yaml"
    if config_path.exists():
        print(f"Config {config_path} already exists. Remove it first or choose a different name.")
        return

    if not address.startswith("0x") or len(address) < 10:
        print(f"Invalid address: {address}")
        return

    config = {
        "leader": {
            "name": name,
            "target_wallet": address,
            "sub_account": "",
            "starting_equity": equity,
            "paper_mode": DEFAULT_PAPER_MODE,
            "slippage_cap_pct": DEFAULT_SLIPPAGE,
            "debounce_ms": DEFAULT_DEBOUNCE_MS,
            "reconciler_interval_s": DEFAULT_RECONCILER_S,
            "max_drawdown_pct": DEFAULT_MAX_DD,
            "daily_loss_limit_pct": DEFAULT_DAILY_LOSS,
        },
        "global": {
            "log_level": "INFO",
        },
    }

    CONFIGS_DIR.mkdir(exist_ok=True)
    config_path.write_text(yaml.dump(config, default_flow_style=False, sort_keys=False))
    print(f"Created config: {config_path}")
    print(f"  Leader: {name}")
    print(f"  Target: {address[:10]}...{address[-6:]}")
    print(f"  Equity: ${equity:,.2f} (paper mode)")
    print(f"\nStart with: python orchestrator.py start {name}")


def cmd_remove(name: str):
    """Remove a leader (stop service, remove config)."""
    if is_running(name):
        print(f"Stopping {name}...")
        _send_supervisor_cmd(f"stop:{name}")

    config_path = CONFIGS_DIR / f"{name}.yaml"
    if config_path.exists():
        config_path.unlink()
        print(f"Removed config: {config_path}")

    state_path = STATE_DIR / f"{name}.json"
    if state_path.exists():
        # Keep state file for history, just rename
        backup = state_path.with_suffix(".json.removed")
        state_path.rename(backup)
        print(f"State backed up to: {backup}")

    print(f"Leader {name} removed.")


def cmd_swap(name: str, new_address: str, approve: bool = False):
    """Swap a leader's target wallet. Reconciler handles orphan positions."""
    config = load_leader_config(name)
    if not config:
        print(f"No config found for {name}")
        return False

    old_address = config.get("leader", {}).get("target_wallet", "")
    if old_address == new_address:
        print(f"{name} already follows {new_address[:10]}...")
        return False

    if not approve:
        print(f"Swap {name}:")
        print(f"  OLD: {old_address[:10]}...{old_address[-6:]}")
        print(f"  NEW: {new_address[:10]}...{new_address[-6:]}")
        confirm = input("Confirm? [y/N] ").strip().lower()
        if confirm != "y":
            print("Cancelled.")
            return False

    # 1. Stop bot
    if is_running(name):
        print(f"  Stopping {name}...")
        _send_supervisor_cmd(f"stop:{name}")
        for _ in range(20):
            time.sleep(0.5)
            if not is_running(name):
                break

    # 2. Backup state file
    state_path = STATE_DIR / f"{name}.json"
    if state_path.exists():
        backup = state_path.with_suffix(f".json.pre_swap_{int(time.time())}")
        state_path.rename(backup)
        print(f"  State backed up: {backup.name}")

    # 3. Delete skip_coins (stale for new leader)
    skip_path = STATE_DIR / f"skip_coins_{name}.json"
    if skip_path.exists():
        skip_path.unlink()
        print(f"  Skip coins deleted")

    # 4. Update config
    config_path = CONFIGS_DIR / f"{name}.yaml"
    config["leader"]["target_wallet"] = new_address
    config_path.write_text(yaml.dump(config, default_flow_style=False, sort_keys=False))
    print(f"  Config updated: target_wallet -> {new_address[:10]}...")

    # 5. Restart bot
    print(f"  Starting {name}...")
    _send_supervisor_cmd(f"start:{name}")
    time.sleep(2)

    # 6. Notify
    send_telegram(
        f"<b>Leader Swap</b>\n"
        f"[{name}] {old_address[:10]}... → {new_address[:10]}...\n"
        f"Reconciler will close orphan positions automatically."
    )

    print(f"  Swap complete. Reconciler handles orphan closure.")
    return True


def cmd_approve_rotation(proposal_path: str = None):
    """Execute approved rotation from proposal file."""
    path = Path(proposal_path) if proposal_path else ROTATION_FILE
    if not path.exists():
        print(f"No proposal found at {path}")
        return

    proposal = yaml.safe_load(path.read_text())
    swaps = proposal.get("proposed_swaps", [])
    if not swaps:
        print("No swaps proposed.")
        return

    print(f"Executing {len(swaps)} approved swap(s):\n")
    success = 0
    for swap in swaps:
        name = swap["current_name"]
        new_addr = swap["new_address"]
        print(f"--- Swapping {name} -> {new_addr[:10]}... ---")
        if cmd_swap(name, new_addr, approve=True):
            success += 1
        print()

    # Archive proposal
    archive = path.with_suffix(f".executed_{int(time.time())}")
    path.rename(archive)
    print(f"Rotation complete: {success}/{len(swaps)} swaps executed.")
    print(f"Proposal archived: {archive.name}")

    send_telegram(
        f"<b>Rotation Executed</b>\n"
        f"{success}/{len(swaps)} leader swaps completed."
    )


def cmd_setup_subaccount(name: str):
    """Create a sub-account for a leader and deposit initial capital."""
    config = load_leader_config(name)
    if not config:
        print(f"No config found for {name}")
        return

    leader = config.get("leader", {})
    if leader.get("sub_account"):
        print(f"{name} already has sub-account: {leader['sub_account'][:10]}...")
        return

    alloc = load_allocation()
    total_capital = alloc.get("total_capital", DEFAULT_EQUITY * MAX_LEADERS)
    n_leaders = len(get_configured_leaders())
    deposit = total_capital / max(n_leaders, 1)

    # Check for per-leader override
    leader_alloc = alloc.get("leaders", {}).get(name, {})
    if leader_alloc.get("allocation_pct"):
        deposit = total_capital * leader_alloc["allocation_pct"] / 100
    if leader_alloc.get("max_allocation_usd"):
        deposit = min(deposit, leader_alloc["max_allocation_usd"])

    print(f"Setting up sub-account for {name}:")
    print(f"  Deposit: ${deposit:,.2f}")

    try:
        exchange = _init_exchange()
        result = exchange.create_sub_account(name)
        sub_address = result.get("response", {}).get("data", {}).get("subAccountUser", "")
        if not sub_address:
            print(f"  Sub-account creation response: {result}")
            print(f"  Could not extract sub-account address. Check response and update config manually.")
            return

        print(f"  Sub-account created: {sub_address[:10]}...")

        # Deposit capital
        transfer_result = exchange.usd_class_transfer(deposit, True, sub_account_user=sub_address)
        print(f"  Capital deposited: ${deposit:,.2f} -> {transfer_result}")

        # Update config
        config_path = CONFIGS_DIR / f"{name}.yaml"
        config["leader"]["sub_account"] = sub_address
        config["leader"]["paper_mode"] = False
        config["leader"]["starting_equity"] = deposit
        config_path.write_text(yaml.dump(config, default_flow_style=False, sort_keys=False))
        print(f"  Config updated: sub_account={sub_address[:10]}..., paper_mode=false")

        send_telegram(
            f"<b>Sub-Account Created</b>\n"
            f"[{name}] {sub_address[:10]}...\n"
            f"Deposit: ${deposit:,.2f}"
        )
    except Exception as e:
        print(f"  Error: {e}")
        send_telegram(f"<b>Sub-Account FAILED</b>\n[{name}] {e}")


def cmd_process_transfers():
    """Process pending transfer requests from portfolio_tracker."""
    if not TRANSFER_DIR.exists():
        return

    # K6+O1 fix: Recover orphaned .processing files (stale > 15 min = crash during transfer)
    for stale in TRANSFER_DIR.glob("*.processing"):
        try:
            age_s = time.time() - stale.stat().st_mtime
            if age_s > 900:  # 15 minutes (O1: raised from 10 to avoid race with slow transfers)
                recovered = stale.with_suffix(".json")
                stale.rename(recovered)
                print(f"  Recovered stale .processing file: {stale.name} (age={age_s:.0f}s)")
        except Exception as e:
            print(f"  Failed to recover {stale.name}: {e}")

    # Process pending deactivate files (K5: 24h grace period)
    for pending in sorted(TRANSFER_DIR.glob("pending_deactivate_*.json")):
        try:
            data = json.loads(pending.read_text())
            if time.time() >= data.get("execute_after", 0):
                # Grace period expired — convert to actual withdraw request
                name = data.get("name", "unknown")
                withdraw = {
                    "type": "withdraw",
                    "name": name,
                    "amount": data.get("amount", 0),
                    "sub_account": "",
                    "timestamp": time.time(),
                    "reason": f"auto-deactivate (was pending since {data.get('reason', '?')})",
                }
                wf = TRANSFER_DIR / f"withdraw_{name}_{int(time.time())}.json"
                tmp = wf.with_suffix(".tmp")
                tmp.write_text(json.dumps(withdraw, indent=2))
                os.replace(str(tmp), str(wf))
                pending.rename(pending.with_suffix(".executed"))
                print(f"  Pending deactivate executed: {name}")
        except Exception as e:
            print(f"  Failed to process pending deactivate: {e}")

    requests = sorted(TRANSFER_DIR.glob("*.json"))
    if not requests:
        return

    exchange = None
    processed = 0

    for req_file in requests:
        try:
            request = json.loads(req_file.read_text())
        except Exception:
            print(f"  Invalid request file: {req_file.name}")
            continue

        # Rename to .processing FIRST to prevent double execution (Krabje's eis)
        processing = req_file.with_suffix(".processing")
        req_file.rename(processing)

        req_type = request.get("type", "")
        name = request.get("name", "unknown")
        amount = request.get("amount", 0)
        sub_account = request.get("sub_account", "")

        if not sub_account:
            # Look up sub_account from config
            config = load_leader_config(name)
            sub_account = config.get("leader", {}).get("sub_account", "") if config else ""

        if not sub_account:
            print(f"  [{name}] No sub-account found, skipping {req_type}")
            processing.rename(processing.with_suffix(".failed"))
            continue

        try:
            if exchange is None:
                exchange = _init_exchange()

            if req_type == "skim":
                result = exchange.usd_class_transfer(amount, False, sub_account_user=sub_account)
                print(f"  [{name}] Skim ${amount:,.2f}: {result}")
                send_telegram(
                    f"<b>Profit Skim</b>\n"
                    f"[{name}] ${amount:,.2f} transferred to main wallet"
                )
            elif req_type == "withdraw":
                result = exchange.usd_class_transfer(amount, False, sub_account_user=sub_account)
                print(f"  [{name}] Withdraw ${amount:,.2f}: {result}")
            else:
                print(f"  [{name}] Unknown request type: {req_type}")

            processing.rename(processing.with_suffix(".done"))
            processed += 1
        except Exception as e:
            print(f"  [{name}] Transfer failed: {e}")
            processing.rename(processing.with_suffix(".failed"))

    if processed:
        print(f"Processed {processed} transfer(s)")


def cmd_withdraw(name: str):
    """Withdraw all capital from a leader's sub-account."""
    config = load_leader_config(name)
    if not config:
        print(f"No config found for {name}")
        return

    sub = config.get("leader", {}).get("sub_account", "")
    if not sub:
        print(f"{name} has no sub-account (paper mode?)")
        return

    # Stop bot first
    if is_running(name):
        print(f"Stopping {name} first...")
        _send_supervisor_cmd(f"stop:{name}")
        for _ in range(20):
            time.sleep(0.5)
            if not is_running(name):
                break

    # Fetch current balance
    try:
        resp = httpx.post(
            f"{HL_API_URL}/info",
            json={"type": "clearinghouseState", "user": sub},
            timeout=10,
        )
        resp.raise_for_status()
        state = resp.json()
        balance = float(state.get("marginSummary", {}).get("accountValue", "0"))

        positions = state.get("assetPositions", [])
        open_positions = [p for p in positions if abs(float(p.get("position", {}).get("szi", "0"))) > 0]

        if open_positions:
            print(f"  WARNING: {len(open_positions)} open position(s) on {sub[:10]}...")
            for p_data in open_positions:
                p_info = p_data.get("position", {})
                coin = p_info.get("coin", "?")
                szi = float(p_info.get("szi", "0"))
                side = "long" if szi > 0 else "short"
                print(f"    - {coin} {side} {abs(szi):.6f}")
            print(f"\n  Options:")
            print(f"    c = Close all positions first, then withdraw")
            print(f"    w = Withdraw anyway (positions will be force-liquidated!)")
            print(f"    n = Cancel")
            choice = input("  Choice [c/w/n]: ").strip().lower()
            if choice == "c":
                # O3 fix: close positions before withdraw
                try:
                    close_exchange = _init_exchange(account_address=sub)
                    for p_data in open_positions:
                        p_info = p_data.get("position", {})
                        coin = p_info.get("coin", "")
                        szi = float(p_info.get("szi", "0"))
                        size = abs(szi)
                        if coin and size > 0:
                            result = close_exchange.market_close(coin=coin, sz=size)
                            print(f"    Closed {coin}: {result}")
                    print("  All positions closed. Proceeding with withdraw...")
                    time.sleep(2)
                    # Re-fetch balance after closing
                    resp = httpx.post(
                        f"{HL_API_URL}/info",
                        json={"type": "clearinghouseState", "user": sub},
                        timeout=10,
                    )
                    resp.raise_for_status()
                    state = resp.json()
                    balance = float(state.get("marginSummary", {}).get("accountValue", "0"))
                except Exception as e:
                    print(f"  Failed to close positions: {e}")
                    return
            elif choice != "w":
                print("  Cancelled.")
                return

        if balance <= 0:
            print(f"  Balance: $0 — nothing to withdraw")
            return

        print(f"  Balance: ${balance:,.2f}")
        exchange = _init_exchange()
        result = exchange.usd_class_transfer(balance, False, sub_account_user=sub)
        print(f"  Withdrawn ${balance:,.2f} -> main wallet: {result}")
        send_telegram(f"<b>Withdrawal</b>\n[{name}] ${balance:,.2f} returned to main wallet")

    except Exception as e:
        print(f"  Error: {e}")


def cmd_start(name: str):
    """Start a leader's service."""
    config_path = CONFIGS_DIR / f"{name}.yaml"
    if not config_path.exists():
        print(f"No config found for {name}. Use 'add' first.")
        return

    # Verify config is valid
    try:
        config = yaml.safe_load(config_path.read_text())
        wallet = config.get("leader", {}).get("target_wallet", "")
        if not wallet:
            print(f"Config for {name} has no target_wallet set!")
            return
    except Exception as e:
        print(f"Invalid config: {e}")
        return

    if is_running(name):
        print(f"{name} is already running.")
        return

    print(f"Starting {name}...")
    if _send_supervisor_cmd(f"start:{name}"):
        time.sleep(2)
        if is_running(name):
            print(f"  {name} started successfully")
        else:
            print(f"  {name} may have failed to start. Check logs/{name}.log")


def cmd_stop(name: str):
    """Stop a leader's service."""
    if not is_running(name):
        print(f"{name} is not running.")
        return
    print(f"Stopping {name}...")
    _send_supervisor_cmd(f"stop:{name}")
    print(f"  {name} stopped")


def cmd_start_all():
    """Start all configured leaders."""
    leaders = get_configured_leaders()
    if not leaders:
        print("No leaders configured.")
        return
    for name in leaders:
        config = load_leader_config(name)
        wallet = config.get("leader", {}).get("target_wallet", "") if config else ""
        if not wallet:
            print(f"Skipping {name}: no target_wallet set")
            continue
        if is_running(name):
            print(f"  {name} already running")
            continue
        cmd_start(name)


def cmd_stop_all():
    """Stop all running leaders."""
    leaders = get_configured_leaders()
    for name in leaders:
        if is_running(name):
            cmd_stop(name)


def cmd_generate(top_n: int = 5, equity_per_leader: float = DEFAULT_EQUITY):
    """Generate leader configs from leaders.yaml discovery output."""
    if not LEADERS_FILE.exists():
        print(f"No {LEADERS_FILE} found. Run leader_scorer.py first.")
        return

    data = yaml.safe_load(LEADERS_FILE.read_text())
    candidates = data.get("candidates", [])
    if not candidates:
        print("No candidates in leaders.yaml")
        return

    existing = set(get_configured_leaders())

    # Pick top N that aren't already configured
    added = 0
    for c in candidates:
        if added >= top_n:
            break

        address = c["address"]
        # Generate a short name from address if no display_name
        display = c.get("display_name", "")
        if display and len(display) <= 12 and display.isalnum():
            name = display.lower()
        else:
            name = f"leader_{address[2:8].lower()}"

        if name in existing:
            print(f"  Skipping {name}: already configured")
            continue

        cmd_add(address, name, equity_per_leader)
        added += 1
        print()

    print(f"\nGenerated {added} new leader configs from discovery results.")
    print("Review configs in configs/ then run: python orchestrator.py start-all")


def cmd_emergency_close_all():
    """Emergency: freeze all bots AND close all open positions on all sub-accounts.

    This is the nuclear option — use only when you want to exit everything NOW.
    Closes perp positions via market order. Does NOT withdraw capital.
    """
    leaders = get_configured_leaders()
    if not leaders:
        print("No leaders configured.")
        return

    # Confirm
    print("⚠️  EMERGENCY CLOSE ALL — this will:")
    print("  1. Freeze all bots (stop new opens)")
    print("  2. Market close ALL open positions on ALL sub-accounts")
    print("  3. Does NOT withdraw capital (use 'withdraw' for that)")
    confirm = input("\nType 'CLOSE ALL' to confirm: ").strip()
    if confirm != "CLOSE ALL":
        print("Cancelled.")
        return

    # 1. Freeze all bots
    print("\nFreezing all bots...")
    _send_supervisor_cmd("stop-all")
    time.sleep(3)

    # 2. Close all positions on each sub-account
    exchange = None
    for name in leaders:
        config = load_leader_config(name)
        if not config:
            continue

        sub = config.get("leader", {}).get("sub_account", "")
        if not sub:
            print(f"  [{name}] No sub-account (paper mode) — skipping")
            continue

        try:
            # Fetch positions
            resp = httpx.post(
                f"{HL_API_URL}/info",
                json={"type": "clearinghouseState", "user": sub},
                timeout=10,
            )
            resp.raise_for_status()
            state = resp.json()

            positions = state.get("assetPositions", [])
            open_positions = [
                p for p in positions
                if abs(float(p.get("position", {}).get("szi", "0"))) > 0
            ]

            if not open_positions:
                print(f"  [{name}] No open positions")
                continue

            # Create exchange with sub-account context (market_close uses account_address)
            sub_exchange = _init_exchange(account_address=sub)

            for pos_data in open_positions:
                p = pos_data.get("position", {})
                coin = p.get("coin", "")
                szi = float(p.get("szi", "0"))
                side_str = "long" if szi > 0 else "short"
                size = abs(szi)

                try:
                    result = sub_exchange.market_close(coin=coin, sz=size)
                    print(f"  [{name}] Closed {coin} {side_str} {size:.6f}: {result}")
                except Exception as e:
                    print(f"  [{name}] FAILED to close {coin}: {e}")

        except Exception as e:
            print(f"  [{name}] Error: {e}")

    send_telegram(
        f"<b>EMERGENCY CLOSE ALL</b>\n"
        f"All bots frozen, all positions market-closed.\n"
        f"Manual review required."
    )
    print("\nEmergency close complete. Review positions manually.")


def cmd_rebalance():
    """Rebalance capital allocation across leaders.

    In paper mode: just reports what would happen.
    In live mode: would execute usdClassTransfer calls.
    """
    leaders = get_configured_leaders()
    if not leaders:
        print("No leaders configured.")
        return

    states = []
    for name in leaders:
        state = load_leader_state(name)
        config = load_leader_config(name)
        if state and config:
            states.append({
                "name": name,
                "equity": state.get("equity", 0),
                "original": state.get("original_start_equity", 0),
                "pnl_pct": ((state["equity"] - state["original_start_equity"])
                           / state["original_start_equity"] * 100)
                           if state.get("original_start_equity", 0) > 0 else 0,
                "paper": config.get("leader", {}).get("paper_mode", True),
            })

    if not states:
        print("No state data available.")
        return

    total = sum(s["equity"] for s in states)
    if total <= 0:
        print("Total equity is 0, nothing to rebalance.")
        return

    # Equal allocation for now (performance-weighted after 30 days)
    target_each = total / len(states)

    print(f"{'Leader':<12} {'Current':>10} {'Target':>10} {'Delta':>10} {'PnL%':>8}")
    print("-" * 55)

    for s in states:
        delta = target_each - s["equity"]
        print(f"{s['name']:<12} ${s['equity']:>9,.2f} ${target_each:>9,.2f} ${delta:>+9,.2f} {s['pnl_pct']:>+7.1f}%")

    if all(s["paper"] for s in states):
        print("\nAll leaders in paper mode — rebalancing is informational only.")
    else:
        print("\nLive rebalancing not yet implemented (requires orchestrator.py + SDK).")


# ── CLI ─────────────────────────────────────────────────────────

def main():
    parser = argparse.ArgumentParser(description="Multi-Target Copy Trader Orchestrator")
    sub = parser.add_subparsers(dest="command")

    sub.add_parser("status", help="Show all leaders status")

    add_p = sub.add_parser("add", help="Add a new leader")
    add_p.add_argument("address", help="Target wallet address")
    add_p.add_argument("name", help="Leader name (alphanumeric, lowercase)")
    add_p.add_argument("--equity", type=float, default=DEFAULT_EQUITY, help="Starting equity")

    rm_p = sub.add_parser("remove", help="Remove a leader")
    rm_p.add_argument("name", help="Leader name")

    start_p = sub.add_parser("start", help="Start a leader")
    start_p.add_argument("name", help="Leader name")

    stop_p = sub.add_parser("stop", help="Stop a leader")
    stop_p.add_argument("name", help="Leader name")

    sub.add_parser("start-all", help="Start all configured leaders")
    sub.add_parser("stop-all", help="Stop all leaders")

    gen_p = sub.add_parser("generate", help="Generate configs from leaders.yaml")
    gen_p.add_argument("--top", type=int, default=5, help="Number of top leaders")
    gen_p.add_argument("--equity", type=float, default=DEFAULT_EQUITY, help="Equity per leader")

    sub.add_parser("rebalance", help="Show/execute capital rebalancing")

    swap_p = sub.add_parser("swap", help="Swap a leader's target wallet")
    swap_p.add_argument("name", help="Leader name")
    swap_p.add_argument("new_address", help="New target wallet address")
    swap_p.add_argument("--approve", action="store_true", help="Skip confirmation")

    approve_p = sub.add_parser("approve-rotation", help="Execute rotation proposal")
    approve_p.add_argument("--proposal", type=str, default=None, help="Path to proposal YAML")

    setup_p = sub.add_parser("setup-subaccount", help="Create sub-account + deposit")
    setup_p.add_argument("name", help="Leader name")

    sub.add_parser("process-transfers", help="Execute pending transfer requests")

    withdraw_p = sub.add_parser("withdraw", help="Withdraw capital from sub-account")
    withdraw_p.add_argument("name", help="Leader name")

    sub.add_parser("emergency-close-all", help="EMERGENCY: freeze all + market close all positions")

    args = parser.parse_args()

    if args.command == "status":
        cmd_status()
    elif args.command == "add":
        cmd_add(args.address, args.name, args.equity)
    elif args.command == "remove":
        cmd_remove(args.name)
    elif args.command == "start":
        cmd_start(args.name)
    elif args.command == "stop":
        cmd_stop(args.name)
    elif args.command == "start-all":
        cmd_start_all()
    elif args.command == "stop-all":
        cmd_stop_all()
    elif args.command == "generate":
        cmd_generate(args.top, args.equity)
    elif args.command == "rebalance":
        cmd_rebalance()
    elif args.command == "swap":
        cmd_swap(args.name, args.new_address, args.approve)
    elif args.command == "approve-rotation":
        cmd_approve_rotation(args.proposal)
    elif args.command == "setup-subaccount":
        cmd_setup_subaccount(args.name)
    elif args.command == "process-transfers":
        cmd_process_transfers()
    elif args.command == "withdraw":
        cmd_withdraw(args.name)
    elif args.command == "emergency-close-all":
        cmd_emergency_close_all()
    else:
        parser.print_help()


if __name__ == "__main__":
    main()
