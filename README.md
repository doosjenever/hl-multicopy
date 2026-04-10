# HL Multi-Target Copy Trader

A robust, multi-target copy trader for Hyperliquid. Follows 7 top perpetual futures traders ("foxes") with proportional sizing. Paper mode simulates trades with $200 virtual capital per leader.

**Code Review Status:** Phase 1 (7 critical) + Phase 2 (14 fixes) DONE. Phase 3 (13 medium) open.
Reviewed by Krabje's team (43 findings) + Claude (50+ findings). Krabje: "Codebase is ready for live."

## Support & Referral 🎁
If you find this bot useful and want to start trading on Hyperliquid, consider using my referral link. You will receive a **4% discount** on your trading fees:
👉 **[https://app.hyperliquid.xyz/join/HLCOPYTRADER](https://app.hyperliquid.xyz/join/HLCOPYTRADER)**
*(Code: `HLCOPYTRADER`)*

## Setup & Installation

You can run this multi-copy orchestrator standalone on your own server or local machine without the full OpenClaw/Krabje ecosystem.

1. **Clone the repository:**
   ```bash
   git clone https://github.com/doosjenever/hl-multicopy.git
   cd hl-multicopy
   ```

2. **Create a virtual environment & install dependencies:**
   ```bash
   python3 -m venv venv
   source venv/bin/activate
   pip install -r requirements.txt
   ```

3. **Configure environment variables:**
   ```bash
   cp .env.example .env
   # Add your Hyperliquid Private Key and Telegram details
   nano .env
   ```

4. **Initialize Leader configs:**
   You can either manually create `.yaml` configs in the `configs/` directory, or use the automated scorer to generate them.

## Quick Start

```bash
# Check status of all bots
python3 orchestrator.py status

# Start/stop all bots
python3 orchestrator.py start-all
python3 orchestrator.py stop-all

# Leader swap (reconciler automatically closes orphan positions)
python3 orchestrator.py swap nachtvos 0xNEW_ADDRESS

# Execute weekly rotation after Telegram approval
python3 orchestrator.py approve-rotation

# Live mode: setup sub-account + deposit capital
python3 orchestrator.py setup-subaccount nachtvos

# Withdraw capital (with option to close positions first)
python3 orchestrator.py withdraw nachtvos

# EMERGENCY: stop everything + close all positions
python3 orchestrator.py emergency-close-all
```

## The 7 Foxes

| Name | Type | Leader Focus |
|------|------|-------------|
| nachtvos | Crypto | BTC/TAO/TRUMP |
| sluipvos | Crypto | BTC/ETH/SOL/HYPE |
| stadsvos | Crypto | BTC/FARTCOIN/STABLE |
| vuurvos | Crypto | BTC/ETH/HYPE |
| woestijnvos | Crypto | BTC/HYPE |
| poolvos | HIP-3 | BRENTOIL |
| sneeuwvos | HIP-3 | AAPL/AMD/AMZN/BRENTOIL |

Leaders are evaluated weekly via `leader_scorer.py` and `rotation_proposer.py`.

## Files

### Core Trading
| File | Function |
|---------|---------|
| `main.py` | Bot entrypoint per leader |
| `config.py` | Loads and validates YAML configurations |
| `listener.py` | WebSocket listener for target fills |
| `execution.py` | Trade engine (paper + live), proportional sizing |
| `reconciler.py` | State sync, drift correction (perps + HIP-3) |
| `shared_state.py` | Shared state for risk aggregation |
| `notifier.py` | Telegram notifications |

### Management & Monitoring
| File | Function |
|---------|---------|
| `orchestrator.py` | Lifecycle management: add/remove/start/stop/swap/withdraw/transfers/emergency-close-all |
| `supervisor.py` | Process supervisor (crash recovery, healthcheck) |
| `leader_scorer.py` | ApexLiquid-style discovery (weekly) |
| `rotation_proposer.py` | Compares current leaders with new candidates |
| `risk_checker.py` | Layer 2 risk checks: DD, correlation, liveness (3min) |
| `portfolio_tracker.py` | Equity tracking + profit skimming (30min) |
| `fallback_watchdog.sh` | Layer 3: bash kill switch (2min) |
| `allocation.yaml` | Capital allocation config |

## Directories

```
configs/                    # YAML config per leader
state/                      # JSON state per leader (positions, equity, trades)
state/transfer_requests/    # Skim/withdraw request files (Container → Host)
logs/                       # Logs per leader + monitoring logs
systemd/                    # Systemd service + timer units
leaders.yaml                # Discovery scan results
rotation_proposal.yaml      # Weekly rotation proposal
```

## Discovery Filters (ApexLiquid-style)

### Pre-filter (leaderboard data, 33K+ traders)
- Account value >= $25K
- All-time ROE >= 100%
- 1D ROE >= 0.5%, 7D >= 5%, 30D >= 15%

### Deep scan (portfolio + fills)
- Max drawdown <= 75% (all-time)
- Min 30 days active
- Last trade <= 2 days ago
- Min 20 closed trades

### Scoring
30% ROE_30d + 20% DD + 15% PF + 10% WR + 15% momentum + 10% confidence

## Architecture

### HOST (systemd)
- `hl-supervisor.service` — Manages all 7 bot processes
- `hl-process-transfers.timer` — Executes skim/withdraw transfers (every 5 min)
- Python venv: `venv/bin/python`

### Container (Krabje monitoring)
- Runs `risk_checker`, `portfolio_tracker`, `leader_scorer`, `rotation_proposer`, `fallback_watchdog`
- Writes JSON request files to `state/transfer_requests/`
- NEVER starts/stops bots directly itself

### Transfer Flow
1. Container writes `skim_{name}_{ts}.json` or `pending_deactivate_{name}_{ts}.json`
2. Host timer renames it to `.processing` (prevents double execution)
3. Orphan recovery: `.processing` > 15 min → reverts to `.json`
4. Pending deactivate: 24h grace period, then converts to withdraw
5. After success → `.done`, after failure → `.failed`

### HOST ↔ Container Sync (`sync_state_to_container.sh`, every minute)
1. `write_host_status.sh` → `state/host_status.json` (process status for Krabje)
2. rsync state files HOST → container
3. Python code HOST → container (all .py files)
4. Leader state container → HOST

## Risk Management

| Threshold | Action |
|---------|-------|
| -20% DD | Telegram warning |
| -25% DD | Soft freeze (no new opens) |
| < -15% DD | Unfreeze (10% hysteresis, prevents oscillation) |
| -30% DD 7 days | Auto-freeze |
| -30% DD 14 days | Pending deactivate (24h grace period) |
| -20% aggregate | Kill switch (all foxes) |
| >40% exposure 1 coin | Concentration alert (current price, HIP-3 dedup) |
| DD escalation timer | Resets only on recovery < 15% DD |

## Profit Taking

| Condition | Action |
|----------|-------|
| 50%+ profit | Skim 50% (always) |
| 14d + 30% profit | Skim 50% |
| 7d + 20% profit | Skim 50% |

24h cooldown. Baseline raised after each skim. Skim capped at max 30% of equity (margin safety).

## Cron Jobs

### Container (Krabje via OpenClaw Gateway)
```
*/3 * * * *  risk_checker.py           # Risk audit
*/30 * * * * portfolio_tracker.py      # Equity + profit taking
0 6 * * 0    leader_scorer.py          # Weekly discovery
30 6 * * 0   rotation_proposer.py      # Rotation proposal
*/2 * * * *  fallback_watchdog.sh      # Kill switch
```

### HOST (systemd timer)
```
hl-process-transfers.timer  # Every 5 min — skim/withdraw transfers
```

## Environment Variables

```
TELEGRAM_BOT_TOKEN   # Telegram bot token
TELEGRAM_CHAT_ID     # Telegram chat ID
HL_PRIVATE_KEY       # Private key (live mode + transfers)
HL_WALLET_ADDRESS    # Master wallet (live mode)
```

## Relationship with Leader Copy Trader

The standalone Leader bot (`hl-copytrader/`) runs separately via `hl-copytrader.service` and is NEVER touched by the multicopy code.
