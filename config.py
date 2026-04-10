"""Multi-target Copy Trader Configuration.

All settings are loaded from a per-leader YAML config file.
No module-level globals — everything flows through dataclasses.
"""

import os
from dataclasses import dataclass, field
from pathlib import Path

import yaml


@dataclass
class LeaderConfig:
    """Per-leader settings loaded from YAML."""
    # Identity
    name: str                          # Human-readable name (e.g. "alice")
    target_wallet: str                 # Wallet address to copy

    # Our sub-account for this leader
    sub_account: str = ""              # Sub-account address (live mode)

    # Sizing
    starting_equity: float = 100.0     # Starting equity allocation (USD)

    # Execution
    slippage_cap_pct: float = 0.5      # Max slippage % vs target fill price
    debounce_ms: int = 80              # Batch partial fills window (ms)

    # Reconciler
    reconciler_interval_s: int = 30    # Seconds between full state syncs

    # Risk (Layer 1 — bot-level, real-time)
    max_drawdown_pct: float = 25.0     # Soft freeze: no new opens beyond this DD%
    daily_loss_limit_pct: float = 10.0 # Block opens if daily loss exceeds this %

    # Mode
    paper_mode: bool = True            # True = simulate, False = real orders


@dataclass
class GlobalConfig:
    """Global settings shared across all leaders."""
    # Connection
    ws_url: str = "wss://api.hyperliquid.xyz/ws"
    api_url: str = "https://api.hyperliquid.xyz"

    # Account (from env)
    private_key: str = ""
    account_address: str = ""

    # Telegram (from env)
    telegram_bot_token: str = ""
    telegram_chat_id: str = ""

    # HIP-3 (Builder-Deployed Perp DEXes)
    perp_dexes: list[str] = field(default_factory=lambda: ["xyz"])

    # Logging
    log_level: str = "INFO"

    # Paths (set after loading)
    base_dir: Path = field(default_factory=lambda: Path(__file__).parent)

    def state_dir(self) -> Path:
        d = self.base_dir / "state"
        d.mkdir(exist_ok=True)
        return d

    def logs_dir(self) -> Path:
        d = self.base_dir / "logs"
        d.mkdir(exist_ok=True)
        return d


def load_config(config_path: str) -> tuple[LeaderConfig, GlobalConfig]:
    """Load leader config from YAML, global config from env + YAML defaults."""
    path = Path(config_path)
    if not path.exists():
        raise FileNotFoundError(f"Config file not found: {config_path}")

    with open(path) as f:
        raw = yaml.safe_load(f)

    # Leader config from YAML
    leader_data = raw.get("leader", {})
    leader = LeaderConfig(
        name=leader_data["name"],
        target_wallet=leader_data["target_wallet"],
        sub_account=leader_data.get("sub_account", ""),
        starting_equity=float(leader_data.get("starting_equity", 100.0)),
        slippage_cap_pct=float(leader_data.get("slippage_cap_pct", 0.5)),
        debounce_ms=int(leader_data.get("debounce_ms", 80)),
        reconciler_interval_s=int(leader_data.get("reconciler_interval_s", 30)),
        max_drawdown_pct=float(leader_data.get("max_drawdown_pct", 25.0)),
        daily_loss_limit_pct=float(leader_data.get("daily_loss_limit_pct", 10.0)),
        paper_mode=leader_data.get("paper_mode", True),
    )

    # Validate leader config
    if leader.starting_equity <= 0:
        raise ValueError(f"starting_equity must be > 0, got {leader.starting_equity}")
    if not (0 < leader.slippage_cap_pct < 100):
        raise ValueError(f"slippage_cap_pct must be 0-100, got {leader.slippage_cap_pct}")
    if not (0 < leader.max_drawdown_pct <= 100):
        raise ValueError(f"max_drawdown_pct must be 0-100, got {leader.max_drawdown_pct}")
    if not (0 < leader.daily_loss_limit_pct <= 100):
        raise ValueError(f"daily_loss_limit_pct must be 0-100, got {leader.daily_loss_limit_pct}")
    if not leader.name:
        raise ValueError("leader.name is required")

    # Global config from env + YAML overrides
    global_data = raw.get("global", {})
    global_cfg = GlobalConfig(
        ws_url=global_data.get("ws_url", "wss://api.hyperliquid.xyz/ws"),
        api_url=global_data.get("api_url", "https://api.hyperliquid.xyz"),
        private_key=os.environ.get("HL_PRIVATE_KEY", ""),
        account_address=os.environ.get("HL_WALLET_ADDRESS", ""),
        telegram_bot_token=os.environ.get("TELEGRAM_BOT_TOKEN", ""),
        telegram_chat_id=os.environ.get("TELEGRAM_CHAT_ID", ""),
        perp_dexes=global_data.get("perp_dexes", ["xyz"]),
        log_level=global_data.get("log_level", "INFO"),
        base_dir=path.parent.parent,  # configs/alice.yaml → hl-multicopy/
    )

    return leader, global_cfg
