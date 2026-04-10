"""Shared state management for Krabje's monitoring scripts.

Reads bot state from state/{name}.json files and provides
JSONL append-only logging for equity history tracking.
"""

import json
import os
import time
from dataclasses import dataclass
from pathlib import Path


@dataclass
class LeaderState:
    """Parsed state from a bot's state/{name}.json file."""
    name: str
    target_wallet: str
    timestamp: float
    equity: float
    original_start_equity: float
    peak_equity: float
    drawdown_pct: float         # Negative value (e.g. -15.2)
    daily_pnl: float
    positions: dict             # coin -> {side, size, entry_price, leverage}
    is_alive: bool
    last_fill_ts: float
    halted: bool
    mode: str                   # "paper" or "live"
    trade_count: int
    target_equity: float

    @property
    def position_count(self) -> int:
        return len(self.positions)

    @property
    def total_exposure(self) -> float:
        """Sum of all position notional values (approximate)."""
        total = 0.0
        for p in self.positions.values():
            total += abs(p.get("size", 0) * p.get("entry_price", 0))
        return total

    @property
    def pnl_pct(self) -> float:
        if self.original_start_equity <= 0:
            return 0.0
        return (self.equity - self.original_start_equity) / self.original_start_equity * 100

    @property
    def coins(self) -> set[str]:
        return set(self.positions.keys())


def read_all_states(state_dir: str | Path) -> list[LeaderState]:
    """Read all state/*.json files and return parsed LeaderState objects."""
    state_dir = Path(state_dir)
    states = []
    if not state_dir.exists():
        return states

    for f in state_dir.glob("*.json"):
        if f.name.endswith(".tmp") or f.name.startswith("skip_coins_"):
            continue
        try:
            data = json.loads(f.read_text())
            states.append(LeaderState(
                name=data.get("target", f.stem),
                target_wallet=data.get("target_wallet", ""),
                timestamp=data.get("timestamp", 0),
                equity=data.get("equity", 0),
                original_start_equity=data.get("original_start_equity", 0),
                peak_equity=data.get("peak_equity", 0),
                drawdown_pct=data.get("drawdown_pct", 0),
                daily_pnl=data.get("daily_pnl", 0),
                positions=data.get("positions", {}),
                is_alive=data.get("is_alive", False),
                last_fill_ts=data.get("last_fill_ts", 0),
                halted=data.get("halted", False),
                mode=data.get("mode", "paper"),
                trade_count=data.get("trade_count", 0),
                target_equity=data.get("target_equity", 0),
            ))
        except (json.JSONDecodeError, KeyError, TypeError, AttributeError) as e:
            print(f"Warning: could not parse {f}: {e}")

    return states


def append_equity_log(log_path: str | Path, name: str, equity: float,
                      drawdown_pct: float, positions: int):
    """Append a single equity snapshot to JSONL log file."""
    log_path = Path(log_path)
    log_path.parent.mkdir(parents=True, exist_ok=True)
    entry = {
        "ts": time.time(),
        "name": name,
        "equity": round(equity, 2),
        "dd": round(drawdown_pct, 2),
        "pos": positions,
    }
    with open(log_path, "a") as f:
        f.write(json.dumps(entry) + "\n")


def read_equity_log(log_path: str | Path, name: str | None = None,
                    since_ts: float = 0) -> list[dict]:
    """Read equity log entries, optionally filtered by name and time."""
    log_path = Path(log_path)
    if not log_path.exists():
        return []
    entries = []
    with open(log_path) as f:
        for line in f:
            line = line.strip()
            if not line:
                continue
            try:
                entry = json.loads(line)
                if name and entry.get("name") != name:
                    continue
                if entry.get("ts", 0) < since_ts:
                    continue
                entries.append(entry)
            except json.JSONDecodeError:
                continue
    return entries
