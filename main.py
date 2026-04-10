#!/usr/bin/env python3
"""
Hyperliquid Multi-Target Copy Trader — follows a single leader per process.

Each leader runs as a separate systemd unit:
    systemctl start hl-multicopy@alice
    systemctl start hl-multicopy@bob

Usage:
    python main.py --config configs/alice.yaml
"""

import argparse
import asyncio
import json
import logging
import logging.handlers
import signal
import sys
from pathlib import Path

from config import load_config, LeaderConfig, GlobalConfig
from listener import Listener
from execution import ExecutionEngine, Position
from reconciler import Reconciler


def setup_logging(leader_name: str, global_cfg: GlobalConfig):
    fmt = "%(asctime)s [%(levelname)s] %(name)s: %(message)s"
    log_path = global_cfg.logs_dir() / f"{leader_name}.log"
    file_handler = logging.handlers.RotatingFileHandler(
        log_path, maxBytes=5 * 1024 * 1024, backupCount=3, encoding="utf-8"
    )
    file_handler.setFormatter(logging.Formatter(fmt))
    logging.basicConfig(
        level=getattr(logging, global_cfg.log_level),
        format=fmt,
        handlers=[file_handler],
    )


def parse_args():
    p = argparse.ArgumentParser(description="Hyperliquid Multi-Target Copy Trader")
    p.add_argument("--config", required=True, help="Path to leader YAML config file")
    return p.parse_args()


def _restore_state(engine: ExecutionEngine, leader: LeaderConfig, global_cfg: GlobalConfig, log: logging.Logger):
    """Restore paper positions and equity from state file if it exists."""
    state_file = global_cfg.state_dir() / f"{leader.name}.json"
    if not state_file.exists():
        return
    try:
        state = json.loads(state_file.read_text())
        if state.get("target_wallet") != leader.target_wallet:
            log.info("State file is for a different wallet, ignoring")
            return
        saved_equity = state.get("equity")
        if saved_equity is not None:
            engine.paper_equity = saved_equity
            engine.our_equity = saved_equity
            engine._paper_cash = state.get("paper_cash", saved_equity)
            engine.paper_starting_equity = state.get("original_start_equity", leader.starting_equity)
            engine._original_start_equity = engine.paper_starting_equity
            engine._peak_equity = max(
                state.get("peak_equity", saved_equity),
                engine.paper_starting_equity,
            )
            engine._halted = state.get("halted", False)
            engine.total_trade_count = state.get("trade_count", 0)
            if saved_equity <= 0:
                log.warning(f"Restored negative equity: ${saved_equity:.2f}")
        for coin, p in state.get("positions", {}).items():
            engine.paper_positions[coin] = Position(
                coin=coin,
                side=p["side"],
                size=p["size"],
                entry_price=p["entry_price"],
            )
        log.info(
            f"Restored state: equity=${saved_equity:,.2f}, "
            f"{len(engine.paper_positions)} positions, "
            f"{engine.total_trade_count} trades, "
            f"halted={engine._halted}"
        )
    except Exception:
        log.warning("Could not restore state file, starting fresh", exc_info=True)


async def run(leader: LeaderConfig, global_cfg: GlobalConfig):
    log = logging.getLogger("copytrader")

    mode = "PAPER" if leader.paper_mode else "LIVE"
    log.info(f"===================================================")
    log.info(f"  Hyperliquid Multi-Target Copy Trader")
    log.info(f"  Leader: {leader.name}")
    log.info(f"  Mode:   {mode}")
    log.info(f"  Target: {leader.target_wallet[:10]}...{leader.target_wallet[-6:]}")
    if leader.paper_mode:
        log.info(f"  Equity: ${leader.starting_equity:,.2f}")
    else:
        sub = leader.sub_account
        log.info(f"  Sub-account: {sub[:10]}...{sub[-6:]}" if sub else "  Sub-account: none")
    log.info(f"  Reconciler: every {leader.reconciler_interval_s}s")
    log.info(f"  Debounce: {leader.debounce_ms}ms")
    log.info(f"  Risk: DD max {leader.max_drawdown_pct}%, daily max {leader.daily_loss_limit_pct}%")
    log.info(f"===================================================")

    # Init components
    engine = ExecutionEngine(leader, global_cfg)

    if leader.paper_mode:
        _restore_state(engine, leader, global_cfg, log)
    else:
        if not global_cfg.private_key:
            log.error("HL_PRIVATE_KEY not set — cannot trade live")
            return
        engine.init_live()
        log.info("Live SDK initialized")

    reconciler = Reconciler(engine, leader, global_cfg)
    listener = Listener(
        target_wallet=leader.target_wallet,
        ws_url=global_cfg.ws_url,
        debounce_ms=leader.debounce_ms,
        on_fill_batch=engine.handle_fill_batch,
        on_reconnect=reconciler.force_sync,
    )

    # Graceful shutdown
    stop_event = asyncio.Event()

    def shutdown():
        log.info("Shutting down...")
        stop_event.set()

    loop = asyncio.get_event_loop()
    for sig in (signal.SIGINT, signal.SIGTERM):
        try:
            loop.add_signal_handler(sig, shutdown)
        except NotImplementedError:
            pass

    # Start reconciler first
    log.info("Fetching initial target state...")
    reconciler_task = asyncio.create_task(reconciler.start(), name="reconciler")
    for _ in range(100):
        if engine.target_equity > 0:
            break
        await asyncio.sleep(0.1)
    if engine.target_equity <= 0:
        log.warning("Could not fetch target equity — fills will be skipped until reconciler succeeds")

    tasks = [
        asyncio.create_task(listener.start(), name="listener"),
        reconciler_task,
        asyncio.create_task(stop_event.wait(), name="stop"),
    ]

    done, pending = await asyncio.wait(tasks, return_when=asyncio.FIRST_COMPLETED)

    # Cleanup
    await listener.stop()
    await reconciler.stop()
    await engine.notifier.close()
    for t in pending:
        t.cancel()

    log.info(f"=== FINAL: {engine.stats_summary()} ===")

    if leader.paper_mode and engine.paper_trades:
        log.info(f"=== TRADE LOG ({len(engine.paper_trades)} trades) ===")
        for t in engine.paper_trades:
            pnl_str = f" PnL=${t.pnl:+.2f}" if t.pnl != 0 else ""
            log.info(f"  {t.coin} {t.direction} {t.size:.6f}@${t.price:,.2f}{pnl_str}")


def main():
    args = parse_args()
    leader, global_cfg = load_config(args.config)
    setup_logging(leader.name, global_cfg)
    try:
        asyncio.run(run(leader, global_cfg))
    except KeyboardInterrupt:
        pass


if __name__ == "__main__":
    main()
