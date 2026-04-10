"""Execution engine — paper and live order placement with Layer 1 risk checks."""

import asyncio
import json
import logging
import os
import shutil
import time
from collections import deque
from dataclasses import dataclass
from pathlib import Path

import orjson

from config import LeaderConfig, GlobalConfig
from listener import FillBatch
from notifier import TelegramNotifier

log = logging.getLogger(__name__)

TAKER_FEE = 0.00035  # Hyperliquid taker fee: 0.035%


def _safe_float(val, default: float = 0.0) -> float:
    """F1 fix: Safe float parsing for API responses that may return null/invalid."""
    try:
        return float(val) if val is not None else default
    except (ValueError, TypeError):
        return default

# Lazy imports for live mode
_exchange_cls = None
_info_cls = None
_account_cls = None


@dataclass
class Position:
    """Tracked position state."""
    coin: str
    side: str        # "long" or "short"
    size: float      # Absolute size
    entry_price: float
    unrealized_pnl: float = 0.0


@dataclass
class PaperTrade:
    """Record of a simulated trade."""
    timestamp: float
    coin: str
    direction: str   # "Open Long", "Close Long", "Open Short", "Close Short"
    size: float
    price: float
    pnl: float = 0.0
    fee: float = 0.0


class ExecutionEngine:
    """Handles order execution in paper or live mode."""

    def __init__(self, leader: LeaderConfig, global_cfg: GlobalConfig):
        self.leader = leader
        self.global_cfg = global_cfg

        self.paper_equity = leader.starting_equity
        self.paper_positions: dict[str, Position] = {}
        self.paper_trades: deque[PaperTrade] = deque(maxlen=1000)
        self.total_trade_count: int = 0
        self.paper_starting_equity = leader.starting_equity

        # Target state (refreshed by reconciler)
        self.target_equity: float = 0.0
        self._target_positions: dict[str, dict] = {}

        # Our real equity (for live mode)
        self.our_equity: float = leader.starting_equity if leader.paper_mode else 0.0

        # Layer 1 risk state
        self._peak_equity: float = leader.starting_equity
        self._original_start_equity: float = leader.starting_equity
        self._daily_start_equity: float = leader.starting_equity
        self._daily_start_date: str = ""
        self._halted: bool = False  # Soft freeze active

        self._min_equity_warned = False
        self._bg_tasks: set = set()
        self.notifier = TelegramNotifier(
            leader_name=leader.name,
            bot_token=global_cfg.telegram_bot_token,
            chat_id=global_cfg.telegram_chat_id,
        )

        # Live mode SDK objects
        self._exchange = None
        self._info = None
        self._sz_decimals: dict[str, int] = {}
        self.live_positions: dict[str, Position] = {}

        # State file path
        self._state_dir = global_cfg.state_dir()

        # Skip set (auto-managed, persisted to skip_coins.json)
        self._persistent_skip_file = self._state_dir / f"skip_coins_{leader.name}.json"
        # H2 fix: load persistent skip coins at init to prevent duplicate trades
        # before first reconciler sync. None = file doesn't exist (first run, wait for sync).
        if self._persistent_skip_file.exists():
            self._skip_coins: set[str] | None = self.load_persistent_skip_coins()
        else:
            self._skip_coins: set[str] | None = None

        # Fill timestamp tracker (works in both paper and live mode)
        self._last_fill_ts: float = 0.0

        # Target leverage per coin
        self._target_leverage: dict[str, dict] = {}
        self._our_leverage: dict[str, dict] = {}

        # Spot (HIP-3) asset tracking — coins starting with "@" are spot pairs
        self._spot_coins: set[str] = set()

        # Asyncio lock for _target_positions (prevents race between listener fills and reconciler)
        self._target_lock = asyncio.Lock()

        # Paper cash tracker (separate from equity which includes unrealized PnL)
        self._paper_cash: float = leader.starting_equity

        # H9: Track last known current prices for state file (used by risk_checker)
        self._last_current_prices: dict[str, float] = {}

    def load_persistent_skip_coins(self) -> set[str]:
        """Load skip coins from persistent file."""
        if self._persistent_skip_file.exists():
            try:
                data = json.loads(self._persistent_skip_file.read_text())
                if isinstance(data, list):
                    return set(data)
            except Exception:
                pass
        return set()

    def save_persistent_skip_coins(self, coins: set[str]):
        """Save skip coins to persistent file."""
        tmp = self._persistent_skip_file.with_suffix(".tmp")
        tmp.write_text(json.dumps(sorted(coins)))
        os.replace(str(tmp), str(self._persistent_skip_file))

    # ── Layer 1 Risk Checks ──────────────────────────────────────

    def _check_drawdown(self) -> bool:
        """Check if drawdown exceeds max_drawdown_pct. Returns True if halted."""
        if self._original_start_equity <= 0:
            return False

        # Update peak
        if self.our_equity > self._peak_equity:
            self._peak_equity = self.our_equity

        drawdown_pct = (self._peak_equity - self.our_equity) / self._peak_equity * 100
        if drawdown_pct >= self.leader.max_drawdown_pct:
            if not self._halted:
                self._halted = True
                log.warning(
                    f"RISK: Drawdown {drawdown_pct:.1f}% >= {self.leader.max_drawdown_pct}% "
                    f"— SOFT FREEZE (no new opens)"
                )
                self.notifier.notify_error(
                    f"SOFT FREEZE: DD {drawdown_pct:.1f}% >= {self.leader.max_drawdown_pct}%"
                )
            return True

        # Un-halt only if drawdown recovers well below threshold (H4 fix: 10% gap prevents oscillation)
        unhalt_threshold = self.leader.max_drawdown_pct - 10
        if self._halted and drawdown_pct < unhalt_threshold:
            self._halted = False
            log.info(f"RISK: Drawdown recovered to {drawdown_pct:.1f}% (< {unhalt_threshold:.0f}%) — unfreezing")
            self.notifier.notify_status(f"Unfrozen: DD recovered to {drawdown_pct:.1f}%")

        return self._halted

    def _check_daily_loss(self) -> bool:
        """Check if daily loss exceeds limit. Returns True if blocked."""
        today = time.strftime("%Y-%m-%d", time.gmtime())  # UTC-based daily reset
        if today != self._daily_start_date:
            self._daily_start_equity = self.our_equity
            self._daily_start_date = today
            return False

        if self._daily_start_equity <= 0:
            return False

        daily_loss_pct = (self._daily_start_equity - self.our_equity) / self._daily_start_equity * 100
        if daily_loss_pct >= self.leader.daily_loss_limit_pct:
            log.warning(
                f"RISK: Daily loss {daily_loss_pct:.1f}% >= {self.leader.daily_loss_limit_pct}% "
                f"— blocking new opens"
            )
            return True
        return False

    def get_drawdown_pct(self) -> float:
        """Current drawdown from peak, as positive percentage."""
        if self._peak_equity <= 0:
            return 0.0
        return (self._peak_equity - self.our_equity) / self._peak_equity * 100

    # ── Live SDK Init ────────────────────────────────────────────

    def init_live(self):
        """Initialize Hyperliquid SDK for live trading."""
        global _exchange_cls, _info_cls, _account_cls
        from eth_account import Account as _account_cls
        from hyperliquid.exchange import Exchange as _exchange_cls
        from hyperliquid.info import Info as _info_cls

        wallet = _account_cls.from_key(self.global_cfg.private_key)
        sub = self.leader.sub_account or None

        perp_dexes = [""] + self.global_cfg.perp_dexes
        self._info = _info_cls(base_url=self.global_cfg.api_url, skip_ws=True, perp_dexs=perp_dexes)
        self._exchange = _exchange_cls(
            wallet=wallet,
            base_url=self.global_cfg.api_url,
            vault_address=sub,
            account_address=sub,
            perp_dexs=perp_dexes,
        )

        for dex in perp_dexes:
            meta = self._info.meta(dex=dex)
            for asset_info in meta["universe"]:
                name = asset_info["name"]
                self._sz_decimals[name] = asset_info["szDecimals"]

        log.info(
            f"Live SDK initialized: wallet={wallet.address[:10]}... "
            f"sub={sub[:10] + '...' if sub else 'none'} "
            f"assets={len(self._sz_decimals)} "
            f"perp_dexes={perp_dexes}"
        )

    def _round_size(self, coin: str, sz: float) -> float:
        decimals = self._sz_decimals.get(coin, 4)
        return round(sz, decimals)

    def _round_price(self, px: float) -> float:
        if px == 0:
            return 0.0
        return float(f"{px:.5g}")

    # ── Leverage Matching ──────────────────────────────────────

    async def _match_leverage(self, coin: str):
        """Match our leverage to the leader's for this coin.

        Paper mode: track leverage (no API call needed).
        Live perps: call exchange.update_leverage.
        Live spot (HIP-3): leverage is set differently (spot margin).
        """
        target_lev = self._target_leverage.get(coin)
        if not target_lev:
            return

        our_lev = self._our_leverage.get(coin)
        if our_lev and our_lev == target_lev:
            return

        lev_value = target_lev["value"]
        is_cross = target_lev["type"] == "cross"

        # Paper mode or spot: just track the leverage, don't call API
        if self.leader.paper_mode or self._is_spot(coin):
            self._our_leverage[coin] = target_lev.copy()
            log.info(f"LEVERAGE: {coin} matched to {lev_value}x {'cross' if is_cross else 'isolated'} (tracked)")
            return

        # Live perps: actually set leverage on exchange
        if not self._exchange:
            return

        try:
            result = await asyncio.get_running_loop().run_in_executor(
                None,
                lambda: self._exchange.update_leverage(
                    leverage=lev_value, name=coin, is_cross=is_cross,
                ),
            )
            status = result.get("status", "")
            if status == "ok":
                self._our_leverage[coin] = target_lev.copy()
                log.info(f"LEVERAGE: {coin} set to {lev_value}x {'cross' if is_cross else 'isolated'}")
            else:
                log.warning(f"LEVERAGE: {coin} update failed: {result}")
        except Exception:
            log.warning(f"LEVERAGE: failed to set {coin} to {lev_value}x", exc_info=True)

    # ── Spot (HIP-3) Detection ─────────────────────────────────

    @staticmethod
    def _is_spot(coin: str) -> bool:
        """Check if a coin is a spot pair (HIP-3 asset). Spot fills use '@NNN' naming."""
        return coin.startswith("@")

    # ── Public API ─────────────────────────────────────────────

    async def handle_fill_batch(self, batch: FillBatch):
        """Called by listener when a debounced fill batch is ready."""
        async with self._target_lock:
            async with self._target_lock:
                await self._handle_fill_batch_locked(batch)

        async def _handle_fill_batch_locked(self, batch: FillBatch):
            """Internal fill handler — runs under _target_lock."""
            if self.target_equity <= 0:
                log.warning("Target equity unknown, skipping copy")
                return

            coin = batch.coin
            direction = batch.direction

            # Update target position cache
            if direction in ("Open Long", "Open Short"):
                side = "long" if direction == "Open Long" else "short"
                if coin not in self._target_positions:
                    self._target_positions[coin] = {"side": side, "size": 0}
                tp = self._target_positions[coin]
                if tp.get("side") != side:
                    tp["side"] = side
                    tp["size"] = batch.total_size
                else:
                    tp["size"] = tp.get("size", 0) + batch.total_size

            # Skip coins in skip set ONLY if we don't have a position.
            # If we already have the position, adds and reduces must still work.
            positions = self._our_positions()
            if self._skip_coins is not None and coin in self._skip_coins and coin not in positions:
                log.debug(f"SKIP: {coin} not in our positions, ignoring {direction}")
                return

            if self._skip_coins is None:
                log.debug(f"SKIP: waiting for initial sync, ignoring {coin} {direction}")
                return

            # Track fill timestamp (for liveness monitoring)
            self._last_fill_ts = time.time()

            # Layer 1 risk: block new opens if halted or daily loss exceeded
            if direction in ("Open Long", "Open Short"):
                if self._check_drawdown():
                    log.info(f"RISK: blocking open {coin} — soft freeze active")
                    return
                if self._check_daily_loss():
                    log.info(f"RISK: blocking open {coin} — daily loss limit hit")
                    return

            # Block new opens at zero equity, but always allow closes
            if self.our_equity <= 0 and direction in ("Open Long", "Open Short"):
                if not self._min_equity_warned:
                    log.error(f"Our equity is ${self.our_equity:.2f} — blocking new opens")
                    self._min_equity_warned = True
                return

            # Track if this is a close (for deferred target update)
            is_close = direction in ("Close Long", "Close Short")

            # Liquidation = forced 100% close (bypass proportional sizing)
            positions = self._our_positions()
            if direction == "Liquidation":
                if coin in positions:
                    pos_side = positions[coin].side
                    our_size = positions[coin].size  # 100% of our position
                    batch = FillBatch(
                        coin=coin, side=batch.side,
                        direction=f"Close {'Long' if pos_side == 'long' else 'Short'}",
                        total_size=batch.total_size, avg_price=batch.avg_price,
                        first_price=batch.first_price, count=batch.count, first_seen=batch.first_seen,
                    )
                    direction = batch.direction
                    is_close = True
                    if coin in self._target_positions:
                        self._target_positions[coin]["size"] = 0
                    log.warning(f"LIQUIDATION detected for {coin} — closing 100% ({our_size:.6f})")
                else:
                    return
            elif is_close:
                # Proportional close: close same % as target (does NOT mutate target state)
                our_size = self._calc_close_size(coin, batch.total_size)
            else:
                # Open: match leverage then size proportionally
                await self._match_leverage(coin)
                target_notional = batch.total_size * batch.avg_price
                target_pct = target_notional / self.target_equity
                our_notional = target_pct * self.our_equity
                our_size = our_notional / batch.avg_price if batch.avg_price > 0 else 0

            if our_size <= 0:
                return

            # H1 fix: filter orders below min notional ($10) — silently dropped by exchange
            notional = our_size * batch.avg_price
            if notional < 10.0 and not is_close:
                log.debug(f"SKIP: {coin} {direction} notional ${notional:.2f} below $10 minimum")
                return

            log.info(
                f"COPY: {coin} {direction} | "
                f"target={batch.total_size:.6f}@{batch.avg_price:.2f} -> "
                f"ours={our_size:.6f}"
            )

        if self.leader.paper_mode:
            await self._paper_execute(batch, our_size)
        elif self._is_spot(coin):
            log.info(f"SPOT: {coin} is a HIP-3 spot pair — live spot execution not yet supported, skipping")
        else:
            await self._live_execute(batch, our_size)

        # Update target position cache AFTER successful execution (K1 fix)
        if is_close:
            self._apply_target_close(coin, batch.total_size)

        # Add newly copied positions to skip set + file (safety net:
        # if our position closes unexpectedly, drift correction won't re-open it.
        # Reconciler removes the coin when leader fully closes.)
        if direction in ("Open Long", "Open Short") and self._skip_coins is not None:
            if coin not in self._skip_coins:
                self._skip_coins.add(coin)
                self.save_persistent_skip_coins(self._skip_coins)

    def _our_positions(self) -> dict[str, Position]:
        return self.paper_positions if self.leader.paper_mode else self.live_positions

    def _calc_close_size(self, coin: str, target_close_size: float) -> float:
        """Calculate proportional close size. Does NOT mutate _target_positions.
        Caller must call _apply_target_close() after successful execution."""
        positions = self._our_positions()
        if coin not in positions:
            return 0.0

        target_pos = self._target_positions.get(coin, {})
        target_full_size = target_pos.get("size", 0)

        if target_full_size <= 0:
            log.warning(f"Target size unknown for {coin}, skipping close")
            return 0.0

        close_pct = min(target_close_size / target_full_size, 1.0)
        our_close = positions[coin].size * close_pct
        return our_close

    def _apply_target_close(self, coin: str, target_close_size: float):
        """Update target position cache AFTER successful close execution."""
        target_pos = self._target_positions.get(coin, {})
        target_full_size = target_pos.get("size", 0)
        self._target_positions[coin] = {
            **target_pos,
            "size": max(target_full_size - target_close_size, 0),
        }

    def get_our_positions(self) -> dict[str, Position]:
        return dict(self._our_positions())

    # ── Paper Equity (mark-to-market) ─────────────────────────

    def _calc_paper_equity(self, current_prices: dict[str, float] | None = None) -> float:
        """Calculate paper equity as cash + unrealized PnL, floored at 0.
        If current_prices not provided, uses entry prices (no mark-to-market)."""
        unrealized = 0.0
        for coin, pos in self.paper_positions.items():
            price = (current_prices or {}).get(coin, pos.entry_price)
            if pos.side == "long":
                unrealized += (price - pos.entry_price) * pos.size
            else:
                unrealized += (pos.entry_price - price) * pos.size
        return max(0.0, self._paper_cash + unrealized)

    def update_paper_mark_to_market(self, current_prices: dict[str, float]):
        """Called by reconciler every 30s to update paper equity with current prices."""
        self._last_current_prices.update(current_prices)
        self.paper_equity = self._calc_paper_equity(current_prices)
        self.our_equity = self.paper_equity

    # ── Paper Execution ────────────────────────────────────────

    async def _paper_execute(self, batch: FillBatch, size: float):
        coin = batch.coin
        direction = batch.direction
        price = batch.avg_price
        pnl = 0.0
        actual_size = size

        if direction in ("Open Long", "Open Short"):
            side = "long" if direction == "Open Long" else "short"
            if coin in self.paper_positions:
                pos = self.paper_positions[coin]
                if pos.side == side:
                    total_val = pos.entry_price * pos.size + price * size
                    pos.size += size
                    pos.entry_price = total_val / pos.size if pos.size else price
                else:
                    log.warning(f"Ignoring {direction} — we hold {pos.side} {coin}")
                    return
            else:
                self.paper_positions[coin] = Position(
                    coin=coin, side=side, size=size, entry_price=price
                )

        elif direction in ("Close Long", "Close Short"):
            if coin in self.paper_positions:
                pos = self.paper_positions[coin]
                expected_side = "long" if direction == "Close Long" else "short"
                if pos.side != expected_side:
                    log.warning(f"Ignoring {direction} — we hold {pos.side} {coin}")
                    return
                close_size = min(size, pos.size)
                actual_size = close_size
                if pos.side == "long":
                    pnl = (price - pos.entry_price) * close_size
                else:
                    pnl = (pos.entry_price - price) * close_size
                pos.size = max(0.0, pos.size - close_size)
                decimals = self._sz_decimals.get(coin, 4)
                min_tick = 1.0 / (10 ** decimals)
                if pos.size < min_tick:
                    del self.paper_positions[coin]

        fee = price * actual_size * TAKER_FEE
        net_pnl = pnl - fee
        self._paper_cash += net_pnl
        self.paper_equity = self._calc_paper_equity()
        self.our_equity = self.paper_equity

        trade = PaperTrade(
            timestamp=time.time(), coin=coin, direction=direction,
            size=actual_size, price=price, pnl=pnl, fee=fee,
        )
        self.paper_trades.append(trade)
        self.total_trade_count += 1

        pnl_str = f" PnL=${net_pnl:+.2f}" if net_pnl != 0 else ""
        log.info(
            f"PAPER {direction} {actual_size:.6f} {coin} @ ${price:,.2f}{pnl_str} | "
            f"Equity: ${self.paper_equity:,.2f}"
        )

        self._notify_trade(coin, direction, size, price, net_pnl)
        self._write_state()

    # ── Live Execution ─────────────────────────────────────────

    async def _live_execute(self, batch: FillBatch, size: float):
        if not self._exchange:
            log.error("Live SDK not initialized, skipping order")
            return

        coin = batch.coin
        direction = batch.direction
        is_buy = direction in ("Open Long", "Close Short")
        is_close = direction in ("Close Long", "Close Short")

        # Leverage matching is now handled in handle_fill_batch (for all modes)

        rounded_size = self._round_size(coin, size)
        if rounded_size <= 0:
            log.warning(f"Size {size} rounds to 0 for {coin}, skipping")
            return

        slippage = self.leader.slippage_cap_pct / 100.0

        log.info(
            f"LIVE {direction} {rounded_size:.6f} {coin} | "
            f"slippage={self.leader.slippage_cap_pct}% reduce_only={is_close}"
        )

        # Retry logic for closes (K3 fix: IOC close can fail, retry up to 3x)
        max_attempts = 3 if is_close else 1
        result = None

        for attempt in range(1, max_attempts + 1):
            try:
                def _do_order(c=coin, buy=is_buy, sz=rounded_size, sl=slippage, px=batch.avg_price, close=is_close):
                    if not close:
                        return self._exchange.market_open(c, is_buy=buy, sz=sz, slippage=sl, px=px)
                    else:
                        limit_price = self._round_price(px * (1 + sl) if buy else px * (1 - sl))
                        return self._exchange.order(
                            c, is_buy=buy, sz=sz, limit_px=limit_price,
                            order_type={"limit": {"tif": "Ioc"}},
                            reduce_only=True
                        )
                result = await asyncio.get_running_loop().run_in_executor(None, _do_order)
            except Exception:
                log.exception(f"LIVE order failed for {coin} {direction} (attempt {attempt}/{max_attempts})")
                if attempt < max_attempts:
                    await asyncio.sleep(1.0)
                    continue
                self._notify_trade(coin, direction, rounded_size, batch.avg_price, 0)
                if is_close:
                    self.notifier.notify_error(
                        f"⚠️ CLOSE FAILED after {max_attempts} retries: {coin} {rounded_size:.6f}"
                    )
                return

            status = result.get("status", "")
            if status == "ok":
                break  # Success
            log.error(f"LIVE order rejected (attempt {attempt}/{max_attempts}): {result}")
            if attempt < max_attempts:
                await asyncio.sleep(1.0)
            else:
                self._notify_trade(coin, direction, rounded_size, batch.avg_price, 0)
                if is_close:
                    self.notifier.notify_error(
                        f"⚠️ CLOSE REJECTED after {max_attempts} retries: {coin} {rounded_size:.6f}"
                    )
                return

        statuses = (result.get("response", {}).get("data", {}).get("statuses", []))
        if not statuses:
            log.error(f"LIVE order: no statuses in response: {result}")
            return

        order_status = statuses[0]
        if "filled" in order_status:
            filled = order_status["filled"]
            fill_sz = _safe_float(filled.get("totalSz", "0"))
            fill_px = _safe_float(filled.get("avgPx", "0"))
            log.info(f"LIVE FILLED: {direction} {fill_sz:.6f} {coin} @ ${fill_px:,.2f}")
            self.total_trade_count += 1
            self._notify_trade(coin, direction, fill_sz, fill_px, 0)
            self._update_live_position(coin, direction, fill_sz, fill_px)
        elif "resting" in order_status:
            oid = order_status["resting"].get("oid", "?")
            log.warning(f"LIVE order resting (IOC should not rest): oid={oid}")
        elif "error" in order_status:
            log.error(f"LIVE order error: {order_status['error']}")
            if is_close:
                self.notifier.notify_error(
                    f"⚠️ CLOSE ERROR: {coin} — {order_status['error']}"
                )
        else:
            log.warning(f"LIVE order unknown status: {order_status}")

        self._write_state()

    def _update_live_position(self, coin: str, direction: str, size: float, price: float):
        if direction in ("Open Long", "Open Short"):
            side = "long" if direction == "Open Long" else "short"
            if coin in self.live_positions:
                pos = self.live_positions[coin]
                if pos.side == side:
                    total_val = pos.entry_price * pos.size + price * size
                    pos.size += size
                    pos.entry_price = total_val / pos.size if pos.size else price
            else:
                self.live_positions[coin] = Position(
                    coin=coin, side=side, size=size, entry_price=price,
                )
        elif direction in ("Close Long", "Close Short"):
            if coin in self.live_positions:
                pos = self.live_positions[coin]
                pos.size = max(0.0, pos.size - size)
                decimals = self._sz_decimals.get(coin, 4)
                min_tick = 1.0 / (10 ** decimals)
                if pos.size < min_tick:
                    del self.live_positions[coin]

    # ── Paper State Sync ───────────────────────────────────────

    async def sync_paper_to_target(self, target_positions: dict[str, dict],
                                   current_prices: dict[str, float]):
        """Reconciler calls this to sync paper positions with target.
        Runs under _target_lock to prevent race with handle_fill_batch."""
        async with self._target_lock:
            await self._sync_paper_to_target_locked(target_positions, current_prices)

    async def _sync_paper_to_target_locked(self, target_positions: dict[str, dict],
                                            current_prices: dict[str, float]):
        """Internal sync — runs under _target_lock."""
        self._target_positions = target_positions
        skip = self._skip_coins or set()
        target_coins = set(target_positions.keys()) - skip
        our_coins = set(self.paper_positions.keys()) - skip

        # Update skip set: if leader fully closed a skipped coin, unblock it
        for coin in list(skip):
            if coin not in target_positions:
                skip.discard(coin)
                log.info(f"SKIP REMOVED: {coin} — leader closed position")
        # Persist skip set to file (auto-managed)
        self.save_persistent_skip_coins(skip)

        # Close positions target no longer has
        for coin in our_coins - target_coins:
            pos = self.paper_positions[coin]
            price = current_prices.get(coin, pos.entry_price)
            if pos.side == "long":
                pnl = (price - pos.entry_price) * pos.size
            else:
                pnl = (pos.entry_price - price) * pos.size
            fee = price * pos.size * TAKER_FEE
            net_pnl = pnl - fee
            self._paper_cash += net_pnl
            direction = f"Close {'Long' if pos.side == 'long' else 'Short'}"
            log.warning(
                f"DRIFT: closing orphan {coin} ({pos.side} {pos.size:.6f}) "
                f"PnL=${net_pnl:+.2f}"
            )
            self.paper_trades.append(PaperTrade(
                timestamp=time.time(), coin=coin, direction=direction,
                size=pos.size, price=price, pnl=pnl, fee=fee,
            ))
            self.total_trade_count += 1
            del self.paper_positions[coin]

        # Detect side flips
        for coin in target_coins & our_coins:
            tp = target_positions[coin]
            pos = self.paper_positions[coin]
            if tp["side"] != pos.side:
                price = current_prices.get(coin, pos.entry_price)
                if pos.side == "long":
                    pnl = (price - pos.entry_price) * pos.size
                else:
                    pnl = (pos.entry_price - price) * pos.size
                fee = price * pos.size * TAKER_FEE
                net_pnl = pnl - fee
                self._paper_cash += net_pnl
                direction = f"Close {'Long' if pos.side == 'long' else 'Short'}"
                log.warning(
                    f"DRIFT: side flip {coin} {pos.side}->{tp['side']} "
                    f"closing {pos.size:.6f} PnL=${net_pnl:+.2f}"
                )
                self.paper_trades.append(PaperTrade(
                    timestamp=time.time(), coin=coin, direction=direction,
                    size=pos.size, price=price, pnl=pnl, fee=fee,
                ))
                self.total_trade_count += 1
                del self.paper_positions[coin]
                our_coins.discard(coin)

        # H3 fix: Paper drift correction with hysteresis
        # Open/increase threshold: 15% (prevents flapping on small movements)
        # Close/decrease threshold: 5% (tighter to prevent bleed)
        for coin in target_coins & our_coins:
            tp = target_positions[coin]
            pos = self.paper_positions[coin]
            price = current_prices.get(coin, pos.entry_price)
            if self.target_equity <= 0 or self.our_equity <= 0 or price <= 0:
                continue
            if pos.size <= 0:
                continue
            target_notional = tp["size"] * price
            target_pct = target_notional / self.target_equity
            expected_size = target_pct * self.our_equity / price
            if expected_size <= 0:
                continue
            drift_pct = abs(pos.size - expected_size) / pos.size
            delta = expected_size - pos.size
            is_increase = delta > 0
            threshold = 0.15 if is_increase else 0.05
            if drift_pct < threshold:
                continue
            # Filter sub-$10 corrections in paper mode
            if abs(delta) * price < 10:
                log.debug(f"DRIFT: {coin} delta ${abs(delta) * price:.2f} below $10 minimum")
                continue
            old_size = pos.size
            if delta < 0:
                close_delta = abs(delta)
                if pos.side == "long":
                    pnl = (price - pos.entry_price) * close_delta
                else:
                    pnl = (pos.entry_price - price) * close_delta
                fee = price * close_delta * TAKER_FEE
                net_pnl = pnl - fee
                self._paper_cash += net_pnl
                direction = f"Close {'Long' if pos.side == 'long' else 'Short'}"
            else:
                pnl = 0.0
                fee = price * delta * TAKER_FEE
                self._paper_cash -= fee
                direction = f"Open {'Long' if pos.side == 'long' else 'Short'}"
                net_pnl = -fee
                total_val = pos.entry_price * pos.size + price * delta
                pos.entry_price = total_val / expected_size if expected_size else price
            pos.size = expected_size
            log.warning(
                f"DRIFT: size correction {coin} {old_size:.6f}->{expected_size:.6f} "
                f"({pos.side}) PnL=${net_pnl:+.2f}"
            )
            self.paper_trades.append(PaperTrade(
                timestamp=time.time(), coin=coin, direction=direction,
                size=abs(delta), price=price, pnl=pnl, fee=fee,
            ))
            self.total_trade_count += 1

        # Open positions we're missing
        for coin in target_coins - our_coins:
            tp = target_positions[coin]
            if self.target_equity <= 0 or self.our_equity <= 0:
                continue
            price = current_prices.get(coin, tp["entry_price"])
            target_notional = tp["size"] * price
            target_pct = target_notional / self.target_equity
            our_notional = target_pct * self.our_equity
            our_size = our_notional / price if price > 0 else 0
            if our_size > 0:
                fee = price * our_size * TAKER_FEE
                self._paper_cash -= fee
                direction = f"Open {'Long' if tp['side'] == 'long' else 'Short'}"
                log.warning(
                    f"DRIFT: opening missing {coin} "
                    f"({tp['side']} {our_size:.6f}@${price:,.2f} fee=${fee:.2f})"
                )
                self.paper_trades.append(PaperTrade(
                    timestamp=time.time(), coin=coin, direction=direction,
                    size=our_size, price=price, pnl=0.0, fee=fee,
                ))
                self.total_trade_count += 1
                self.paper_positions[coin] = Position(
                    coin=coin, side=tp["side"], size=our_size, entry_price=price,
                )

        # Mark-to-market: recalculate paper equity with current prices (K2 fix)
        self.paper_equity = self._calc_paper_equity(current_prices)
        self.our_equity = self.paper_equity
        self._write_state()

    # ── Live State Sync ──────────────────────────────────────────

    def sync_live_state(self, our_state: dict, spot_state: dict | None = None):
        margin = our_state.get("marginSummary", our_state.get("crossMarginSummary", {}))
        account_value = _safe_float(margin.get("accountValue", "0"))

        spot_usdc = 0.0
        if spot_state:
            for bal in spot_state.get("balances", []):
                if bal.get("coin") == "USDC":
                    spot_usdc = _safe_float(bal.get("total", "0"))
                    break

        total_equity = account_value + spot_usdc
        if total_equity != 0 or self.our_equity != 0:
            self.our_equity = total_equity

        self.live_positions.clear()
        for pos_data in our_state.get("assetPositions", []):
            p = pos_data.get("position", {})
            coin = p.get("coin", "")
            szi = _safe_float(p.get("szi", "0"))
            entry_px = _safe_float(p.get("entryPx", "0"))
            upnl = _safe_float(p.get("unrealizedPnl", "0"))
            if coin and abs(szi) > 0:
                self.live_positions[coin] = Position(
                    coin=coin,
                    side="long" if szi > 0 else "short",
                    size=abs(szi),
                    entry_price=entry_px,
                    unrealized_pnl=upnl,
                )

    def sync_live_state_merge(self, dex_state: dict):
        """Merge HIP-3 DEX positions into live_positions (additive, no clear)."""
        for pos_data in dex_state.get("assetPositions", []):
            p = pos_data.get("position", {})
            coin = p.get("coin", "")
            szi = _safe_float(p.get("szi", "0"))
            entry_px = _safe_float(p.get("entryPx", "0"))
            upnl = _safe_float(p.get("unrealizedPnl", "0"))
            if coin and abs(szi) > 0:
                self.live_positions[coin] = Position(
                    coin=coin,
                    side="long" if szi > 0 else "short",
                    size=abs(szi),
                    entry_price=entry_px,
                    unrealized_pnl=upnl,
                )

    # ── Notifications & State ─────────────────────────────────

    def _notify_trade(self, coin: str, direction: str, size: float, price: float, pnl: float):
        leverage = self._target_leverage.get(coin)
        equity = self.our_equity if not self.leader.paper_mode else self.paper_equity

        positions = self.live_positions if not self.leader.paper_mode else self.paper_positions
        existing = positions.get(coin)

        if direction in ("Open Long", "Open Short") and existing and existing.size > 0:
            action = "add"
            total_size = existing.size
        elif direction in ("Close Long", "Close Short") and existing and existing.size > 0:
            action = "reduce"
            total_size = existing.size
        else:
            action = None
            total_size = 0

        self.notifier.notify_trade(
            coin=coin, direction=direction, size=size, price=price,
            pnl=pnl, leverage=leverage, equity=equity,
            action=action, total_size=total_size,
        )

    def _write_state(self):
        """Trigger background disk write to avoid blocking event loop."""
        try:
            asyncio.get_running_loop().run_in_executor(None, self._write_state_sync)
        except Exception:
            self._write_state_sync()

    def _write_state_sync(self):
        """Write current state to state/{name}.json — shared contract with Krabje's scripts."""
        try:
            positions = self._our_positions()
            # H9-1: Clean up stale prices for closed positions
            self._last_current_prices = {
                k: v for k, v in self._last_current_prices.items() if k in positions
            }
            state = {
                "version": 2,
                "target": self.leader.name,
                "target_wallet": self.leader.target_wallet,
                "timestamp": time.time(),
                "equity": self.our_equity,
                "paper_cash": self._paper_cash if self.leader.paper_mode else None,
                "original_start_equity": self._original_start_equity,
                "peak_equity": self._peak_equity,
                "drawdown_pct": -self.get_drawdown_pct(),
                "daily_pnl": self.our_equity - self._daily_start_equity,
                "positions": {
                    coin: {
                        "side": p.side,
                        "size": p.size,
                        "entry_price": p.entry_price,
                        "current_price": self._last_current_prices.get(coin, p.entry_price),
                        "leverage": self._target_leverage.get(coin, {}),
                    }
                    for coin, p in positions.items()
                },
                "is_alive": True,
                "last_fill_ts": self._last_fill_ts,
                "halted": self._halted,
                "mode": "paper" if self.leader.paper_mode else "live",
                "trade_count": self.total_trade_count,
                "target_equity": self.target_equity,
            }
            state_path = self._state_dir / f"{self.leader.name}.json"
            tmp_path = state_path.with_suffix(".tmp")
            state_bytes = orjson.dumps(state, option=orjson.OPT_INDENT_2)
            tmp_path.write_bytes(state_bytes)
            os.replace(str(tmp_path), str(state_path))

            # Periodic backup (max 1 per 30 min, keep 48)
            backup_dir = self._state_dir / "backups"
            backup_dir.mkdir(exist_ok=True)
            last_backup_marker = backup_dir / f".last_backup_{self.leader.name}"
            should_backup = True
            if last_backup_marker.exists():
                try:
                    should_backup = (time.time() - last_backup_marker.stat().st_mtime) > 1800
                except Exception:
                    pass
            if should_backup:
                ts = time.strftime("%Y%m%d_%H%M")
                backup_path = backup_dir / f"{self.leader.name}_{ts}.json"
                backup_path.write_bytes(state_bytes)
                last_backup_marker.write_text(str(time.time()))
                # Cleanup old backups (keep 48)
                backups = sorted(backup_dir.glob(f"{self.leader.name}_*.json"))
                for old in backups[:-48]:
                    old.unlink(missing_ok=True)
        except Exception:
            log.warning("Failed to write state file", exc_info=True)

    # ── Stats ──────────────────────────────────────────────────

    def stats_summary(self) -> str:
        positions = self._our_positions()
        if self.leader.paper_mode:
            total_pnl = self.paper_equity - self.paper_starting_equity
            pnl_pct = (total_pnl / self.paper_starting_equity) * 100 if self.paper_starting_equity else 0
            equity_str = f"${self.paper_equity:,.2f} ({pnl_pct:+.1f}%)"
        else:
            equity_str = f"${self.our_equity:,.2f}"
        dd_str = f" DD={self.get_drawdown_pct():.1f}%"
        halt_str = " [HALTED]" if self._halted else ""
        pos_str = ", ".join(
            f"{p.coin} {p.side} {p.size:.4f}@{p.entry_price:.2f}"
            for p in positions.values()
        ) or "flat"
        return (
            f"[{self.leader.name}] Equity: {equity_str}{dd_str}{halt_str} | "
            f"Trades: {self.total_trade_count} | "
            f"Positions: {pos_str}"
        )
