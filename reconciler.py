"""State reconciler — periodically syncs our positions with target's."""

import asyncio
import logging
from pathlib import Path

import httpx
import orjson

from config import LeaderConfig, GlobalConfig
from execution import ExecutionEngine

log = logging.getLogger(__name__)


def _safe_float(val, default: float = 0.0) -> float:
    """F1 fix: Safe float parsing for API responses that may return null/invalid."""
    try:
        return float(val) if val is not None else default
    except (ValueError, TypeError):
        return default


class Reconciler:
    """
    Periodically fetches the target wallet's full state via REST API
    and compares it with our positions. Corrects any drift.
    """

    def __init__(self, engine: ExecutionEngine, leader: LeaderConfig, global_cfg: GlobalConfig):
        self.engine = engine
        self.leader = leader
        self.global_cfg = global_cfg
        self._http = httpx.AsyncClient(base_url=global_cfg.api_url, timeout=10)
        self._running = False
        self._initial_sync_done = False
        # H8: Retry queue for orphan closes that failed (e.g. missing price)
        self._orphan_retry: set[str] = set()
        self._reconcile_lock = asyncio.Lock()

    async def start(self):
        self._running = True
        await self._reconcile()
        while self._running:
            await asyncio.sleep(self.leader.reconciler_interval_s)
            try:
                await self._reconcile()
            except Exception:
                log.exception("Reconciler error")

    async def force_sync(self):
        log.info("Force sync triggered")
        try:
            await self._reconcile()
        except Exception:
            log.exception("Force sync failed")

    async def stop(self):
        self._running = False
        await self._http.aclose()

    @staticmethod
    def _extract_positions(state: dict, target_positions: dict,
                           current_prices: dict, target_leverage: dict):
        """Extract positions, prices, and leverage from a clearinghouseState response."""
        for pos_data in state.get("assetPositions", []):
            p = pos_data.get("position", {})
            coin = p.get("coin", "")
            szi = _safe_float(p.get("szi", "0"))
            entry_px = _safe_float(p.get("entryPx", "0"))
            pos_value = _safe_float(p.get("positionValue", "0"))
            if coin and abs(szi) > 0:
                target_positions[coin] = {
                    "side": "long" if szi > 0 else "short",
                    "size": abs(szi),
                    "entry_price": entry_px,
                }
                if abs(pos_value) > 0:
                    current_prices[coin] = abs(pos_value) / abs(szi)
                lev = p.get("leverage", {})
                if lev:
                    target_leverage[coin] = {
                        "type": lev.get("type", "cross"),
                        "value": int(lev.get("value", 10)),
                    }

    async def _reconcile(self):
        if self._reconcile_lock.locked():
            return
        async with self._reconcile_lock:
            await self._reconcile_locked()

    async def _reconcile_locked(self):
        target_state = await self._fetch_clearinghouse_state(self.leader.target_wallet)
        if target_state is None:
            return

        # Fetch target HIP-3 states (once, used for both equity and positions)
        hip3_states = []
        hip3_av = 0.0
        for dex in self.global_cfg.perp_dexes:
            dex_state = await self._fetch_clearinghouse_state(self.leader.target_wallet, perp_dex=dex)
            if dex_state:
                hip3_states.append(dex_state)
                hip3_av += _safe_float(dex_state.get("marginSummary", {}).get("accountValue", "0"))

        # Update target equity: base perps AV + spot USDC + HIP-3 AVs - spot hold
        # Spot hold = USDC locked as isolated margin for HIP-3 positions
        margin_summary = target_state.get("marginSummary", {})
        perps_value = _safe_float(margin_summary.get("accountValue", "0"))

        spot_usdc, spot_hold = await self._fetch_spot_usdc_with_hold(self.leader.target_wallet)
        # Only subtract hold if positive (it's the margin locked for HIP-3 isolated positions)
        # Negative hold = spot margin short, not related to HIP-3
        hip3_hold = max(0, spot_hold) if hip3_av > 0 else 0
        account_value = perps_value + spot_usdc + hip3_av - hip3_hold

        old_eq = self.engine.target_equity
        if account_value != 0 or old_eq != 0:
            self.engine.target_equity = account_value
            if old_eq == 0 and account_value != 0:
                log.info(f"Target equity initialized: ${account_value:,.2f} (perps=${perps_value:,.0f} + spot=${spot_usdc:,.0f}" + (f" + hip3=${hip3_av:,.0f} - hold=${hip3_hold:,.0f}" if hip3_av > 0 else "") + ")")
            elif account_value == 0 and old_eq > 0:
                log.warning("Target equity dropped to $0 (liquidation?)")
            elif old_eq != 0 and abs(account_value - old_eq) / abs(old_eq) > 0.05:
                log.info(f"Target equity updated: ${old_eq:,.0f} -> ${account_value:,.0f}")

        # Build target positions map from base DEX + HIP-3 DEXes
        target_positions = {}
        current_prices = {}
        target_leverage = {}

        all_states = [target_state] + hip3_states
        for state in all_states:
            self._extract_positions(state, target_positions, current_prices, target_leverage)

        self.engine._target_leverage.update(target_leverage)

        if target_positions:
            pos_str = ", ".join(
                f"{c} {p['side']} {p['size']:.4f}@{p['entry_price']:.2f}"
                for c, p in target_positions.items()
            )
            log.debug(f"Target positions: {pos_str}")
        else:
            log.debug("Target: flat (no positions)")

        # Skip set initialization (first sync)
        # Merge persistent file (survives restarts) with leader's current positions
        if not self._initial_sync_done:
            self._initial_sync_done = True
            persistent = self.engine.load_persistent_skip_coins()
            self.engine._skip_coins = set(target_positions.keys()) | persistent
            self.engine.save_persistent_skip_coins(self.engine._skip_coins)
            if self.engine._skip_coins:
                coins_str = ", ".join(sorted(self.engine._skip_coins))
                log.info(f"SKIP SET initialized: {coins_str} (waiting for new positions)")

        # Guard against API glitches
        our_positions = self.engine._our_positions()
        if not target_positions and our_positions and account_value <= 0:
            log.warning("API returned 0 positions and 0 equity — likely glitch, skipping sync")
            return

        if self.leader.paper_mode:
            await self.engine.sync_paper_to_target(target_positions, current_prices)
        else:
            # Live mode: fetch our sub-account state
            synced = False
            if self.leader.sub_account and self.global_cfg.account_address:
                sub_data = await self._fetch_sub_account_state(
                    self.global_cfg.account_address, self.leader.sub_account
                )
                if sub_data:
                    perps = sub_data.get("clearinghouseState", {})
                    spot = sub_data.get("spotState")
                    self.engine.sync_live_state(perps, spot)
                    synced = True
            else:
                our_address = self.global_cfg.account_address
                if our_address:
                    our_state = await self._fetch_clearinghouse_state(our_address)
                    if our_state:
                        self.engine.sync_live_state(our_state)
                        synced = True

            # Also fetch our HIP-3 positions (separate clearinghouse per DEX)
            if synced:
                for dex in self.global_cfg.perp_dexes:
                    our_addr = self.leader.sub_account or self.global_cfg.account_address
                    if our_addr:
                        dex_state = await self._fetch_clearinghouse_state(our_addr, perp_dex=dex)
                        if dex_state:
                            self.engine.sync_live_state_merge(dex_state)

            if synced:
                # R1 fix: wrap live drift correction + target update in lock
                # Prevents race with listener fill handler re-opening closed positions
                async with self.engine._target_lock:
                    await self._live_drift_correction(target_positions, current_prices)
                    self.engine._target_positions = target_positions
            else:
                log.warning("Live state sync failed, skipping drift correction")
                self.engine._target_positions = target_positions

        # H9: Store current prices for state file (risk_checker uses these)
        self.engine._last_current_prices.update(current_prices)
        log.info(f"STATUS: {self.engine.stats_summary()}")
        self.engine._write_state()

    async def _live_drift_correction(self, target_positions: dict, current_prices: dict):
        tasks = []
        from listener import FillBatch

        if self.engine.our_equity <= 0 or self.engine.target_equity <= 0:
            return

        our_pos = dict(self.engine.live_positions)
        skip = self.engine._skip_coins or set()
        target_coins = set(target_positions.keys())
        our_coins = set(our_pos.keys())

        # Update skip set: remove coins leader fully closed
        for coin in list(skip):
            if coin not in target_positions:
                skip.discard(coin)
                log.info(f"SKIP REMOVED: {coin} — leader closed position")
        # Persist skip set to file (auto-managed)
        self.engine.save_persistent_skip_coins(skip)

        target_coins -= skip
        our_coins -= skip

        # Fetch mid prices for orphans
        missing_price_coins = our_coins - target_coins
        if missing_price_coins:
            await self._fetch_all_mids(missing_price_coins, current_prices)

        # H8: Add retry queue coins to orphan set (previous cycles' failures)
        orphans_to_close = (our_coins - target_coins) | (self._orphan_retry & our_coins)
        self._orphan_retry.clear()

        # Close orphans
        for coin in orphans_to_close:
            pos = our_pos.get(coin)
            if not pos:
                continue
            price = current_prices.get(coin)
            if not price:
                log.warning(f"DRIFT: orphan {coin} has no price — queued for retry")
                self._orphan_retry.add(coin)
                continue
            direction = f"Close {'Long' if pos.side == 'long' else 'Short'}"
            log.warning(f"LIVE DRIFT: closing orphan {coin} ({pos.side} {pos.size:.6f})")
            batch = FillBatch(
                coin=coin, side="Sell" if pos.side == "long" else "Buy",
                direction=direction, total_size=pos.size,
                avg_price=price, first_price=price, count=1,
            )
            tasks.append(self.engine._live_execute(batch, pos.size))

        # Side flips
        for coin in target_coins & our_coins:
            tp = target_positions[coin]
            pos = our_pos[coin]
            if tp["side"] != pos.side:
                price = current_prices.get(coin)
                if not price:
                    continue
                direction = f"Close {'Long' if pos.side == 'long' else 'Short'}"
                log.warning(f"LIVE DRIFT: side flip {coin} {pos.side}->{tp['side']}")
                batch = FillBatch(
                    coin=coin, side="Sell" if pos.side == "long" else "Buy",
                    direction=direction, total_size=pos.size,
                    avg_price=price, first_price=price, count=1,
                )
                tasks.append(self.engine._live_execute(batch, pos.size))
                our_coins.discard(coin)

        # H3 fix: Size drift correction with hysteresis
        # Increase threshold: 15%, decrease threshold: 5%
        for coin in target_coins & our_coins:
            tp = target_positions[coin]
            pos = our_pos.get(coin)
            if not pos or pos.size <= 0:
                continue
            price = current_prices.get(coin)
            if not price or price <= 0:
                continue
            target_notional = tp["size"] * price
            target_pct = target_notional / self.engine.target_equity
            expected_size = target_pct * self.engine.our_equity / price
            if expected_size <= 0:
                continue
            delta = expected_size - pos.size
            drift_pct = abs(delta) / pos.size
            threshold = 0.15 if delta > 0 else 0.05
            if drift_pct < threshold:
                continue
            if abs(delta) * price < 10:
                log.debug(f"DRIFT: {coin} delta ${abs(delta) * price:.2f} below $10 minimum")
                continue
            if delta < 0:
                close_delta = abs(delta)
                direction = f"Close {'Long' if pos.side == 'long' else 'Short'}"
                log.warning(f"LIVE DRIFT: shrinking {coin} by {close_delta:.6f}")
                batch = FillBatch(
                    coin=coin, side="Sell" if pos.side == "long" else "Buy",
                    direction=direction, total_size=close_delta,
                    avg_price=price, first_price=price, count=1,
                )
                tasks.append(self.engine._live_execute(batch, close_delta))
            else:
                direction = f"Open {'Long' if pos.side == 'long' else 'Short'}"
                log.warning(f"LIVE DRIFT: growing {coin} by {delta:.6f}")
                batch = FillBatch(
                    coin=coin, side="Buy" if pos.side == "long" else "Sell",
                    direction=direction, total_size=delta,
                    avg_price=price, first_price=price, count=1,
                )
                tasks.append(self.engine._live_execute(batch, delta))

        # Open missing positions
        for coin in target_coins - our_coins:
            tp = target_positions[coin]
            price = current_prices.get(coin)
            if not price or price <= 0:
                continue
            target_notional = tp["size"] * price
            target_pct = target_notional / self.engine.target_equity
            our_notional = target_pct * self.engine.our_equity
            our_size = our_notional / price
            if our_size > 0:
                direction = f"Open {'Long' if tp['side'] == 'long' else 'Short'}"
                log.warning(f"LIVE DRIFT: opening missing {coin} ({tp['side']} {our_size:.6f})")
                batch = FillBatch(
                    coin=coin, side="Buy" if tp["side"] == "long" else "Sell",
                    direction=direction, total_size=our_size,
                    avg_price=price, first_price=price, count=1,
                )
                tasks.append(self.engine._live_execute(batch, our_size))

        if tasks:
            await asyncio.gather(*tasks)

    async def _fetch_sub_account_state(self, master: str, sub: str) -> dict | None:
        try:
            resp = await self._http.post(
                "/info", json={"type": "subAccounts", "user": master},
            )
            resp.raise_for_status()
            subs = orjson.loads(resp.content)
            sub_lower = sub.lower()
            for s in subs:
                if s.get("subAccountUser", "").lower() == sub_lower:
                    return s
            log.warning(f"Sub-account {sub[:10]}... not found")
            return None
        except Exception:
            log.debug("Failed to fetch sub-account state", exc_info=True)
            return None

    async def _fetch_clearinghouse_state(self, address: str, perp_dex: str = "") -> dict | None:
        """Fetch user's clearinghouse state from Hyperliquid REST API.

        Args:
            perp_dex: HIP-3 DEX name (e.g. "xyz"). Empty string for base DEX.
        """
        try:
            payload = {"type": "clearinghouseState", "user": address}
            if perp_dex:
                payload["dex"] = perp_dex
            resp = await self._http.post("/info", json=payload)
            resp.raise_for_status()
            return orjson.loads(resp.content)
        except httpx.HTTPStatusError as e:
            if e.response.status_code == 429:
                log.warning("Rate limited on REST API, backing off")
                await asyncio.sleep(5)
            else:
                log.error(f"REST API error: {e.response.status_code}")
            return None
        except Exception:
            log.exception("Failed to fetch clearinghouse state")
            return None

    async def _fetch_all_mids(self, coins: set[str], current_prices: dict):
        """Fetch mid prices from base DEX and any HIP-3 DEXes that have missing coins."""
        dexes_needed = {""}  # Always query base DEX
        for coin in coins:
            if ":" in coin:
                dex_prefix = coin.split(":")[0]
                dexes_needed.add(dex_prefix)

        for dex in dexes_needed:
            try:
                payload = {"type": "allMids"}
                if dex:
                    payload["dex"] = dex
                resp = await self._http.post("/info", json=payload)
                resp.raise_for_status()
                all_mids = orjson.loads(resp.content)
                for coin in coins:
                    mid = all_mids.get(coin)
                    if mid and coin not in current_prices:
                        current_prices[coin] = float(mid)
            except Exception:
                log.debug(f"Failed to fetch allMids for dex={dex or 'base'}")

    async def _fetch_spot_usdc(self, address: str) -> float:
        """Fetch user's spot USDC balance. Returns total USDC (including hold)."""
        total, _ = await self._fetch_spot_usdc_with_hold(address)
        return total

    async def _fetch_spot_usdc_with_hold(self, address: str) -> tuple[float, float]:
        """Fetch user's spot USDC total and hold. Hold = margin locked for HIP-3 isolated positions."""
        try:
            resp = await self._http.post(
                "/info",
                json={"type": "spotClearinghouseState", "user": address},
            )
            resp.raise_for_status()
            data = orjson.loads(resp.content)
            for bal in data.get("balances", []):
                if bal.get("coin") == "USDC":
                    return _safe_float(bal.get("total", "0")), _safe_float(bal.get("hold", "0"))
            return 0.0, 0.0
        except Exception:
            log.debug("Failed to fetch spot USDC balance", exc_info=True)
            return 0.0, 0.0

