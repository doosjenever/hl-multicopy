"""WebSocket listener for target wallet fills on Hyperliquid."""

import asyncio
import json
import logging
import time
from collections import OrderedDict
from dataclasses import dataclass, field

import websockets
import orjson

log = logging.getLogger(__name__)

VALID_DIRECTIONS = {"Open Long", "Close Long", "Open Short", "Close Short", "Liquidation"}
MAX_DEBOUNCE_AGE_S = 0.5  # Hard deadline: flush batch after 500ms regardless of new fills


@dataclass
class Fill:
    """A single fill event from the target wallet."""
    coin: str
    side: str          # "Buy" or "Sell" (from target's perspective)
    size: float        # Absolute size filled
    price: float       # Fill price
    timestamp: int     # Server timestamp ms
    direction: str     # "Open Long", "Close Long", "Open Short", "Close Short"
    oid: int           # Order ID
    closed_pnl: float  # Realized PnL (0 if opening)


@dataclass
class FillBatch:
    """Accumulates partial fills for debouncing."""
    coin: str
    side: str
    direction: str
    total_size: float = 0.0
    avg_price: float = 0.0
    first_price: float = 0.0
    count: int = 0
    first_seen: float = 0.0

    def add(self, size: float, price: float):
        total_value = self.avg_price * self.total_size + price * size
        self.total_size += size
        self.avg_price = total_value / self.total_size if self.total_size else price
        if self.count == 0:
            self.first_price = price
            self.first_seen = time.time()
        self.count += 1


class Listener:
    """Subscribes to target wallet fills via WebSocket."""

    def __init__(self, target_wallet: str, ws_url: str, debounce_ms: int,
                 on_fill_batch, on_reconnect=None):
        """
        Args:
            target_wallet: Wallet address to subscribe to.
            ws_url: WebSocket URL.
            debounce_ms: Debounce window in milliseconds.
            on_fill_batch: async callback(FillBatch) called when a debounced
                           batch of fills is ready to be copied.
            on_reconnect: optional async callback() called after WS reconnect.
        """
        self.target_wallet = target_wallet
        self.ws_url = ws_url
        self.debounce_ms = debounce_ms
        self.on_fill_batch = on_fill_batch
        self.on_reconnect = on_reconnect
        self._pending: dict[str, FillBatch] = {}
        self._debounce_tasks: dict[str, asyncio.Task] = {}
        self._seen_oids: OrderedDict = OrderedDict()
        self._ws = None
        self._running = False
        self._connected_before = False

    async def start(self):
        """Connect and listen forever with auto-reconnect."""
        self._running = True
        backoff_s = 1.0
        while self._running:
            try:
                await self._connect_and_listen()
                backoff_s = 1.0
            except (websockets.ConnectionClosed, ConnectionError, OSError) as e:
                log.warning(f"WS disconnected: {e}. Reconnecting in {backoff_s}s...")
                self._clear_pending()
                await asyncio.sleep(backoff_s)
                backoff_s = min(60.0, backoff_s * 2)
            except Exception:
                log.exception(f"Unexpected listener error. Reconnecting in {backoff_s}s...")
                self._clear_pending()
                await asyncio.sleep(backoff_s)
                backoff_s = min(60.0, backoff_s * 2)

    async def stop(self):
        self._running = False
        self._clear_pending()
        if self._ws:
            await self._ws.close()

    def _clear_pending(self):
        for task in self._debounce_tasks.values():
            task.cancel()
        self._debounce_tasks.clear()
        self._pending.clear()

    async def _connect_and_listen(self):
        log.info(f"Connecting to {self.ws_url} for wallet {self.target_wallet[:10]}...")
        async with websockets.connect(
            self.ws_url,
            ping_interval=20,
            ping_timeout=20,
            close_timeout=5,
            max_size=10 * 1024 * 1024,
        ) as ws:
            self._ws = ws
            sub_msg = {
                "method": "subscribe",
                "subscription": {
                    "type": "userEvents",
                    "user": self.target_wallet,
                },
            }
            await ws.send(json.dumps(sub_msg))
            log.info("Subscribed to target userEvents")

            if self._connected_before and self.on_reconnect:
                log.info("Triggering force-sync after reconnect (background)")
                asyncio.get_running_loop().create_task(self._safe_reconnect_sync())
            self._connected_before = True

            ping_task = asyncio.get_running_loop().create_task(self._keep_alive(ws))

            try:
                async for raw in ws:
                    try:
                        msg = orjson.loads(raw)
                    except (orjson.JSONDecodeError, Exception):
                        log.warning("Malformed WS message, skipping")
                        continue
                    if msg.get("channel") == "pong":
                        continue
                    if msg.get("channel") == "userEvents":
                        self._handle_event(msg.get("data", {}))
            finally:
                ping_task.cancel()

    async def _safe_reconnect_sync(self):
        try:
            await self.on_reconnect()
        except Exception:
            log.exception("Force-sync after reconnect failed")

    async def _keep_alive(self, ws):
        while True:
            await asyncio.sleep(50)
            try:
                await ws.send(json.dumps({"method": "ping"}))
            except Exception:
                break

    def _handle_event(self, data: dict):
        fills = data.get("fills", [])
        if not fills:
            return

        for f in fills:
            try:
                fill = Fill(
                    coin=f["coin"],
                    side=f["side"],
                    size=float(f["sz"]),
                    price=float(f["px"]),
                    timestamp=f.get("time", 0),
                    direction=f.get("dir", ""),
                    oid=f.get("oid", 0),
                    closed_pnl=float(f.get("closedPnl", "0")),
                )
            except (KeyError, ValueError, TypeError):
                log.warning(f"Malformed fill: {f}")
                continue

            if fill.direction not in VALID_DIRECTIONS:
                log.warning(f"Unknown fill direction '{fill.direction}', skipping")
                continue

            if fill.oid in self._seen_oids:
                log.debug(f"Duplicate fill oid={fill.oid}, skipping")
                continue
            self._seen_oids[fill.oid] = True
            while len(self._seen_oids) > 10_000:
                self._seen_oids.popitem(last=False)

            log.info(f"FILL: {fill.coin} {fill.direction} {fill.size}@{fill.price}")
            self._accumulate(fill)

    def _accumulate(self, fill: Fill):
        key = f"{fill.coin}:{fill.side}:{fill.direction}"

        if key not in self._pending:
            self._pending[key] = FillBatch(
                coin=fill.coin,
                side=fill.side,
                direction=fill.direction,
            )

        batch = self._pending[key]
        batch.add(fill.size, fill.price)

        if batch.first_seen > 0 and (time.time() - batch.first_seen) >= MAX_DEBOUNCE_AGE_S:
            if key in self._debounce_tasks:
                self._debounce_tasks[key].cancel()
            task = asyncio.get_running_loop().create_task(self._flush_now(key))
            self._debounce_tasks[key] = task
            return

        if key in self._debounce_tasks:
            self._debounce_tasks[key].cancel()
        task = asyncio.get_running_loop().create_task(
            self._flush_after_debounce(key)
        )
        self._debounce_tasks[key] = task

    async def _flush_now(self, key: str):
        batch = self._pending.pop(key, None)
        self._debounce_tasks.pop(key, None)
        if batch and batch.total_size > 0:
            log.info(
                f"BATCH (deadline): {batch.coin} {batch.direction} "
                f"total={batch.total_size:.6f} avg_px={batch.avg_price:.2f} "
                f"fills={batch.count}"
            )
            try:
                await self.on_fill_batch(batch)
            except Exception:
                log.exception(f"Error in fill batch callback for {key}")

    async def _flush_after_debounce(self, key: str):
        await asyncio.sleep(self.debounce_ms / 1000.0)
        batch = self._pending.pop(key, None)
        self._debounce_tasks.pop(key, None)
        if batch and batch.total_size > 0:
            log.info(
                f"BATCH: {batch.coin} {batch.direction} "
                f"total={batch.total_size:.6f} avg_px={batch.avg_price:.2f} "
                f"fills={batch.count}"
            )
            try:
                await self.on_fill_batch(batch)
            except Exception:
                log.exception(f"Error in fill batch callback for {key}")
