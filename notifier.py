"""Telegram trade notifier — fire-and-forget alerts for copy trader events."""

import asyncio
import logging
from datetime import datetime, timezone

import httpx

log = logging.getLogger(__name__)


class TelegramNotifier:
    """Sends trade notifications to Telegram. Never blocks execution."""

    def __init__(self, leader_name: str, bot_token: str, chat_id: str):
        """
        Args:
            leader_name: Human-readable leader name for message prefix.
            bot_token: Telegram bot token (from env).
            chat_id: Telegram chat ID (from env).
        """
        self.leader_name = leader_name
        self._bot_token = bot_token
        self._chat_id = chat_id
        self._api_url = f"https://api.telegram.org/bot{bot_token}"
        self._http = httpx.AsyncClient(timeout=5)
        self._bg_tasks: set = set()
        self._enabled = bool(bot_token and chat_id)

    # ── Public API ─────────────────────────────────────────────

    def notify_trade(
        self,
        coin: str,
        direction: str,
        size: float,
        price: float,
        pnl: float = 0,
        leverage: dict | None = None,
        equity: float = 0,
        action: str | None = None,
        total_size: float = 0,
    ):
        """Fire-and-forget trade notification."""
        if not self._enabled:
            return
        try:
            task = asyncio.get_running_loop().create_task(
                self._send_trade(coin, direction, size, price, pnl, leverage, equity, action, total_size)
            )
            self._bg_tasks.add(task)
            task.add_done_callback(self._bg_tasks.discard)
        except Exception:
            log.debug("notify_trade: no running event loop", exc_info=True)

    def notify_status(self, message: str):
        """Fire-and-forget status notification."""
        if not self._enabled:
            return
        try:
            task = asyncio.get_running_loop().create_task(
                self._send(f"[{_esc(self.leader_name)}] {message}")
            )
            self._bg_tasks.add(task)
            task.add_done_callback(self._bg_tasks.discard)
        except Exception:
            log.debug("notify_status: no running event loop", exc_info=True)

    def notify_error(self, message: str):
        """Fire-and-forget error notification."""
        if not self._enabled:
            return
        try:
            task = asyncio.get_running_loop().create_task(
                self._send(f"[{_esc(self.leader_name)}] {_esc(message)}")
            )
            self._bg_tasks.add(task)
            task.add_done_callback(self._bg_tasks.discard)
        except Exception:
            log.debug("notify_error: no running event loop", exc_info=True)

    async def close(self):
        await self._http.aclose()

    # ── Internal ───────────────────────────────────────────────

    async def _send_trade(
        self,
        coin: str,
        direction: str,
        size: float,
        price: float,
        pnl: float,
        leverage: dict | None,
        equity: float,
        action: str | None = None,
        total_size: float = 0,
    ):
        if action == "add":
            emoji = "+"
            side = "LONG" if "Long" in direction else "SHORT"
            label = f"ADD {side}"
        elif action == "reduce":
            emoji = "-"
            side = "LONG" if "Long" in direction else "SHORT"
            label = f"REDUCE {side}"
        elif direction == "Open Long":
            emoji = ">"
            label = "OPEN LONG"
        elif direction == "Open Short":
            emoji = "<"
            label = "OPEN SHORT"
        elif direction == "Close Long":
            emoji = "x"
            label = "CLOSE LONG"
        elif direction == "Close Short":
            emoji = "x"
            label = "CLOSE SHORT"
        else:
            emoji = "~"
            label = direction.upper()

        notional = size * price
        now = datetime.now(timezone.utc).strftime("%H:%M UTC")

        lines = [
            f"[{_esc(self.leader_name)}] <b>{label}</b>",
            f"<b>{_esc(coin)}</b>",
            f"Size: {size:.6g} (${notional:,.2f})",
            f"Price: ${price:,.2f}",
        ]

        if action == "add" and total_size > 0:
            new_total = total_size + size
            lines.append(f"Position: {total_size:.6g} -> {new_total:.6g}")
        elif action == "reduce" and total_size > 0:
            remaining = max(0, total_size - size)
            lines.append(f"Position: {total_size:.6g} -> {remaining:.6g}")

        if leverage:
            lev_type = leverage.get("type", "cross")
            lev_val = leverage.get("value", "?")
            lines.append(f"Leverage: {lev_val}x {lev_type}")

        if pnl != 0:
            pnl_label = "+" if pnl > 0 else ""
            lines.append(f"PnL: ${pnl_label}{pnl:,.2f}")

        if equity > 0:
            lines.append(f"Equity: ${equity:,.2f}")

        lines.append(f"{now}")

        await self._send("\n".join(lines))

    async def _send(self, text: str):
        try:
            resp = await self._http.post(
                f"{self._api_url}/sendMessage",
                json={
                    "chat_id": self._chat_id,
                    "text": text,
                    "parse_mode": "HTML",
                    "disable_web_page_preview": True,
                },
            )
            if resp.status_code != 200:
                log.debug(f"Telegram API returned {resp.status_code}")
        except Exception:
            log.debug("Telegram notification failed", exc_info=True)


def _esc(text: str) -> str:
    """Escape HTML special characters."""
    return str(text).replace("&", "&amp;").replace("<", "&lt;").replace(">", "&gt;")
