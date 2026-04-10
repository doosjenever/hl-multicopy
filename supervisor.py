#!/usr/bin/env python3
"""Supervisor — enterprise process manager for hl-multicopy bots.

Single entry point that manages all bot processes with:
- Auto-restart on crash with exponential backoff
- SIGTERM propagation for graceful shutdown
- Telegram crash alerts (rate-limited)
- Heartbeat file for external health checks
- Command file interface for orchestrator integration
- Periodic tasks (health check)

Usage:
    python3 supervisor.py              # Start all bots from configs/
    python3 supervisor.py --dry-run    # Show what would start, don't run
"""

import asyncio
import json
import logging
import logging.handlers
import os
import signal
import subprocess
import sys
import time
from dataclasses import dataclass, field
from pathlib import Path

import httpx

# ── Config ──────────────────────────────────────────────────────

BASE_DIR = Path(__file__).parent
CONFIGS_DIR = BASE_DIR / "configs"
LOGS_DIR = BASE_DIR / "logs"
STATE_DIR = BASE_DIR / "state"
HEARTBEAT_FILE = STATE_DIR / ".supervisor_heartbeat"
PID_FILE = STATE_DIR / ".supervisor.pid"
CMD_FILE = STATE_DIR / ".supervisor_cmd"

TELEGRAM_BOT_TOKEN = os.environ.get("TELEGRAM_BOT_TOKEN", "")
TELEGRAM_CHAT_ID = os.environ.get("TELEGRAM_CHAT_ID", "")

# Restart policy
MIN_BACKOFF_S = 2
MAX_BACKOFF_S = 60
STABLE_AFTER_S = 120          # Reset backoff after 2 min stable
MAX_RAPID_RESTARTS = 10       # Halt bot after 10 rapid restarts
ALERT_COOLDOWN_S = 300        # Max 1 Telegram alert per bot per 5 min
HEARTBEAT_INTERVAL_S = 30
HEALTH_CHECK_INTERVAL_S = 60
CMD_POLL_INTERVAL_S = 2

log = logging.getLogger("supervisor")


# ── Data ────────────────────────────────────────────────────────

@dataclass
class BotProcess:
    name: str
    config_path: Path
    process: subprocess.Popen | None = None
    pid: int = 0
    restart_count: int = 0
    rapid_restart_count: int = 0
    last_start: float = 0
    last_crash: float = 0
    last_alert: float = 0
    backoff_s: float = MIN_BACKOFF_S
    halted: bool = False         # Permanently stopped (too many crashes)
    intentional_stop: bool = False  # Stopped by command, don't restart
    _log_file: object = None     # S1 fix: track log file handle for cleanup


# ── Telegram ────────────────────────────────────────────────────

def send_telegram(message: str):
    if not TELEGRAM_BOT_TOKEN or not TELEGRAM_CHAT_ID:
        log.info(f"[ALERT] {message}")
        return
    try:
        httpx.post(
            f"https://api.telegram.org/bot{TELEGRAM_BOT_TOKEN}/sendMessage",
            json={
                "chat_id": TELEGRAM_CHAT_ID,
                "text": message,
                "parse_mode": "HTML",
                "disable_web_page_preview": True,
            },
            timeout=10,
        )
    except Exception as e:
        log.warning(f"Telegram failed: {e}")


# ── Supervisor ──────────────────────────────────────────────────

class Supervisor:
    def __init__(self):
        self.bots: dict[str, BotProcess] = {}
        self._shutdown = False
        self._start_time = time.time()

    # ── Discovery ─────────────────────────────────────────────

    def discover_configs(self) -> list[Path]:
        """Find all valid bot configs (skip example.yaml)."""
        configs = []
        for f in sorted(CONFIGS_DIR.glob("*.yaml")):
            if f.name in ("example.yaml", "alice.yaml", "bob.yaml", "carol.yaml"):
                continue
            configs.append(f)
        return configs

    # ── Process Management ────────────────────────────────────

    def start_bot(self, name: str, config_path: Path) -> bool:
        """Start a single bot process."""
        if name in self.bots and self.bots[name].process and self.bots[name].process.poll() is None:
            log.warning(f"{name}: already running (PID {self.bots[name].pid})")
            return False

        log_path = LOGS_DIR / f"{name}.log"
        # S1 fix: close previous log file handle before opening new one
        existing = self.bots.get(name)
        if existing and existing._log_file:
            try:
                existing._log_file.close()
            except Exception:
                pass
        try:
            log_file = open(log_path, "a")
            proc = subprocess.Popen(
                [sys.executable, "-u", "main.py", "--config", str(config_path)],
                cwd=str(BASE_DIR),
                stdout=log_file,
                stderr=subprocess.STDOUT,
                preexec_fn=os.setsid if hasattr(os, 'setsid') else None,
            )
        except Exception as e:
            log.error(f"{name}: failed to start: {e}")
            return False

        bot = self.bots.get(name) or BotProcess(name=name, config_path=config_path)
        bot.process = proc
        bot.pid = proc.pid
        bot._log_file = log_file
        bot.last_start = time.time()
        bot.config_path = config_path
        bot.intentional_stop = False
        self.bots[name] = bot

        log.info(f"{name}: started (PID {proc.pid})")
        return True

    def stop_bot(self, name: str, graceful_timeout: float = 10.0):
        """Stop a bot with graceful SIGTERM -> SIGKILL fallback."""
        bot = self.bots.get(name)
        if not bot or not bot.process:
            return

        bot.intentional_stop = True
        proc = bot.process

        # S1 fix: close log file handle
        if bot._log_file:
            try:
                bot._log_file.close()
            except Exception:
                pass
            bot._log_file = None

        if proc.poll() is not None:
            return

        # SIGTERM first
        log.info(f"{name}: sending SIGTERM (PID {bot.pid})")
        try:
            os.killpg(os.getpgid(proc.pid), signal.SIGTERM)
        except (ProcessLookupError, PermissionError):
            try:
                proc.terminate()
            except ProcessLookupError:
                return

        # Wait for graceful exit
        deadline = time.time() + graceful_timeout
        while time.time() < deadline:
            if proc.poll() is not None:
                log.info(f"{name}: exited gracefully (code {proc.returncode})")
                return
            time.sleep(0.5)

        # Force kill
        log.warning(f"{name}: SIGKILL after {graceful_timeout}s timeout")
        try:
            os.killpg(os.getpgid(proc.pid), signal.SIGKILL)
        except (ProcessLookupError, PermissionError):
            try:
                proc.kill()
            except ProcessLookupError:
                pass
        proc.wait(timeout=5)

    def stop_all(self):
        """Stop all bots gracefully."""
        log.info(f"Stopping all {len(self.bots)} bots...")
        for name in list(self.bots):
            self.stop_bot(name)

    # ── Monitoring ────────────────────────────────────────────

    def check_processes(self):
        """Check all bot processes, restart crashed ones."""
        for name, bot in list(self.bots.items()):
            if bot.halted or bot.intentional_stop or not bot.process:
                continue

            rc = bot.process.poll()
            if rc is None:
                # Still running — reset rapid restart counter if stable
                if bot.rapid_restart_count > 0 and (time.time() - bot.last_start) > STABLE_AFTER_S:
                    bot.rapid_restart_count = 0
                    bot.backoff_s = MIN_BACKOFF_S
                continue

            # Process exited
            now = time.time()
            uptime = now - bot.last_start
            bot.last_crash = now
            bot.restart_count += 1

            if uptime < STABLE_AFTER_S:
                bot.rapid_restart_count += 1
            else:
                bot.rapid_restart_count = 0
                bot.backoff_s = MIN_BACKOFF_S

            log.error(f"{name}: crashed (code {rc}, uptime {uptime:.0f}s, "
                      f"restarts {bot.restart_count}, rapid {bot.rapid_restart_count})")

            # Alert (rate limited)
            if now - bot.last_alert > ALERT_COOLDOWN_S:
                bot.last_alert = now
                send_telegram(
                    f"🦊 <b>[CRASH] {name}</b>\n"
                    f"Exit code: {rc}\n"
                    f"Uptime: {uptime:.0f}s\n"
                    f"Total restarts: {bot.restart_count}\n"
                    f"{'Halting — too many rapid restarts!' if bot.rapid_restart_count >= MAX_RAPID_RESTARTS else f'Restarting in {bot.backoff_s:.0f}s...'}"
                )

            # Too many rapid restarts → halt
            if bot.rapid_restart_count >= MAX_RAPID_RESTARTS:
                bot.halted = True
                log.error(f"{name}: HALTED after {MAX_RAPID_RESTARTS} rapid restarts")
                continue

            # Schedule restart with backoff
            log.info(f"{name}: restarting in {bot.backoff_s:.0f}s...")
            time.sleep(bot.backoff_s)
            bot.backoff_s = min(bot.backoff_s * 2, MAX_BACKOFF_S)
            self.start_bot(name, bot.config_path)

    # ── Command Interface ─────────────────────────────────────

    def process_commands(self):
        """Read and execute commands from command file."""
        if not CMD_FILE.exists():
            return

        try:
            cmd_text = CMD_FILE.read_text().strip()
            CMD_FILE.unlink()
        except Exception:
            return

        if not cmd_text:
            return

        for line in cmd_text.splitlines():
            line = line.strip()
            if not line or line.startswith("#"):
                continue

            parts = line.split(":", 1)
            action = parts[0].strip().lower()
            target = parts[1].strip() if len(parts) > 1 else ""

            log.info(f"Command: {action} {target}")

            if action == "stop" and target:
                self.stop_bot(target)
            elif action == "start" and target:
                config = CONFIGS_DIR / f"{target}.yaml"
                if config.exists():
                    bot = self.bots.get(target)
                    if bot:
                        bot.intentional_stop = False
                        bot.halted = False
                        bot.rapid_restart_count = 0
                        bot.backoff_s = MIN_BACKOFF_S
                    self.start_bot(target, config)
                else:
                    log.warning(f"Command start {target}: config not found")
            elif action == "restart" and target:
                self.stop_bot(target)
                time.sleep(1)
                config = CONFIGS_DIR / f"{target}.yaml"
                if config.exists():
                    bot = self.bots.get(target)
                    if bot:
                        bot.intentional_stop = False
                        bot.halted = False
                    self.start_bot(target, config)
            elif action == "stop-all":
                self.stop_all()
            elif action == "restart-all":
                self.stop_all()
                time.sleep(2)
                for config_path in self.discover_configs():
                    name = config_path.stem
                    bot = self.bots.get(name)
                    if bot:
                        bot.intentional_stop = False
                        bot.halted = False
                    self.start_bot(name, config_path)
            elif action == "shutdown":
                self._shutdown = True
            else:
                log.warning(f"Unknown command: {line}")

    # ── Heartbeat ─────────────────────────────────────────────

    def write_heartbeat(self):
        """Write heartbeat file for external health checks."""
        status = {
            "timestamp": time.time(),
            "uptime_s": time.time() - self._start_time,
            "bots": {},
        }
        for name, bot in self.bots.items():
            running = bot.process and bot.process.poll() is None
            status["bots"][name] = {
                "running": running,
                "pid": bot.pid if running else 0,
                "restarts": bot.restart_count,
                "halted": bot.halted,
                "intentional_stop": bot.intentional_stop,
                "uptime_s": time.time() - bot.last_start if running and bot.last_start else 0,
            }

        try:
            tmp = HEARTBEAT_FILE.with_suffix(".tmp")
            tmp.write_text(json.dumps(status, indent=2))
            os.replace(str(tmp), str(HEARTBEAT_FILE))
        except Exception:
            pass

    # ── Main Loop ─────────────────────────────────────────────

    def run(self):
        """Main supervisor loop."""
        # Write PID
        STATE_DIR.mkdir(exist_ok=True)
        LOGS_DIR.mkdir(exist_ok=True)
        PID_FILE.write_text(str(os.getpid()))

        # Signal handling
        def handle_shutdown(signum, frame):
            sig_name = signal.Signals(signum).name
            log.info(f"Received {sig_name}, shutting down...")
            self._shutdown = True

        signal.signal(signal.SIGTERM, handle_shutdown)
        signal.signal(signal.SIGINT, handle_shutdown)

        # Start all bots
        configs = self.discover_configs()
        if not configs:
            log.error("No configs found in configs/. Nothing to supervise.")
            return

        log.info(f"Supervisor starting — {len(configs)} bots to manage")
        send_telegram(
            f"🦊 <b>Supervisor started</b>\n"
            f"Bots: {', '.join(c.stem for c in configs)}\n"
            f"PID: {os.getpid()}"
        )

        for config_path in configs:
            name = config_path.stem
            self.start_bot(name, config_path)
            time.sleep(0.5)  # Stagger starts

        last_heartbeat = 0
        last_health = 0

        # Main loop
        while not self._shutdown:
            now = time.time()

            # Check processes (restart crashed)
            self.check_processes()

            # Process commands
            self.process_commands()

            # Heartbeat
            if now - last_heartbeat > HEARTBEAT_INTERVAL_S:
                self.write_heartbeat()
                last_heartbeat = now

            # Health summary log + bot log rotation
            if now - last_health > HEALTH_CHECK_INTERVAL_S:
                running = sum(1 for b in self.bots.values()
                              if b.process and b.process.poll() is None)
                halted = sum(1 for b in self.bots.values() if b.halted)
                total = len(self.bots)
                log.info(f"Health: {running}/{total} running, {halted} halted, "
                         f"uptime {now - self._start_time:.0f}s")
                # S1 fix: rotate bot logs > 2MB (keep 1 backup)
                for bot in self.bots.values():
                    bot_log = LOGS_DIR / f"{bot.name}.log"
                    try:
                        if bot_log.exists() and bot_log.stat().st_size > 2 * 1024 * 1024:
                            backup = bot_log.with_suffix(".log.1")
                            if backup.exists():
                                backup.unlink()
                            bot_log.rename(backup)
                            # Reopen log file for the running process
                            if bot._log_file:
                                bot._log_file.close()
                            bot._log_file = open(bot_log, "a")
                            if bot.process and bot.process.poll() is None:
                                # Can't redirect existing process stdout, but new
                                # file handle will be used on next restart
                                pass
                            log.info(f"{bot.name}: log rotated (>2MB)")
                    except Exception as e:
                        log.debug(f"{bot.name}: log rotation failed: {e}")
                last_health = now

            time.sleep(CMD_POLL_INTERVAL_S)

        # Shutdown
        log.info("Supervisor shutting down — stopping all bots...")
        self.stop_all()

        send_telegram(
            f"🦊 <b>Supervisor stopped</b>\n"
            f"Uptime: {time.time() - self._start_time:.0f}s\n"
            f"Total restarts: {sum(b.restart_count for b in self.bots.values())}"
        )

        # Cleanup
        try:
            PID_FILE.unlink(missing_ok=True)
            HEARTBEAT_FILE.unlink(missing_ok=True)
        except Exception:
            pass

        log.info("Supervisor exited cleanly.")


# ── Entry Point ─────────────────────────────────────────────────

def setup_logging():
    fmt = "%(asctime)s [%(levelname)s] %(name)s: %(message)s"
    log_path = LOGS_DIR / "supervisor.log"
    LOGS_DIR.mkdir(exist_ok=True)

    handlers = [
        logging.handlers.RotatingFileHandler(
            log_path, maxBytes=5 * 1024 * 1024, backupCount=3, encoding="utf-8"
        ),
        logging.StreamHandler(sys.stdout),
    ]

    logging.basicConfig(
        level=logging.INFO,
        format=fmt,
        handlers=handlers,
    )


def main():
    if "--dry-run" in sys.argv:
        configs = Supervisor().discover_configs()
        print(f"Would start {len(configs)} bots:")
        for c in configs:
            print(f"  {c.stem}: {c}")
        return

    setup_logging()
    log.info("=" * 60)
    log.info("  HL Multi-Target Copy Trader — Supervisor")
    log.info(f"  PID: {os.getpid()}")
    log.info(f"  Python: {sys.version.split()[0]}")
    log.info(f"  Base: {BASE_DIR}")
    log.info("=" * 60)

    supervisor = Supervisor()
    supervisor.run()


if __name__ == "__main__":
    main()
