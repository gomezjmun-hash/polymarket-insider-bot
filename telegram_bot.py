"""Bot de Telegram: alertas y comandos /historial y /stats."""
import asyncio
import json
import logging
from dataclasses import dataclass, field as dc_field
from datetime import datetime, timezone
from typing import Optional

from telegram import Update
from telegram.ext import Application, CommandHandler, ContextTypes
from telegram.constants import ParseMode

from config import TELEGRAM_BOT_TOKEN, TELEGRAM_CHAT_ID, SCORE_HIGH, SCORE_MEDIUM
from database import (
    get_history, get_top_wallets, get_unnotified_alerts, mark_notified,
    update_event_state,
)
from wallet_scorer import breakdown_text

logger = logging.getLogger(__name__)

POLYGONSCAN_BASE = "https://polygonscan.com/address/"
HL_PORTFOLIO_BASE = "https://app.hyperliquid.xyz/portfolio/"

# ── Buffer agrupado de alertas Hyperliquid ────────────────────────────────────
_HL_FLUSH_MAX_SECONDS = 300   # 5 min desde la primera alerta del asset
_HL_FLUSH_IDLE_SECONDS = 120  # 2 min sin nueva alerta del asset
_HL_CHECK_INTERVAL = 30       # frecuencia del bucle de comprobación (seg)


@dataclass
class _HLEntry:
    alert_id: int
    wallet: str
    amount_usd: float
    direction: str   # LONG / SHORT
    score: int
    level: str       # HIGH / MEDIUM
    age_days: float
    category: str
    added_at: datetime = dc_field(
        default_factory=lambda: datetime.now(tz=timezone.utc)
    )


class _HLAlertBuffer:
    """Buffer en memoria que agrupa alertas de Hyperliquid por asset."""

    def __init__(self) -> None:
        self._entries: dict[str, list[_HLEntry]] = {}
        self._first_seen: dict[str, datetime] = {}
        self._last_seen: dict[str, datetime] = {}

    def add(self, coin: str, entry: _HLEntry) -> None:
        now = entry.added_at
        if coin not in self._entries:
            self._entries[coin] = []
            self._first_seen[coin] = now
        self._entries[coin].append(entry)
        self._last_seen[coin] = now

    def coins_ready_to_flush(self) -> list[str]:
        now = datetime.now(tz=timezone.utc)
        ready = []
        for coin in list(self._entries.keys()):
            age = (now - self._first_seen[coin]).total_seconds()
            idle = (now - self._last_seen[coin]).total_seconds()
            if age >= _HL_FLUSH_MAX_SECONDS or idle >= _HL_FLUSH_IDLE_SECONDS:
                ready.append(coin)
        return ready

    def pop(self, coin: str) -> list[_HLEntry]:
        entries = self._entries.pop(coin, [])
        self._first_seen.pop(coin, None)
        self._last_seen.pop(coin, None)
        return entries


def _format_hl_grouped(coin: str, entries: list[_HLEntry]) -> str:
    """Formatea el mensaje resumen agrupado para un asset de Hyperliquid."""
    now_str = datetime.now(tz=timezone.utc).strftime("%Y-%m-%d %H:%M UTC")

    longs = [e for e in entries if e.direction == "LONG"]
    shorts = [e for e in entries if e.direction == "SHORT"]
    long_total = sum(e.amount_usd for e in longs)
    short_total = sum(e.amount_usd for e in shorts)
    grand_total = long_total + short_total

    if grand_total > 0:
        long_pct = long_total / grand_total * 100
        short_pct = 100 - long_pct
        dominance = (
            f"LONG {long_pct:.1f}%" if long_pct >= short_pct
            else f"SHORT {short_pct:.1f}%"
        )
    else:
        dominance = "N/A"

    new_count = sum(1 for e in entries if e.age_days < 7)
    scores = [e.score for e in entries]
    avg_score = sum(scores) / len(scores) if scores else 0
    max_score = max(scores) if scores else 0
    biggest = max(entries, key=lambda e: e.amount_usd)
    top3 = sorted(entries, key=lambda e: e.amount_usd, reverse=True)[:3]

    hl_link = f"https://app.hyperliquid.xyz/trade/{coin}"
    lines = [
        f"🚨 *ALERTA AGRUPADA · {coin} · Hyperliquid*",
        f"🕐 {now_str}",
        "",
        f"📊 *{len(entries)}* wallets coordinadas en ventana de 2h",
        f"🟢 LONG: {len(longs)} wallets · ${long_total:,.0f} total",
        f"🔴 SHORT: {len(shorts)} wallets · ${short_total:,.0f} total",
        f"⚡ Dominancia: {dominance}",
        f"🆕 Wallets nuevas (<7 días): {new_count} de {len(entries)}",
        f"📈 Score medio: {avg_score:.0f} pts · Score máximo: {max_score} pts",
        f"💰 Posición mayor: ${biggest.amount_usd:,.0f} ({biggest.direction})",
        "",
        "*Top 3 wallets por nocional:*",
    ]
    for e in top3:
        short_w = e.wallet[:6] + "…" + e.wallet[-4:]
        lines.append(
            f"• [{short_w}]({HL_PORTFOLIO_BASE}{e.wallet}) · "
            f"${e.amount_usd:,.0f} · {e.direction} · Score {e.score}"
        )
    lines += ["", f"🔗 [Ver {coin} en Hyperliquid]({hl_link})"]
    return "\n".join(lines)


_CATEGORY_EMOJI = {
    "CRYPTO":      "🪙",
    "BOLSA":       "📈",
    "COMMODITIES": "🛢",
    "GEO":         "🌍",
}


def _level_emoji(level: str) -> str:
    return "🔴" if level == "HIGH" else "🟡"


def _format_alert(row: dict) -> str:
    breakdown = json.loads(row.get("breakdown") or "{}")
    bd_text = breakdown_text(breakdown)
    level = row.get("level", "MEDIUM")
    category = row.get("category") or "GEO"
    source = row.get("source") or "polymarket"
    level_emoji = _level_emoji(level)
    cat_emoji = _CATEGORY_EMOJI.get(category, "📊")
    created = row.get("created_at", "")[:19].replace("T", " ")

    if source == "hyperliquid":
        coin = row["market_id"].replace("HL:", "")
        market_link = f"https://app.hyperliquid.xyz/trade/{coin}"
        wallet_link = f"{HL_PORTFOLIO_BASE}{row['wallet']}"
        market_label = "Asset"
        platform = "Hyperliquid"
    else:
        market_link = f"https://polymarket.com/event/{row['market_id']}"
        wallet_link = f"{POLYGONSCAN_BASE}{row['wallet']}"
        market_label = "Mercado"
        platform = "Polymarket"

    return (
        f"{level_emoji} *ALERTA {level} · {cat_emoji} {category}*\n"
        f"🕐 {created} UTC · {platform}\n\n"
        f"📊 *{market_label}:* {row['market_name']}\n"
        f"👛 *Wallet:* `{row['wallet']}`\n"
        f"🎯 *Dirección:* {row['direction']}\n"
        f"💰 *Nocional:* ${row['amount_usd']:,.0f}\n"
        f"🔢 *Score:* {row['score']} pts\n\n"
        f"*Desglose de puntos:*\n{bd_text}\n\n"
        f"🔗 [{market_label}]({market_link}) | [Wallet]({wallet_link})"
    )


class TelegramNotifier:
    def __init__(self) -> None:
        if not TELEGRAM_BOT_TOKEN:
            raise ValueError("TELEGRAM_BOT_TOKEN no configurado")
        self._app = (
            Application.builder()
            .token(TELEGRAM_BOT_TOKEN)
            .build()
        )
        self._register_handlers()
        self._hl_buffer = _HLAlertBuffer()
        self._hl_flush_task: Optional[asyncio.Task] = None

    def _register_handlers(self) -> None:
        self._app.add_handler(CommandHandler("start", self._cmd_start))
        self._app.add_handler(CommandHandler("historial", self._cmd_historial))
        self._app.add_handler(CommandHandler("stats", self._cmd_stats))
        self._app.add_handler(CommandHandler("resuelto", self._cmd_resuelto))
        self._app.add_handler(CommandHandler("ayuda", self._cmd_ayuda))

    # ── Comandos ──────────────────────────────────────────────────────────────

    async def _cmd_start(self, update: Update, ctx: ContextTypes.DEFAULT_TYPE) -> None:
        await update.message.reply_text(
            "🤖 *Polymarket Insider Bot activo*\n\n"
            "Comandos disponibles:\n"
            "/historial — Alertas de los últimos 7 días\n"
            "/stats — Wallets más repetidas\n"
            "/resuelto <id> <si|no> — Marcar si el evento se resolvió como predijo la wallet\n"
            "/ayuda — Esta ayuda",
            parse_mode=ParseMode.MARKDOWN,
        )

    async def _cmd_historial(
        self, update: Update, ctx: ContextTypes.DEFAULT_TYPE
    ) -> None:
        rows = await get_history(days=7)
        if not rows:
            await update.message.reply_text("Sin alertas en los últimos 7 días.")
            return

        header = f"📋 *Historial últimos 7 días* ({len(rows)} alertas)\n\n"
        # Agrupa por nivel
        highs = [r for r in rows if r["level"] == "HIGH"]
        mediums = [r for r in rows if r["level"] == "MEDIUM"]

        lines = [header]
        for level_label, group in (("🔴 ALTA SOSPECHA", highs), ("🟡 MEDIA SOSPECHA", mediums)):
            if group:
                lines.append(f"*{level_label}* ({len(group)})\n")
                for r in group[:10]:
                    created = r["created_at"][:10]
                    lines.append(
                        f"• {created} | Score {r['score']} | "
                        f"${r['amount_usd']:,.0f} {r['direction']} | "
                        f"`{r['wallet'][:10]}…` | {r['market_name'][:40]}"
                    )
                lines.append("")

        msg = "\n".join(lines)
        # Telegram max 4096 chars por mensaje
        for chunk in _split_message(msg, 4000):
            await update.message.reply_text(chunk, parse_mode=ParseMode.MARKDOWN)

    async def _cmd_stats(
        self, update: Update, ctx: ContextTypes.DEFAULT_TYPE
    ) -> None:
        wallets = await get_top_wallets(days=7, limit=10)
        if not wallets:
            await update.message.reply_text("Sin datos de wallets en los últimos 7 días.")
            return

        lines = ["👾 *Wallets más sospechosas (7 días)*\n"]
        for i, w in enumerate(wallets, 1):
            lines.append(
                f"{i}. `{w['wallet'][:14]}…`\n"
                f"   Apariciones: {w['appearances']} | "
                f"Score máx: {w['max_score']} | "
                f"Total apostado: ${w['total_usd']:,.0f}\n"
                f"   [Polygonscan]({POLYGONSCAN_BASE}{w['wallet']})"
            )

        await update.message.reply_text(
            "\n".join(lines), parse_mode=ParseMode.MARKDOWN,
            disable_web_page_preview=True,
        )

    async def _cmd_resuelto(
        self, update: Update, ctx: ContextTypes.DEFAULT_TYPE
    ) -> None:
        args = ctx.args
        if not args or len(args) < 2:
            await update.message.reply_text(
                "Uso: /resuelto <alert_id> <si|no>\n"
                "Ejemplo: /resuelto 42 si"
            )
            return
        try:
            alert_id = int(args[0])
        except ValueError:
            await update.message.reply_text("ID de alerta inválido.")
            return

        state = "acertó" if args[1].lower() in ("si", "sí", "yes") else "falló"
        await update_event_state(alert_id, state)
        await update.message.reply_text(
            f"✅ Alerta #{alert_id} marcada como: *{state}*",
            parse_mode=ParseMode.MARKDOWN,
        )

    async def _cmd_ayuda(
        self, update: Update, ctx: ContextTypes.DEFAULT_TYPE
    ) -> None:
        await self._cmd_start(update, ctx)

    # ── Envío de alertas ──────────────────────────────────────────────────────

    async def send_alert(self, row: dict) -> None:
        if not TELEGRAM_CHAT_ID:
            logger.warning("TELEGRAM_CHAT_ID no configurado, no se puede enviar alerta.")
            return
        text = _format_alert(row)
        try:
            await self._app.bot.send_message(
                chat_id=TELEGRAM_CHAT_ID,
                text=text,
                parse_mode=ParseMode.MARKDOWN,
                disable_web_page_preview=False,
            )
            await mark_notified(row["id"])
            logger.info("Alerta enviada a Telegram: id=%s wallet=%s", row["id"], row["wallet"])
        except Exception as exc:
            logger.error("Error enviando alerta Telegram: %s", exc)

    async def flush_pending(self) -> None:
        """Envía alertas pendientes de notificar (excluye HL, gestionadas por buffer)."""
        pending = await get_unnotified_alerts()
        for row in pending:
            if row.get("source") == "hyperliquid":
                continue  # gestionadas por _hl_flush_loop
            await self.send_alert(row)

    # ── Buffer agrupado Hyperliquid ───────────────────────────────────────────

    def buffer_hl_alert(
        self,
        coin: str,
        alert_id: int,
        wallet: str,
        amount_usd: float,
        direction: str,
        score: int,
        level: str,
        age_days: float,
        category: str,
    ) -> None:
        """Añade una alerta HL al buffer agrupado por asset."""
        entry = _HLEntry(
            alert_id=alert_id,
            wallet=wallet,
            amount_usd=amount_usd,
            direction=direction,
            score=score,
            level=level,
            age_days=age_days,
            category=category,
        )
        self._hl_buffer.add(coin, entry)
        logger.debug(
            "HL buffer: alerta %d añadida para %s (%s $%.0f)",
            alert_id, coin, direction, amount_usd,
        )

    async def _send_hl_grouped(self, coin: str, entries: list[_HLEntry]) -> None:
        """Envía el mensaje agrupado para un asset y marca las alertas como notificadas."""
        if not entries or not TELEGRAM_CHAT_ID:
            return
        text = _format_hl_grouped(coin, entries)
        try:
            await self._app.bot.send_message(
                chat_id=TELEGRAM_CHAT_ID,
                text=text,
                parse_mode=ParseMode.MARKDOWN,
                disable_web_page_preview=False,
            )
            for entry in entries:
                await mark_notified(entry.alert_id)
            logger.info(
                "HL agrupado enviado: %s — %d wallets (ids: %s)",
                coin, len(entries), [e.alert_id for e in entries],
            )
        except Exception as exc:
            logger.error("Error enviando alerta agrupada HL %s: %s", coin, exc)

    async def _hl_flush_loop(self) -> None:
        """Bucle background: envía mensajes agrupados cuando se cumplen los timers."""
        while True:
            await asyncio.sleep(_HL_CHECK_INTERVAL)
            try:
                ready = self._hl_buffer.coins_ready_to_flush()
                for coin in ready:
                    entries = self._hl_buffer.pop(coin)
                    if entries:
                        await self._send_hl_grouped(coin, entries)
            except asyncio.CancelledError:
                raise
            except Exception as exc:
                logger.error("Error en _hl_flush_loop: %s", exc)

    # ── Ciclo de vida ─────────────────────────────────────────────────────────

    async def start_polling(self) -> None:
        """Inicia el polling de comandos en background.

        Llama a deleteWebhook antes de arrancar para que Telegram invalide
        cualquier sesión getUpdates activa de una instancia anterior, evitando
        el error Conflict cuando Railway reinicia el proceso.
        """
        await self._app.initialize()
        # Elimina webhook y sesión de polling previa antes de arrancar.
        await self._app.bot.delete_webhook(drop_pending_updates=True)
        await self._app.start()
        await self._app.updater.start_polling(drop_pending_updates=True)
        self._hl_flush_task = asyncio.create_task(
            self._hl_flush_loop(), name="hl-alert-flush"
        )
        logger.info("Bot de Telegram iniciado, escuchando comandos...")

    async def stop(self) -> None:
        if self._hl_flush_task:
            self._hl_flush_task.cancel()
            try:
                await self._hl_flush_task
            except asyncio.CancelledError:
                pass
        await self._app.updater.stop()
        await self._app.stop()
        await self._app.shutdown()


def _split_message(text: str, limit: int = 4000) -> list[str]:
    chunks = []
    while len(text) > limit:
        split_at = text.rfind("\n", 0, limit)
        if split_at == -1:
            split_at = limit
        chunks.append(text[:split_at])
        text = text[split_at:].lstrip("\n")
    if text:
        chunks.append(text)
    return chunks
