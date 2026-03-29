"""Bot de Telegram: alertas y comandos /historial y /stats."""
import json
import logging
from datetime import datetime

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


def _level_emoji(level: str) -> str:
    return "🔴" if level == "HIGH" else "🟡"


def _format_alert(row: dict) -> str:
    breakdown = json.loads(row.get("breakdown") or "{}")
    bd_text = breakdown_text(breakdown)
    level = row.get("level", "MEDIUM")
    emoji = _level_emoji(level)
    poly_link = f"https://polymarket.com/event/{row['market_id']}"
    scan_link = f"{POLYGONSCAN_BASE}{row['wallet']}"
    created = row.get("created_at", "")[:19].replace("T", " ")

    return (
        f"{emoji} *ALERTA {level} SOSPECHA*\n"
        f"🕐 {created} UTC\n\n"
        f"📊 *Mercado:* {row['market_name']}\n"
        f"👛 *Wallet:* `{row['wallet']}`\n"
        f"🎯 *Dirección:* {row['direction']}\n"
        f"💰 *Importe:* ${row['amount_usd']:,.0f}\n"
        f"🔢 *Score:* {row['score']} pts\n\n"
        f"*Desglose de puntos:*\n{bd_text}\n\n"
        f"🔗 [Mercado]({poly_link}) | [Wallet en Polygonscan]({scan_link})"
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
        """Envía todas las alertas pendientes de notificar."""
        pending = await get_unnotified_alerts()
        for row in pending:
            await self.send_alert(row)

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
        logger.info("Bot de Telegram iniciado, escuchando comandos...")

    async def stop(self) -> None:
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
