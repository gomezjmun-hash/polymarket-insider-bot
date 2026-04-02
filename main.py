"""
Polymarket Insider Bot
======================
Monitoriza mercados geopolíticos en Polymarket buscando patrones de
insider trading usando datos on-chain de Polygon.

Arranque:
    python main.py

Primeras veces te pedirá el token del bot de Telegram y el chat ID.
"""
import asyncio
import os
import sys
import logging

from logger_setup import setup_logging
from config import (
    LOG_LEVEL, POLL_INTERVAL_SECONDS,
    TELEGRAM_BOT_TOKEN, TELEGRAM_CHAT_ID,
)

logger = logging.getLogger(__name__)


def _ask_telegram_config() -> None:
    """Pide interactivamente token y chat_id si no están en .env."""
    env_path = ".env"
    lines: list[str] = []
    if os.path.exists(env_path):
        with open(env_path) as f:
            lines = f.readlines()

    changed = False

    if not TELEGRAM_BOT_TOKEN:
        token = input(
            "\n🤖 Ingresa el TOKEN de tu bot de Telegram "
            "(obtenlo con @BotFather): "
        ).strip()
        if token:
            lines.append(f"TELEGRAM_BOT_TOKEN={token}\n")
            os.environ["TELEGRAM_BOT_TOKEN"] = token
            changed = True

    if not TELEGRAM_CHAT_ID:
        chat_id = input(
            "💬 Ingresa tu CHAT ID de Telegram "
            "(usa @userinfobot para obtenerlo): "
        ).strip()
        if chat_id:
            lines.append(f"TELEGRAM_CHAT_ID={chat_id}\n")
            os.environ["TELEGRAM_CHAT_ID"] = chat_id
            changed = True

    if changed:
        with open(env_path, "w") as f:
            f.writelines(lines)
        print("✅ Configuración guardada en .env\n")
        # Recarga config
        import importlib
        import config
        importlib.reload(config)


async def _main_loop() -> None:
    from database import init_db
    from polymarket_api import PolymarketClient
    from polygon_rpc import PolygonClient
    from monitor import run_monitoring_cycle
    from hyperliquid_api import HyperliquidClient, HyperliquidWSClient
    from hl_monitor import run_hl_monitoring_cycle, on_ws_trade
    from telegram_bot import TelegramNotifier
    from config import HL_CRYPTO_ASSETS, HL_BOLSA_ASSETS, HL_COMMODITY_ASSETS

    await init_db()

    notifier = TelegramNotifier()
    await notifier.start_polling()

    poly_client = PolymarketClient()
    polygon_client = PolygonClient()
    hl_client = HyperliquidClient()

    # WebSocket de Hyperliquid: acumula wallets activas en tiempo real.
    # Se suscribe a todos los assets fijos monitorizados.
    ws_client = HyperliquidWSClient()
    ws_client.add_trade_callback(on_ws_trade)
    ws_coins = list(
        set(HL_CRYPTO_ASSETS) | set(HL_BOLSA_ASSETS) | set(HL_COMMODITY_ASSETS)
    )
    await ws_client.start(ws_coins)

    logger.info(
        "Bot iniciado (Polymarket + Hyperliquid). Intervalo: %ds. "
        "Presiona Ctrl+C para detener.",
        POLL_INTERVAL_SECONDS,
    )

    async def poly_loop() -> None:
        while True:
            try:
                await run_monitoring_cycle(poly_client, polygon_client, notifier)
            except Exception as exc:
                logger.error("Error en ciclo Polymarket: %s", exc, exc_info=True)
            logger.info("Polymarket — próximo ciclo en %ds...", POLL_INTERVAL_SECONDS)
            await asyncio.sleep(POLL_INTERVAL_SECONDS)

    async def hl_loop() -> None:
        while True:
            try:
                await run_hl_monitoring_cycle(hl_client, notifier)
            except Exception as exc:
                logger.error("Error en ciclo Hyperliquid: %s", exc, exc_info=True)
            logger.info("Hyperliquid — próximo ciclo en %ds...", POLL_INTERVAL_SECONDS)
            await asyncio.sleep(POLL_INTERVAL_SECONDS)

    try:
        await asyncio.gather(poly_loop(), hl_loop())
    except (KeyboardInterrupt, asyncio.CancelledError):
        logger.info("Deteniendo bot...")
    finally:
        await ws_client.stop()
        await poly_client.close()
        await polygon_client.close()
        await hl_client.close()
        await notifier.stop()
        logger.info("Bot detenido.")


def main() -> None:
    setup_logging(LOG_LEVEL)

    # Banner
    print("\n" + "=" * 60)
    print("  POLYMARKET INSIDER BOT - Monitor de Geopolitica")
    print("=" * 60 + "\n")

    # Solo pedir config interactiva si hay terminal (no en Railway/Docker/CI)
    if sys.stdin.isatty():
        _ask_telegram_config()

    # Valida que tengamos token
    from config import TELEGRAM_BOT_TOKEN as TOKEN
    if not TOKEN:
        print("❌ TELEGRAM_BOT_TOKEN no configurado. Defínelo como variable de entorno.")
        sys.exit(1)

    try:
        asyncio.run(_main_loop())
    except KeyboardInterrupt:
        print("\nBot detenido por el usuario.")


if __name__ == "__main__":
    main()
