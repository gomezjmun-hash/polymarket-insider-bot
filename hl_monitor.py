"""Loop de monitorización de Hyperliquid para insider trading.

Categorías:
  CRYPTO      — BTC, ETH, SOL y cualquier altcoin con volumen > $1M/24h
  BOLSA       — Índices (SPX, NDX, DJI) y acciones individuales
  COMMODITIES — Petróleo (WTI, BRENT), oro (XAU), plata (XAG), gas (NG)

El sistema de scoring reutiliza WalletContext + score_wallet del módulo
principal, adaptando los campos a la semántica de Hyperliquid:
  - age_days          → días desde el primer fill en HL
  - poly_trade_count  → total de fills en HL
  - funding_source    → None (no aplica en HL)
  - has_defi          → True (no penalizar por ausencia de DeFi)
  - direction         → LONG / SHORT
"""
import logging
from collections import defaultdict
from datetime import datetime, timezone, timedelta
from typing import Optional

from config import (
    SCORE_HIGH, SCORE_MEDIUM,
    HL_CRYPTO_ASSETS, HL_BOLSA_ASSETS, HL_COMMODITY_ASSETS,
    HL_MIN_USD_CRYPTO, HL_MIN_USD_BOLSA, HL_MIN_USD_COMMODITY,
)
from database import insert_alert, alert_exists
from hyperliquid_api import HyperliquidClient
from wallet_scorer import WalletContext, score_wallet

logger = logging.getLogger(__name__)

# Caché de trades recientes por asset → detectar entradas coordinadas en 2h
# asset → list[(wallet, timestamp, side)]
_hl_recent_trades: dict[str, list[tuple[str, datetime, str]]] = defaultdict(list)


# ── Helpers de caché ──────────────────────────────────────────────────────────

def _purge_old_hl_trades(asset: str, window_hours: int = 2) -> None:
    cutoff = datetime.now(tz=timezone.utc) - timedelta(hours=window_hours)
    _hl_recent_trades[asset] = [
        (w, ts, side)
        for w, ts, side in _hl_recent_trades[asset]
        if ts > cutoff
    ]


def _find_hl_group_wallets(asset: str, wallet: str, side: str) -> list[str]:
    """Wallets que entraron en el mismo asset/lado en la ventana de 2h."""
    _purge_old_hl_trades(asset)
    return [
        w for w, ts, s in _hl_recent_trades[asset]
        if w != wallet and s == side
    ]


def _register_hl_trade(asset: str, wallet: str, side: str) -> None:
    _hl_recent_trades[asset].append(
        (wallet, datetime.now(tz=timezone.utc), side)
    )


# ── Clasificación de assets ───────────────────────────────────────────────────

_CRYPTO_SET = {c.upper() for c in HL_CRYPTO_ASSETS}
_BOLSA_SET = {c.upper() for c in HL_BOLSA_ASSETS}
_COMMODITY_SET = {c.upper() for c in HL_COMMODITY_ASSETS}


def _category_for_asset(asset: str) -> str:
    a = asset.upper()
    if a in _BOLSA_SET:
        return "BOLSA"
    if a in _COMMODITY_SET:
        return "COMMODITIES"
    return "CRYPTO"  # default para assets de crypto y altcoins


def _min_usd_for_category(category: str) -> float:
    if category == "COMMODITIES":
        return HL_MIN_USD_COMMODITY
    if category == "BOLSA":
        return HL_MIN_USD_BOLSA
    return HL_MIN_USD_CRYPTO


# ── Análisis de trade individual ──────────────────────────────────────────────

async def analyze_hl_trade(
    trade: dict,
    asset: str,
    category: str,
    wallet: str,
    hl_client: HyperliquidClient,
) -> Optional[int]:
    """Analiza un trade de Hyperliquid y genera alerta si score >= umbral.

    Devuelve el alert_id creado o None si no supera el umbral.
    """
    market_id = f"HL:{asset}"
    market_name = f"{asset} Perpetual ({category})"

    if await alert_exists(market_id, wallet):
        return None

    notional = float(trade.get("_notional_usd", 0))
    # "B" = buy (taker compra = LONG agresivo), "A" = sell (taker vende = SHORT)
    side = trade.get("side", "B")
    direction = "LONG" if side == "B" else "SHORT"

    # ── Datos on-chain de la cuenta en HL ────────────────────────────────────
    age_days = await hl_client.get_account_age_days(wallet)
    fills = await hl_client.get_user_fills(wallet)
    fill_count = len(fills)

    first_trade_date: Optional[datetime] = None
    if fills:
        times = []
        for f in fills:
            t = f.get("time")
            if t:
                try:
                    times.append(
                        datetime.fromtimestamp(int(t) / 1000, tz=timezone.utc)
                    )
                except Exception:
                    pass
        first_trade_date = min(times) if times else None

    account_equity = await hl_client.get_account_equity(wallet)
    position_value = await hl_client.get_position_value(wallet, asset)

    # ── Detección de entradas coordinadas ────────────────────────────────────
    group_wallets = _find_hl_group_wallets(asset, wallet, direction)
    _register_hl_trade(asset, wallet, direction)

    # Valor total de la posición en este mercado (histórico + este trade)
    market_position_usd = max(position_value, notional)

    ctx = WalletContext(
        wallet=wallet,
        age_days=age_days,
        poly_trade_count=fill_count,      # fills históricos en HL
        funding_source=None,               # no aplica en HL
        has_defi=True,                     # no penalizar por ausencia de DeFi
        first_poly_date=first_trade_date,  # primer fill en HL
        wallet_created=first_trade_date,   # proxy de creación = primer fill
        amount_usd=notional,
        direction=direction,
        has_hedge=False,                   # simplificado
        group_wallets=group_wallets,
        shared_origin_wallets=[],          # no se comprueba origen on-chain en HL
        total_portfolio_usd=max(account_equity, notional),
        market_position_usd=market_position_usd,
    )

    result = await score_wallet(ctx)

    if result.level not in ("HIGH", "MEDIUM"):
        return None

    alert_id = await insert_alert(
        market_id=market_id,
        market_name=market_name,
        wallet=wallet,
        score=result.total,
        breakdown=result.breakdown,
        amount_usd=notional,
        direction=direction,
        level=result.level,
        source="hyperliquid",
        category=category,
    )
    logger.info(
        "ALERTA HL %s [%s] [id=%d] wallet=%s score=%d asset=%s nocional=$%,.0f",
        result.level, category, alert_id, wallet, result.total, asset, notional,
    )
    return alert_id


# ── Ciclo principal ───────────────────────────────────────────────────────────

async def run_hl_monitoring_cycle(
    hl_client: HyperliquidClient,
    notifier,
) -> None:
    """Un ciclo completo de monitorización de Hyperliquid."""
    logger.info("-- HL: Iniciando ciclo de monitorización --")

    # Obtener todos los assets disponibles para descubrir altcoins activos
    asset_contexts = await hl_client.get_asset_contexts()
    if not asset_contexts:
        logger.warning("HL: No se pudieron obtener asset contexts.")
        return

    available_coins_upper = {a.get("name", "").upper() for a in asset_contexts}

    # ── Construir lista de assets a monitorizar ───────────────────────────────
    # asset → category
    monitored: dict[str, str] = {}

    for coin in HL_CRYPTO_ASSETS:
        if coin.upper() in available_coins_upper:
            monitored[coin] = "CRYPTO"

    for coin in HL_BOLSA_ASSETS:
        if coin.upper() in available_coins_upper:
            monitored[coin] = "BOLSA"

    for coin in HL_COMMODITY_ASSETS:
        if coin.upper() in available_coins_upper:
            monitored[coin] = "COMMODITIES"

    # Altcoins dinámicos: cualquier asset de HL con > $1M de volumen 24h
    # que no esté ya clasificado explícitamente
    known_upper = _CRYPTO_SET | _BOLSA_SET | _COMMODITY_SET
    for ctx_entry in asset_contexts:
        coin = ctx_entry.get("name", "")
        if not coin or coin.upper() in known_upper or coin in monitored:
            continue
        try:
            volume_24h = float(ctx_entry.get("dayNtlVlm", 0) or 0)
            if volume_24h > 1_000_000:
                monitored[coin] = "CRYPTO"
        except (TypeError, ValueError):
            pass

    logger.info("HL: %d assets monitorizados", len(monitored))

    # ── Escanear trades grandes por asset ─────────────────────────────────────
    for asset, category in monitored.items():
        min_usd = _min_usd_for_category(category)
        try:
            large_trades = await hl_client.get_large_trades(asset, min_usd)
            if large_trades:
                logger.debug(
                    "HL %s [%s]: %d trades >= $%,.0f",
                    asset, category, len(large_trades), min_usd,
                )

            for trade in large_trades:
                # 'users' es un array [buyer_addr, seller_addr] cuando está disponible.
                # Analizamos solo el aggresor (taker = users[0]).
                users = trade.get("users") or []
                if not users:
                    continue
                wallet = users[0].lower() if isinstance(users[0], str) else ""
                if not wallet or wallet == "0x0000000000000000000000000000000000000000":
                    continue
                try:
                    await analyze_hl_trade(
                        trade, asset, category, wallet, hl_client,
                    )
                except Exception as exc:
                    logger.error(
                        "Error analizando trade HL %s wallet %s: %s",
                        asset, wallet, exc, exc_info=True,
                    )

        except Exception as exc:
            logger.error("Error procesando asset HL %s: %s", asset, exc)

    await notifier.flush_pending()
    logger.info("-- HL: Ciclo completado --")
