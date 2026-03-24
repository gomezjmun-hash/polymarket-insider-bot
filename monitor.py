"""Loop principal de monitorización."""
import asyncio
import logging
from collections import defaultdict
from datetime import datetime, timezone, timedelta
from typing import Optional

from config import SCORE_HIGH, SCORE_MEDIUM, MIN_POSITION_USD
from database import (
    init_db, insert_alert, alert_exists, save_wallet_cache, get_wallet_cache
)
from polymarket_api import PolymarketClient
from polygon_rpc import PolygonClient
from wallet_scorer import WalletContext, score_wallet

logger = logging.getLogger(__name__)

# Caché en memoria de trades recientes por mercado → detectar entrada grupal 2h
# market_id → list of (wallet, timestamp, direction)
_recent_trades_cache: dict[str, list[tuple[str, datetime, str]]] = defaultdict(list)

# Caché de origin por wallet → detectar shared_origin
_wallet_origin_cache: dict[str, Optional[str]] = {}


def _purge_old_trades(market_id: str, window_hours: int = 2) -> None:
    cutoff = datetime.now(tz=timezone.utc) - timedelta(hours=window_hours)
    _recent_trades_cache[market_id] = [
        (w, ts, d)
        for w, ts, d in _recent_trades_cache[market_id]
        if ts > cutoff
    ]


def _find_group_wallets(
    market_id: str,
    wallet: str,
    direction: str,
    window_hours: int = 2,
) -> list[str]:
    """Wallets que entraron en el mismo mercado/dirección en la ventana de 2h."""
    _purge_old_trades(market_id, window_hours)
    return [
        w for w, ts, d in _recent_trades_cache[market_id]
        if w != wallet and d == direction
    ]


def _register_trade(market_id: str, wallet: str, direction: str) -> None:
    _recent_trades_cache[market_id].append(
        (wallet, datetime.now(tz=timezone.utc), direction)
    )


def _find_shared_origin_wallets(wallet: str, origin: Optional[str]) -> list[str]:
    if not origin:
        return []
    return [
        w for w, o in _wallet_origin_cache.items()
        if o and o == origin and w != wallet
    ]


async def analyze_trade(
    trade: dict,
    market: dict,
    poly_client: PolymarketClient,
    poly_client_rpc: PolygonClient,
) -> None:
    """Analiza un trade individual y genera alerta si score >= umbral."""
    wallet = (
        trade.get("proxyWallet") or
        trade.get("maker") or trade.get("makerAddress") or
        trade.get("taker") or trade.get("takerAddress") or ""
    ).lower()
    if not wallet or wallet == "0x0000000000000000000000000000000000000000":
        return

    market_id = market.get("id") or market.get("conditionId", "")
    market_name = market.get("question") or market.get("title", "Unknown")

    # Evitar duplicar alertas recientes
    if await alert_exists(market_id, wallet):
        return

    # ── Importe ───────────────────────────────────────────────────────────────
    try:
        size = float(trade.get("size", 0) or 0)
        price = float(trade.get("price", 1) or 1)
        usdc = float(trade.get("usdcSize", 0) or 0)
        amount_usd = max(size * price, usdc)
    except (TypeError, ValueError):
        amount_usd = 0.0

    if amount_usd < MIN_POSITION_USD:
        return

    direction = "YES" if str(trade.get("outcome", "")).upper() == "YES" else "NO"

    # ── Caché de wallet ───────────────────────────────────────────────────────
    cached = await get_wallet_cache(wallet)
    now = datetime.now(tz=timezone.utc)

    if cached and (now - datetime.fromisoformat(cached["first_seen"])).seconds < 3600:
        # Usar cache si es reciente (< 1h)
        age_days = cached["age_days"]
        has_defi = bool(cached.get("has_defi"))
        trade_count = cached.get("trade_count", 0)
        funding_source = cached.get("funding_source")
        wallet_created_str = cached.get("wallet_created")
        wallet_created = (
            datetime.fromisoformat(wallet_created_str)
            if wallet_created_str else None
        )
    else:
        # Consultar datos on-chain y Polymarket
        age_days = await poly_client_rpc.get_wallet_age_days(wallet)
        has_defi = await poly_client_rpc.has_defi_activity(wallet)
        funding_source = await poly_client_rpc.get_funding_source(wallet)
        wallet_first_ts = await poly_client_rpc.get_wallet_first_tx_timestamp(wallet)
        wallet_created = wallet_first_ts

        poly_trades = await poly_client.get_trades_for_wallet(wallet, limit=50)
        trade_count = len(poly_trades)

        origin = await poly_client_rpc.get_funding_address(wallet)
        _wallet_origin_cache[wallet] = origin

        cache_data = {
            "first_seen": now.isoformat(),
            "age_days": age_days,
            "has_defi": has_defi,
            "trade_count": trade_count,
            "funding_source": funding_source,
            "wallet_created": wallet_created.isoformat() if wallet_created else None,
            "origin": origin,
        }
        await save_wallet_cache(wallet, cache_data)

    origin = _wallet_origin_cache.get(wallet)

    # ── Detección de entradas coordinadas ─────────────────────────────────────
    group_wallets = _find_group_wallets(market_id, wallet, direction)
    shared_origin_wallets = _find_shared_origin_wallets(wallet, origin)
    _register_trade(market_id, wallet, direction)

    # ── Hedge check (simplificado) ────────────────────────────────────────────
    # Busca si la misma wallet tiene posición en dirección contraria en este mercado
    condition_id = market.get("conditionId", market_id)
    all_positions = await poly_client.get_positions_for_wallet(wallet)
    wallet_positions = [
        p for p in all_positions
        if p.get("conditionId", "").lower() == condition_id.lower()
    ]
    opposite = "NO" if direction == "YES" else "YES"
    has_hedge = any(
        str(p.get("outcome", "")).upper() == opposite
        for p in wallet_positions
    )

    # ── Capital total en Polymarket (aproximado) ──────────────────────────────
    total_portfolio_usd = sum(
        float(p.get("currentValue", 0) or 0)
        for p in all_positions
    )
    market_position_usd = sum(
        float(p.get("currentValue", 0) or 0)
        for p in wallet_positions
        if str(p.get("outcome", "")).upper() == direction
    ) + amount_usd

    # Primera apuesta en Polymarket
    poly_age = await poly_client.get_wallet_polymarket_age(wallet)

    ctx = WalletContext(
        wallet=wallet,
        age_days=age_days,
        poly_trade_count=trade_count,
        funding_source=funding_source,
        has_defi=has_defi,
        first_poly_date=poly_age,
        wallet_created=wallet_created,
        amount_usd=amount_usd,
        direction=direction,
        has_hedge=has_hedge,
        group_wallets=group_wallets,
        shared_origin_wallets=shared_origin_wallets,
        total_portfolio_usd=max(total_portfolio_usd, amount_usd),
        market_position_usd=market_position_usd,
    )

    result = await score_wallet(ctx)

    if result.level in ("HIGH", "MEDIUM"):
        alert_id = await insert_alert(
            market_id=market_id,
            market_name=market_name,
            wallet=wallet,
            score=result.total,
            breakdown=result.breakdown,
            amount_usd=amount_usd,
            direction=direction,
            level=result.level,
        )
        logger.info(
            "ALERTA %s [id=%d] wallet=%s score=%d mercado='%s'",
            result.level, alert_id, wallet, result.total, market_name,
        )


async def run_monitoring_cycle(
    poly_client: PolymarketClient,
    polygon_client: PolygonClient,
    notifier,
) -> None:
    """Un ciclo completo de monitorización."""
    logger.info("-- Iniciando ciclo de monitorizacion --")

    markets = await poly_client.get_geo_markets()
    if not markets:
        logger.warning("No se encontraron mercados geopolíticos activos.")
        return

    for market in markets:
        market_id = market.get("conditionId") or market.get("id", "")
        if not market_id:
            continue

        try:
            trades = await poly_client.get_large_trades(market_id, min_usd=MIN_POSITION_USD)
            logger.debug(
                "Mercado '%s': %d trades grandes",
                market.get("question", market_id), len(trades),
            )

            for trade in trades:
                try:
                    await analyze_trade(trade, market, poly_client, polygon_client)
                except Exception as exc:
                    logger.error("Error analizando trade: %s", exc, exc_info=True)

        except Exception as exc:
            logger.error("Error procesando mercado %s: %s", market_id, exc)

    # Enviar alertas no notificadas
    await notifier.flush_pending()
    logger.info("-- Ciclo completado --")
