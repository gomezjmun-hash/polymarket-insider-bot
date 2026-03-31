"""Loop de monitorización de Hyperliquid — estrategia basada en spikes de OI.

Flujo de cada ciclo:
  1. Obtener metaAndAssetCtxs → OI actual + markPx por asset.
  2. Detectar assets con variación de OI significativa respecto al ciclo anterior.
  3. Obtener el leaderboard diario para conseguir direcciones de wallets activas.
  4. Obtener clearinghouseState de cada wallet del leaderboard una sola vez.
  5. Cruzar posiciones abiertas con los assets que tuvieron spike de OI.
  6. Puntuar y alertar posiciones que superen el umbral de score.

Categorías:
  CRYPTO      — BTC, ETH, SOL y altcoins con volumen > $1M/24h
  BOLSA       — Índices (SPX, NDX, DJI) y acciones individuales
  COMMODITIES — Petróleo (WTI, BRENT), oro (XAU), plata (XAG), gas (NG)
"""
import logging
from collections import defaultdict
from datetime import datetime, timezone, timedelta
from typing import Optional

from config import (
    SCORE_HIGH, SCORE_MEDIUM,
    HL_CRYPTO_ASSETS, HL_BOLSA_ASSETS, HL_COMMODITY_ASSETS,
    HL_MIN_USD_CRYPTO, HL_MIN_USD_BOLSA, HL_MIN_USD_COMMODITY,
    HL_OI_SPIKE_PCT, HL_OI_SPIKE_MIN_USD, HL_LEADERBOARD_SIZE,
)
from database import insert_alert, alert_exists
from hyperliquid_api import HyperliquidClient
from wallet_scorer import WalletContext, score_wallet

logger = logging.getLogger(__name__)

# ── Estado persistente entre ciclos ──────────────────────────────────────────
_prev_oi_usd: dict[str, float] = {}   # asset → OI en USD del ciclo anterior
_prev_mids: dict[str, float] = {}     # asset → mid price del ciclo anterior

# Caché de entradas recientes para detección de movimientos coordinados (2h)
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
    return "CRYPTO"


def _min_usd_for_category(category: str) -> float:
    if category == "COMMODITIES":
        return HL_MIN_USD_COMMODITY
    if category == "BOLSA":
        return HL_MIN_USD_BOLSA
    return HL_MIN_USD_CRYPTO


# ── Extracción de posición ────────────────────────────────────────────────────

def _extract_position(state: dict, coin: str) -> Optional[dict]:
    """Devuelve el dict de posición para coin o None si no hay posición abierta."""
    for p in state.get("assetPositions", []):
        pos = p.get("position", {})
        if pos.get("coin", "").upper() == coin.upper():
            try:
                if float(pos.get("szi", 0) or 0) != 0:
                    return pos
            except (TypeError, ValueError):
                pass
    return None


# ── Análisis de una posición concreta ────────────────────────────────────────

async def _analyze_position(
    wallet: str,
    coin: str,
    category: str,
    state: dict,
    pos: dict,
    oi_usd: float,
    oi_change_pct: float,
    mark_px: float,
    prev_mid: float,
    hl_client: HyperliquidClient,
) -> Optional[int]:
    """Puntúa una posición abierta en un asset con spike de OI.

    Devuelve el alert_id creado o None si no supera el umbral.
    """
    market_id = f"HL:{coin}"
    market_name = f"{coin} Perpetual ({category})"

    if await alert_exists(market_id, wallet):
        return None

    try:
        szi = float(pos.get("szi", 0) or 0)
        entry_px = float(pos.get("entryPx", 0) or 0)
    except (TypeError, ValueError):
        return None

    # Valor nocional: entrada × tamaño; si no hay entry_px, usamos markPx
    position_value = abs(szi * entry_px) if entry_px > 0 else abs(szi * mark_px)
    direction = "LONG" if szi > 0 else "SHORT"

    if position_value < _min_usd_for_category(category):
        return None

    # ── Contra-tendencia ──────────────────────────────────────────────────────
    counter_trend = False
    if prev_mid > 0 and mark_px > 0 and prev_mid != mark_px:
        price_went_up = mark_px > prev_mid
        counter_trend = (price_went_up and direction == "SHORT") or \
                        (not price_went_up and direction == "LONG")

    # ── Concentración vs OI total ─────────────────────────────────────────────
    oi_pct = (position_value / oi_usd * 100) if oi_usd > 0 else 0.0

    # ── Datos históricos de la cuenta ────────────────────────────────────────
    age_days = await hl_client.get_account_age_days(wallet)
    fills = await hl_client.get_user_fills(wallet)
    fill_count = len(fills)

    first_trade_date: Optional[datetime] = None
    if fills:
        times: list[datetime] = []
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

    try:
        account_equity = float(
            state.get("marginSummary", {}).get("accountValue", 0) or 0
        )
    except (TypeError, ValueError):
        account_equity = 0.0

    # ── Entradas coordinadas ──────────────────────────────────────────────────
    group_wallets = _find_hl_group_wallets(coin, wallet, direction)
    _register_hl_trade(coin, wallet, direction)

    ctx = WalletContext(
        wallet=wallet,
        age_days=age_days,
        poly_trade_count=fill_count,
        funding_source=None,
        has_defi=True,            # no penalizar por ausencia de DeFi en HL
        first_poly_date=first_trade_date,
        wallet_created=None,      # no tenemos fecha real de creación on-chain en HL
        amount_usd=position_value,
        direction=direction,
        has_hedge=False,
        group_wallets=group_wallets,
        shared_origin_wallets=[],
        total_portfolio_usd=max(account_equity, position_value),
        market_position_usd=position_value,
        oi_pct=oi_pct,
        counter_trend=counter_trend,
    )

    result = await score_wallet(ctx)

    if result.level not in ("HIGH", "MEDIUM"):
        logger.debug(
            "HL %s wallet=%s score=%d (%s) — descartado",
            coin, wallet, result.total, result.level,
        )
        return None

    alert_id = await insert_alert(
        market_id=market_id,
        market_name=market_name,
        wallet=wallet,
        score=result.total,
        breakdown=result.breakdown,
        amount_usd=position_value,
        direction=direction,
        level=result.level,
        source="hyperliquid",
        category=category,
    )
    logger.info(
        "ALERTA HL %s [%s] [id=%d] wallet=%s score=%d asset=%s "
        "pos=$%,.0f oi_pct=%.3f%% oi_chg=%.1f%%",
        result.level, category, alert_id, wallet, result.total,
        coin, position_value, oi_pct, oi_change_pct,
    )
    return alert_id


# ── Ciclo principal ───────────────────────────────────────────────────────────

async def run_hl_monitoring_cycle(
    hl_client: HyperliquidClient,
    notifier,
) -> None:
    """Un ciclo completo de monitorización de Hyperliquid."""
    logger.info("-- HL: Iniciando ciclo de monitorización --")

    # 1. Datos de mercado actuales
    asset_contexts = await hl_client.get_asset_contexts()
    all_mids = await hl_client.get_all_mids()

    if not asset_contexts:
        logger.warning("HL: No se pudieron obtener asset contexts.")
        logger.info("-- HL: Ciclo completado --")
        return

    # 2. Detectar spikes de OI comparando con ciclo anterior
    spiked: list[dict] = []
    current_oi: dict[str, float] = {}
    current_mids: dict[str, float] = {}

    for ctx_entry in asset_contexts:
        coin = ctx_entry.get("name", "")
        if not coin:
            continue
        try:
            mark_px = float(ctx_entry.get("markPx", 0) or 0)
            oi_contracts = float(ctx_entry.get("openInterest", 0) or 0)
            oi_usd = oi_contracts * mark_px
        except (TypeError, ValueError):
            continue

        current_oi[coin] = oi_usd

        # allMids tiene mayor precisión que markPx para el seguimiento de tendencia
        mid = all_mids.get(coin, mark_px)
        current_mids[coin] = mid if mid else mark_px

        # Comparar con el ciclo anterior para detectar spike
        prev_oi = _prev_oi_usd.get(coin, 0.0)
        if prev_oi > 0 and oi_usd >= HL_OI_SPIKE_MIN_USD:
            pct = (oi_usd - prev_oi) / prev_oi * 100
            if abs(pct) >= HL_OI_SPIKE_PCT:
                spiked.append({
                    "coin": coin,
                    "category": _category_for_asset(coin),
                    "oi_usd": oi_usd,
                    "oi_change_pct": pct,
                    "mark_px": mark_px,
                    "prev_mid": _prev_mids.get(coin, 0.0),
                })

    # Actualizar estado persistente para el próximo ciclo
    _prev_oi_usd.update(current_oi)
    _prev_mids.update(current_mids)

    logger.info(
        "HL: %d assets monitorizados, %d con spike de OI (≥%.0f%%).",
        len(current_oi), len(spiked), HL_OI_SPIKE_PCT,
    )

    if not spiked:
        logger.info("-- HL: Ciclo completado --")
        return

    logger.info(
        "HL: Assets con spike: %s",
        [(s["coin"], f"{s['oi_change_pct']:+.1f}%") for s in spiked],
    )

    # 3. Obtener wallets del leaderboard diario
    leaderboard = await hl_client.get_leaderboard("day")
    wallet_addresses: list[str] = []
    for row in leaderboard:
        addr = (row.get("ethAddress") or row.get("address") or "").lower()
        if addr and addr != "0x0000000000000000000000000000000000000000":
            wallet_addresses.append(addr)
        if len(wallet_addresses) >= HL_LEADERBOARD_SIZE:
            break

    if not wallet_addresses:
        logger.warning("HL: Leaderboard vacío — sin wallets que analizar.")
        logger.info("-- HL: Ciclo completado --")
        return

    logger.info("HL: %d wallets del leaderboard a revisar.", len(wallet_addresses))

    # 4. Obtener clearinghouseState de cada wallet una sola vez
    wallet_states: dict[str, dict] = {}
    for wallet in wallet_addresses:
        state = await hl_client.get_user_state(wallet)
        if state and state.get("assetPositions"):
            wallet_states[wallet] = state

    logger.info("HL: %d wallets con posiciones abiertas.", len(wallet_states))

    # 5. Cruzar posiciones con assets que tuvieron spike de OI
    for spike in spiked:
        coin = spike["coin"]
        category = spike["category"]
        logger.info(
            "HL SPIKE [%s/%s]: OI=$%,.0f (%+.1f%% vs ciclo anterior)",
            coin, category, spike["oi_usd"], spike["oi_change_pct"],
        )

        for wallet, state in wallet_states.items():
            pos = _extract_position(state, coin)
            if pos is None:
                continue
            try:
                await _analyze_position(
                    wallet=wallet,
                    coin=coin,
                    category=category,
                    state=state,
                    pos=pos,
                    oi_usd=spike["oi_usd"],
                    oi_change_pct=spike["oi_change_pct"],
                    mark_px=spike["mark_px"],
                    prev_mid=spike["prev_mid"],
                    hl_client=hl_client,
                )
            except Exception as exc:
                logger.error(
                    "Error analizando posición HL %s wallet %s: %s",
                    coin, wallet, exc, exc_info=True,
                )

    await notifier.flush_pending()
    logger.info("-- HL: Ciclo completado --")
