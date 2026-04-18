"""Loop de monitorización de Hyperliquid — estrategia basada en spikes de OI.

Flujo de cada ciclo:
  1. Obtener metaAndAssetCtxs → OI actual + markPx por asset.
  2. Detectar assets con variación de OI significativa respecto al ciclo anterior.
  3. Para cada asset con spike, buscar en el wallet pool las wallets activas
     que fueron vistas en trades recientes (acumuladas vía WebSocket).
  4. Consultar clearinghouseState de esas wallets para encontrar posiciones abiertas.
  5. Puntuar y alertar posiciones que superen el umbral de score.

Descubrimiento de wallets:
  El endpoint REST de Hyperliquid no ofrece listados de traders. El WS sí incluye
  el campo 'users: [buyer, seller]' en cada evento de trade. La función
  on_ws_trade() actúa como callback del HyperliquidWSClient y va llenando
  _wallet_pool, un dict por asset con las wallets vistas en las últimas N horas.

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
    HL_OI_SPIKE_PCT, HL_OI_SPIKE_MIN_USD,
)
from database import insert_alert, alert_exists
from hyperliquid_api import HyperliquidClient
from wallet_scorer import WalletContext, score_wallet

logger = logging.getLogger(__name__)

# ── Wallet pool: alimentado en tiempo real por el WebSocket ───────────────────
# asset → {wallet_addr → último timestamp en que fue vista en un trade}
_wallet_pool: dict[str, dict[str, datetime]] = defaultdict(dict)
_WALLET_POOL_TTL_HOURS = 4

_NULL_ADDR = "0x" + "0" * 40


def on_ws_trade(trade: dict) -> None:
    """Callback síncrono para HyperliquidWSClient.

    Extrae el campo 'users' de cada trade recibido por WebSocket y almacena
    las direcciones en _wallet_pool con TTL de 4 horas.
    """
    coin = trade.get("coin", "")
    users = trade.get("users") or []
    if not coin or not users:
        return
    now = datetime.now(tz=timezone.utc)
    for addr in users:
        if isinstance(addr, str) and addr and addr.lower() != _NULL_ADDR:
            _wallet_pool[coin][addr.lower()] = now


def _prune_wallet_pool(asset: str) -> None:
    cutoff = datetime.now(tz=timezone.utc) - timedelta(hours=_WALLET_POOL_TTL_HOURS)
    _wallet_pool[asset] = {
        w: ts for w, ts in _wallet_pool[asset].items() if ts > cutoff
    }


def wallet_pool_stats() -> dict[str, int]:
    """Devuelve el número de wallets activas por asset (para diagnóstico)."""
    return {asset: len(wallets) for asset, wallets in _wallet_pool.items() if wallets}


# ── Estado persistente entre ciclos ──────────────────────────────────────────
_prev_oi_usd: dict[str, float] = {}
_prev_mids: dict[str, float] = {}

# Caché de entradas recientes para detección de movimientos coordinados (2h)
_hl_recent_entries: dict[str, list[tuple[str, datetime, str]]] = defaultdict(list)


# ── Helpers de entradas coordinadas ──────────────────────────────────────────

def _purge_old_entries(asset: str, window_hours: int = 2) -> None:
    cutoff = datetime.now(tz=timezone.utc) - timedelta(hours=window_hours)
    _hl_recent_entries[asset] = [
        (w, ts, side)
        for w, ts, side in _hl_recent_entries[asset]
        if ts > cutoff
    ]


def _find_group_wallets(asset: str, wallet: str, side: str) -> list[str]:
    _purge_old_entries(asset)
    return [
        w for w, ts, s in _hl_recent_entries[asset]
        if w != wallet and s == side
    ]


def _register_entry(asset: str, wallet: str, side: str) -> None:
    _hl_recent_entries[asset].append(
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
    notifier,
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

    # ── Entradas coordinadas (entre wallets del pool) ─────────────────────────
    group_wallets = _find_group_wallets(coin, wallet, direction)
    _register_entry(coin, wallet, direction)

    ctx = WalletContext(
        wallet=wallet,
        age_days=age_days,
        poly_trade_count=fill_count,
        funding_source=None,
        has_defi=True,
        first_poly_date=first_trade_date,
        wallet_created=None,      # no disponible via API pública de HL
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
    notifier.buffer_hl_alert(
        coin=coin,
        alert_id=alert_id,
        wallet=wallet,
        amount_usd=position_value,
        direction=direction,
        score=result.total,
        level=result.level,
        age_days=age_days,
        category=category,
        oi_pct=oi_pct,
    )
    logger.info(
        "ALERTA HL %s [%s] [id=%d] wallet=%s score=%d asset=%s "
        "pos=$%,.0f oi_pct=%.3f%% oi_chg=%+.1f%%",
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

    # 2. Detectar spikes de OI
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
        mid = all_mids.get(coin, mark_px)
        current_mids[coin] = mid if mid else mark_px

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

    _prev_oi_usd.update(current_oi)
    _prev_mids.update(current_mids)

    # Log estado del wallet pool para diagnóstico
    pool_stats = wallet_pool_stats()
    logger.info(
        "HL: %d assets con OI, %d con spike (≥%.0f%%). "
        "Wallet pool: %d assets, %d wallets totales.",
        len(current_oi),
        len(spiked),
        HL_OI_SPIKE_PCT,
        len(pool_stats),
        sum(pool_stats.values()),
    )

    if not spiked:
        logger.info("-- HL: Ciclo completado --")
        return

    logger.info(
        "HL: Assets con spike: %s",
        [(s["coin"], f"{s['oi_change_pct']:+.1f}%") for s in spiked],
    )

    # 3. Para cada asset con spike, analizar wallets del pool
    for spike in spiked:
        coin = spike["coin"]
        category = spike["category"]

        _prune_wallet_pool(coin)
        wallets = list(_wallet_pool[coin].keys())

        logger.info(
            "HL SPIKE [%s/%s]: OI=$%,.0f (%+.1f%%) — %d wallets en pool.",
            coin, category, spike["oi_usd"], spike["oi_change_pct"], len(wallets),
        )

        if not wallets:
            logger.warning(
                "HL: Pool vacío para %s — el WebSocket aún está acumulando wallets. "
                "Las alertas empezarán tras unos minutos de actividad.",
                coin,
            )
            continue

        # Obtener estado de cada wallet y cruzar con posición en este asset
        for wallet in wallets:
            state = await hl_client.get_user_state(wallet)
            if not state:
                continue
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
                    notifier=notifier,
                )
            except Exception as exc:
                logger.error(
                    "Error analizando posición HL %s wallet %s: %s",
                    coin, wallet, exc, exc_info=True,
                )

    await notifier.flush_pending()
    logger.info("-- HL: Ciclo completado --")
