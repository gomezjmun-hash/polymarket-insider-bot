"""Cliente para la API pública de Hyperliquid Perps.

Hyperliquid expone un único endpoint REST (POST /info) con distintos tipos
de request. No requiere autenticación para datos de mercado y trades.

Referencia: https://hyperliquid.gitbook.io/hyperliquid-docs/for-developers/api
"""
import asyncio
import json
import logging
import time
from datetime import datetime, timezone
from typing import Any, Callable, Optional

import aiohttp

logger = logging.getLogger(__name__)

HL_API_BASE = "https://api.hyperliquid.xyz"
HL_WS_URL = "wss://api.hyperliquid.xyz/ws"
REQUEST_TIMEOUT = aiohttp.ClientTimeout(total=20)

# Rate limiting: mínimo de segundos entre peticiones
_MIN_REQUEST_INTERVAL = 0.5  # 2 req/s como margen seguro

# Backoff exponencial: base, máximo de reintentos y cap de espera
_BACKOFF_BASE = 2.0
_BACKOFF_MAX_RETRIES = 5
_BACKOFF_CAP = 60.0


class HyperliquidClient:
    def __init__(self) -> None:
        self._session: Optional[aiohttp.ClientSession] = None
        self._last_request_time: float = 0.0
        self._lock = asyncio.Lock()

    async def _get_session(self) -> aiohttp.ClientSession:
        if self._session is None or self._session.closed:
            self._session = aiohttp.ClientSession(timeout=REQUEST_TIMEOUT)
        return self._session

    async def close(self) -> None:
        if self._session and not self._session.closed:
            await self._session.close()

    async def _throttle(self) -> None:
        """Espera lo necesario para respetar el intervalo mínimo entre peticiones."""
        async with self._lock:
            now = time.monotonic()
            elapsed = now - self._last_request_time
            wait = _MIN_REQUEST_INTERVAL - elapsed
            if wait > 0:
                await asyncio.sleep(wait)
            self._last_request_time = time.monotonic()

    async def _post(self, payload: dict) -> Any:
        session = await self._get_session()
        url = f"{HL_API_BASE}/info"
        req_type = payload.get("type")

        for attempt in range(_BACKOFF_MAX_RETRIES + 1):
            await self._throttle()
            try:
                async with session.post(url, json=payload) as resp:
                    if resp.status == 429:
                        wait = min(_BACKOFF_BASE ** attempt, _BACKOFF_CAP)
                        logger.warning(
                            "HL rate limit (429) en %s, reintento %d/%d en %.1fs",
                            req_type, attempt + 1, _BACKOFF_MAX_RETRIES, wait,
                        )
                        if attempt < _BACKOFF_MAX_RETRIES:
                            await asyncio.sleep(wait)
                            continue
                        logger.error("HL max reintentos alcanzado para %s", req_type)
                        return None
                    if resp.status != 200:
                        logger.warning(
                            "HL POST %s -> HTTP %s", req_type, resp.status
                        )
                        return None
                    return await resp.json(content_type=None)
            except Exception as exc:
                logger.error("Error HL POST %s: %s", req_type, exc)
                if attempt < _BACKOFF_MAX_RETRIES:
                    wait = min(_BACKOFF_BASE ** attempt, _BACKOFF_CAP)
                    await asyncio.sleep(wait)
                    continue
                return None

        return None

    # ── Metadatos de mercado ───────────────────────────────────────────────────

    async def get_asset_contexts(self) -> list[dict]:
        """Lista de assets con metadatos + contexto de mercado (OI, volumen, precio).

        Devuelve lista de dicts con campos del asset (name, szDecimals, …)
        fusionados con su contexto (dayNtlVlm, openInterest, markPx, …).
        """
        data = await self._post({"type": "metaAndAssetCtxs"})
        if not data or not isinstance(data, list) or len(data) < 2:
            return []
        meta_list = data[0].get("universe", [])
        ctx_list = data[1]
        result = []
        for meta, ctx in zip(meta_list, ctx_list):
            entry = {**meta, **ctx}
            result.append(entry)
        return result

    # ── Trades recientes ───────────────────────────────────────────────────────

    async def get_recent_trades(self, coin: str) -> list[dict]:
        """Trades recientes de un asset.

        Cada trade puede incluir el campo 'users' con [buyer_addr, seller_addr].
        Si la API no lo incluye el campo estará ausente; el caller lo maneja.
        """
        data = await self._post({"type": "recentTrades", "coin": coin})
        if not data:
            return []
        return data if isinstance(data, list) else []

    async def get_large_trades(self, coin: str, min_usd: float) -> list[dict]:
        """Trades recientes con nocional >= min_usd.

        Añade el campo '_notional_usd' a cada trade devuelto.
        """
        trades = await self.get_recent_trades(coin)
        result = []
        for t in trades:
            try:
                px = float(t.get("px", 0) or 0)
                sz = float(t.get("sz", 0) or 0)
                notional = px * sz
                if notional >= min_usd:
                    t["_notional_usd"] = notional
                    result.append(t)
            except (TypeError, ValueError):
                continue
        return result

    # ── Datos de cuenta ────────────────────────────────────────────────────────

    async def get_user_fills(self, address: str) -> list[dict]:
        """Historial completo de fills de una dirección en Hyperliquid."""
        data = await self._post({
            "type": "userFills",
            "user": address.lower(),
        })
        if not data:
            return []
        return data if isinstance(data, list) else []

    async def get_user_state(self, address: str) -> Optional[dict]:
        """Estado de cuenta: posiciones abiertas, equity, leverage."""
        return await self._post({
            "type": "clearinghouseState",
            "user": address.lower(),
        })

    async def get_account_age_days(self, address: str) -> float:
        """Días desde el primer fill de la cuenta (0.0 si sin historial)."""
        fills = await self.get_user_fills(address)
        if not fills:
            return 0.0
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
        if not times:
            return 0.0
        first = min(times)
        return (datetime.now(tz=timezone.utc) - first).total_seconds() / 86400

    async def get_first_trade_date(self, address: str) -> Optional[datetime]:
        """Datetime del primer fill de la cuenta."""
        fills = await self.get_user_fills(address)
        if not fills:
            return None
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
        return min(times) if times else None

    async def get_account_equity(self, address: str) -> float:
        """Equity total de la cuenta en USD (marginSummary.accountValue)."""
        state = await self.get_user_state(address)
        if not state:
            return 0.0
        try:
            return float(
                state.get("marginSummary", {}).get("accountValue", 0) or 0
            )
        except (TypeError, ValueError):
            return 0.0

    async def get_position_value(self, address: str, coin: str) -> float:
        """Valor nocional de la posición abierta en un asset concreto."""
        state = await self.get_user_state(address)
        if not state:
            return 0.0
        for p in state.get("assetPositions", []):
            pos = p.get("position", {})
            if pos.get("coin", "").upper() == coin.upper():
                try:
                    szi = float(pos.get("szi", 0) or 0)
                    entry_px = float(pos.get("entryPx", 0) or 0)
                    return abs(szi * entry_px)
                except (TypeError, ValueError):
                    pass
        return 0.0

    # ── Datos de mercado agregados ─────────────────────────────────────────────

    async def get_all_mids(self) -> dict[str, float]:
        """Precios mid actuales para todos los assets. Devuelve {coin: mid_price}."""
        data = await self._post({"type": "allMids"})
        if not data or not isinstance(data, dict):
            return {}
        result: dict[str, float] = {}
        for coin, price in data.items():
            try:
                result[coin] = float(price)
            except (TypeError, ValueError):
                pass
        return result

    async def get_leaderboard(self, window: str = "day") -> list[dict]:
        """Top traders del leaderboard público.

        window: 'day' | 'week' | 'month' | 'allTime'
        Cada fila puede tener 'ethAddress' o 'address' con la dirección de la wallet.
        """
        data = await self._post({"type": "leaderboard", "leaderboardWindow": window})
        if not data:
            return []
        # La API puede devolver un dict con 'leaderboardRows' o directamente una lista
        if isinstance(data, dict):
            return data.get("leaderboardRows", [])
        if isinstance(data, list):
            return data
        return []


# ── Cliente WebSocket ─────────────────────────────────────────────────────────

class HyperliquidWSClient:
    """Suscriptor WebSocket a trades de Hyperliquid en tiempo real.

    Los mensajes de trades vía WS incluyen el campo 'users' con las
    direcciones [buyer, seller], lo que nos permite construir un pool
    de wallets activas sin necesitar endpoints de descubrimiento (que
    no existen en la API pública REST de Hyperliquid).

    Uso:
        ws = HyperliquidWSClient()
        ws.add_trade_callback(my_fn)   # fn(trade_dict) síncrono
        await ws.start(["BTC", "ETH"])
        ...
        await ws.stop()
    """

    _WS_RECONNECT_DELAY = 10   # segundos entre reconexiones
    _PING_INTERVAL = 30        # segundos entre pings JSON
    _SUBSCRIBE_PAUSE = 0.05    # pausa entre suscripciones para no saturar el servidor

    def __init__(self) -> None:
        self._callbacks: list[Callable[[dict], None]] = []
        self._coins: list[str] = []
        self._running = False
        self._task: Optional[asyncio.Task] = None

    def add_trade_callback(self, fn: Callable[[dict], None]) -> None:
        """Registra un callback síncrono que se invoca por cada trade recibido."""
        self._callbacks.append(fn)

    async def start(self, coins: list[str]) -> None:
        """Inicia la tarea WebSocket en background."""
        self._coins = list(coins)
        self._running = True
        self._task = asyncio.create_task(self._run(), name="hl-ws")
        logger.info("HL WebSocket: arrancando suscripción a %d assets.", len(coins))

    async def _run(self) -> None:
        while self._running:
            try:
                await self._connect_and_listen()
            except asyncio.CancelledError:
                break
            except Exception as exc:
                logger.warning(
                    "HL WebSocket desconectado: %s — reconectando en %ds",
                    exc, self._WS_RECONNECT_DELAY,
                )
                await asyncio.sleep(self._WS_RECONNECT_DELAY)

    async def _ping_loop(self, ws: aiohttp.ClientWebSocketResponse) -> None:
        """Envía ping JSON cada 30s para mantener la conexión viva.

        Hyperliquid usa pings a nivel de aplicación (JSON), no frames PING
        de protocolo WebSocket. Por eso no se usa heartbeat= en ws_connect.
        """
        while True:
            await asyncio.sleep(self._PING_INTERVAL)
            try:
                await ws.send_json({"method": "ping"})
            except Exception:
                break  # la conexión ya está caída; _connect_and_listen lo detectará

    async def _connect_and_listen(self) -> None:
        # Sin heartbeat= para evitar que aiohttp envíe frames PING de protocolo
        # que Hyperliquid no responde con PONG, lo que causa desconexiones.
        async with aiohttp.ClientSession() as session:
            async with session.ws_connect(HL_WS_URL) as ws:
                logger.info(
                    "HL WebSocket conectado. Suscribiendo a %d assets…",
                    len(self._coins),
                )
                for coin in self._coins:
                    await ws.send_json({
                        "method": "subscribe",
                        "subscription": {"type": "trades", "coin": coin},
                    })
                    await asyncio.sleep(self._SUBSCRIBE_PAUSE)

                ping_task = asyncio.create_task(self._ping_loop(ws))
                try:
                    async for msg in ws:
                        if not self._running:
                            break
                        if msg.type == aiohttp.WSMsgType.TEXT:
                            self._dispatch(msg.data)
                        elif msg.type in (
                            aiohttp.WSMsgType.ERROR,
                            aiohttp.WSMsgType.CLOSED,
                        ):
                            logger.warning("HL WebSocket: mensaje de cierre/error.")
                            break
                finally:
                    ping_task.cancel()
                    try:
                        await ping_task
                    except asyncio.CancelledError:
                        pass

    def _dispatch(self, raw: str) -> None:
        try:
            msg = json.loads(raw)
        except Exception:
            return
        channel = msg.get("channel")
        if channel == "pong":
            return  # respuesta al ping JSON, ignorar
        if channel != "trades":
            return
        data = msg.get("data", [])
        trades = data if isinstance(data, list) else [data]
        for trade in trades:
            if not isinstance(trade, dict):
                continue
            for cb in self._callbacks:
                try:
                    cb(trade)
                except Exception as exc:
                    logger.debug("Error en callback WS trade: %s", exc)

    async def stop(self) -> None:
        self._running = False
        if self._task:
            self._task.cancel()
            try:
                await self._task
            except asyncio.CancelledError:
                pass
