"""Cliente para Polymarket Gamma API y CLOB API."""
import logging
from typing import Any, Optional
from datetime import datetime, timezone

import aiohttp

from config import GAMMA_API_BASE, CLOB_API_BASE, DATA_API_BASE, GEO_KEYWORDS, GEO_EXCLUDE_KEYWORDS

logger = logging.getLogger(__name__)

# Tiempo máximo de espera por request
REQUEST_TIMEOUT = aiohttp.ClientTimeout(total=20)


def _matches_keywords(text: str) -> bool:
    t = text.lower()
    return any(kw in t for kw in GEO_KEYWORDS)


def _is_excluded(text: str) -> bool:
    t = text.lower()
    return any(kw in t for kw in GEO_EXCLUDE_KEYWORDS)


class PolymarketClient:
    def __init__(self) -> None:
        self._session: Optional[aiohttp.ClientSession] = None

    async def _get_session(self) -> aiohttp.ClientSession:
        if self._session is None or self._session.closed:
            self._session = aiohttp.ClientSession(timeout=REQUEST_TIMEOUT)
        return self._session

    async def close(self) -> None:
        if self._session and not self._session.closed:
            await self._session.close()

    async def _get(self, base: str, path: str, params: dict | None = None) -> Any:
        session = await self._get_session()
        url = f"{base}{path}"
        try:
            async with session.get(url, params=params) as resp:
                if resp.status != 200:
                    logger.warning("GET %s -> HTTP %s", url, resp.status)
                    return None
                return await resp.json(content_type=None)
        except Exception as exc:
            logger.error("Error GET %s: %s", url, exc)
            return None

    # ── Mercados ──────────────────────────────────────────────────────────────

    async def get_geo_markets(self, limit: int = 200) -> list[dict]:
        """Devuelve mercados activos que contengan keywords geopolíticos."""
        data = await self._get(GAMMA_API_BASE, "/markets", {
            "active": "true",
            "closed": "false",
            "limit": limit,
        })
        if not data:
            return []

        markets = data if isinstance(data, list) else data.get("data", data.get("markets", []))
        geo = []
        for m in markets:
            question = m.get("question", "") or m.get("title", "")
            description = m.get("description", "")
            combined = f"{question} {description}"
            if (_matches_keywords(question) or _matches_keywords(description)) and not _is_excluded(combined):
                geo.append(m)

        logger.info("Mercados geopolíticos activos encontrados: %d", len(geo))
        return geo

    async def get_market(self, market_id: str) -> Optional[dict]:
        return await self._get(GAMMA_API_BASE, f"/markets/{market_id}")

    # ── Trades / Actividad ────────────────────────────────────────────────────

    async def get_recent_trades(self, market_condition_id: str, limit: int = 100) -> list[dict]:
        """Obtiene trades recientes de un mercado via Data API (publica)."""
        data = await self._get(DATA_API_BASE, "/trades", {
            "market": market_condition_id,
            "limit": limit,
        })
        if not data:
            return []
        return data if isinstance(data, list) else data.get("data", [])

    async def get_trades_for_wallet(self, wallet: str, limit: int = 200) -> list[dict]:
        """Historial de trades de una wallet en Polymarket via Data API."""
        data = await self._get(DATA_API_BASE, "/activity", {
            "user": wallet.lower(),
            "limit": limit,
        })
        if not data:
            return []
        trades = data if isinstance(data, list) else data.get("data", [])
        # solo entradas de tipo TRADE
        return [t for t in trades if t.get("type", "TRADE") == "TRADE"]

    async def get_positions_for_wallet(self, wallet: str) -> list[dict]:
        """Posiciones abiertas de una wallet (Data API)."""
        data = await self._get(DATA_API_BASE, "/positions", {
            "user": wallet.lower(),
            "limit": 500,
        })
        if not data:
            return []
        return data if isinstance(data, list) else data.get("data", [])

    async def get_large_trades(
        self,
        market_id: str,
        min_usd: float = 500,
        limit: int = 200,
    ) -> list[dict]:
        """Filtra trades grandes en un mercado."""
        trades = await self.get_recent_trades(market_id, limit)
        big = []
        for t in trades:
            try:
                size = float(t.get("size", 0) or t.get("usdcSize", 0) or 0)
                price = float(t.get("price", 1) or 1)
                usd_value = size * price if size < 1e6 else size / 1e6
                # algunos campos ya vienen en USDC directamente
                usdc = float(t.get("usdcSize", 0) or 0)
                value = max(usd_value, usdc)
                if value >= min_usd:
                    t["_usd_value"] = value
                    big.append(t)
            except (TypeError, ValueError):
                continue
        return big

    async def get_wallet_polymarket_age(self, wallet: str) -> Optional[datetime]:
        """Primera vez que la wallet aparece en trades de Polymarket."""
        trades = await self.get_trades_for_wallet(wallet, limit=500)
        if not trades:
            return None
        dates = []
        for t in trades:
            ts = t.get("timestamp") or t.get("createdAt") or t.get("created_at")
            if ts:
                try:
                    if isinstance(ts, (int, float)):
                        dates.append(datetime.fromtimestamp(ts, tz=timezone.utc))
                    else:
                        dates.append(datetime.fromisoformat(str(ts).replace("Z", "+00:00")))
                except Exception:
                    pass
        return min(dates) if dates else None
