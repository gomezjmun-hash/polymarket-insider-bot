"""Módulo de backtest: aplica los filtros a mercados pasados."""
import asyncio
import json
import logging
from datetime import datetime

from database import get_alerts_for_backtest, get_history
from polymarket_api import PolymarketClient
from wallet_scorer import breakdown_text

logger = logging.getLogger(__name__)


async def run_backtest(market_id: str) -> str:
    """
    Dado un market_id, devuelve un resumen de las wallets detectadas
    y si sus predicciones acertaron según el estado guardado.
    """
    rows = await get_alerts_for_backtest(market_id)
    if not rows:
        return f"No hay alertas guardadas para el mercado {market_id}."

    # Intenta obtener el resultado del mercado
    poly = PolymarketClient()
    market = await poly.get_market(market_id)
    await poly.close()

    resolved = None
    if market:
        # Campo que indica si resolvió
        resolved = market.get("resolutionSource") or market.get("resolution")
        closed = market.get("closed", False)

    lines = [
        f"📊 *Backtest: {market.get('question', market_id) if market else market_id}*\n",
        f"Estado del mercado: {'cerrado' if (market and market.get('closed')) else 'activo'}",
        f"Resolución: {resolved or 'no disponible'}\n",
        f"Total alertas detectadas: {len(rows)}\n",
    ]

    high_rows = [r for r in rows if r["level"] == "HIGH"]
    medium_rows = [r for r in rows if r["level"] == "MEDIUM"]

    for label, group in (("🔴 ALTA SOSPECHA", high_rows), ("🟡 MEDIA SOSPECHA", medium_rows)):
        if not group:
            continue
        lines.append(f"\n{label} ({len(group)} wallets):")
        for r in group:
            bd = json.loads(r.get("breakdown") or "{}")
            state = r.get("event_state", "pendiente")
            state_emoji = "✅" if state == "acertó" else ("❌" if state == "falló" else "⏳")
            lines.append(
                f"  {state_emoji} `{r['wallet'][:12]}…` | "
                f"Score {r['score']} | "
                f"${r['amount_usd']:,.0f} {r['direction']} | "
                f"{r['created_at'][:10]} | Estado: {state}"
            )

    # Estadísticas de precisión
    resolved_rows = [r for r in rows if r.get("event_state") in ("acertó", "falló")]
    if resolved_rows:
        hits = sum(1 for r in resolved_rows if r.get("event_state") == "acertó")
        accuracy = hits / len(resolved_rows) * 100
        lines.append(
            f"\n📈 *Precisión (wallets HIGH+MEDIUM):* "
            f"{hits}/{len(resolved_rows)} ({accuracy:.0f}%)"
        )
    else:
        lines.append("\n⏳ Sin resultados confirmados aún. Usa /resuelto <id> <si|no>.")

    return "\n".join(lines)


async def backtest_all_markets(days: int = 30) -> str:
    """Resumen de backtest para todos los mercados con alertas en los últimos N días."""
    rows = await get_history(days=days)
    if not rows:
        return "Sin historial de alertas."

    market_ids = list({r["market_id"] for r in rows})
    lines = [f"🔬 *Backtest global ({days} días, {len(market_ids)} mercados)*\n"]

    for mid in market_ids[:10]:  # limita a 10 mercados para no saturar
        market_rows = [r for r in rows if r["market_id"] == mid]
        name = market_rows[0].get("market_name", mid)[:50]
        resolved = [r for r in market_rows if r.get("event_state") in ("acertó", "falló")]
        hits = sum(1 for r in resolved if r.get("event_state") == "acertó")
        accuracy_str = (
            f"{hits}/{len(resolved)} ({hits/len(resolved)*100:.0f}%)"
            if resolved else "pendiente"
        )
        lines.append(
            f"• {name}\n"
            f"  Alertas: {len(market_rows)} | Precisión: {accuracy_str}"
        )

    return "\n".join(lines)
