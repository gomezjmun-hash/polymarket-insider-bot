"""Sistema de puntuación de wallets sospechosas."""
import logging
from dataclasses import dataclass, field
from datetime import datetime, timezone, timedelta
from typing import Optional

from config import (
    SCORE_WALLET_NEW_7D, SCORE_FEW_TRADES, SCORE_FUNDS_FROM_CEX,
    SCORE_NO_DEFI, SCORE_ONE_DIRECTION, SCORE_GROUP_ENTRY_2H,
    SCORE_SHARED_ORIGIN, SCORE_FAST_FIRST_BET, SCORE_LARGE_POSITION,
    SCORE_CONCENTRATED, SCORE_HIGH, SCORE_MEDIUM,
)

logger = logging.getLogger(__name__)


@dataclass
class ScoreResult:
    wallet: str
    total: int = 0
    breakdown: dict = field(default_factory=dict)
    level: str = "LOW"

    def add(self, key: str, points: int, reason: str) -> None:
        self.total += points
        self.breakdown[key] = {"points": points, "reason": reason}

    def finalize(self) -> None:
        if self.total >= SCORE_HIGH:
            self.level = "HIGH"
        elif self.total >= SCORE_MEDIUM:
            self.level = "MEDIUM"
        else:
            self.level = "LOW"


@dataclass
class WalletContext:
    """Todos los datos necesarios para puntuar una wallet."""
    wallet: str
    age_days: float                     # edad en días
    poly_trade_count: int               # trades históricos en Polymarket
    funding_source: Optional[str]       # CEX label o None
    has_defi: bool                      # interactuó con DeFi
    first_poly_date: Optional[datetime] # primera apuesta en Polymarket
    wallet_created: Optional[datetime]  # primera tx on-chain
    amount_usd: float                   # importe de esta apuesta
    direction: str                      # YES o NO
    has_hedge: bool                     # tiene apuesta contraria activa
    group_wallets: list[str]            # otras wallets que entraron en ventana 2h
    shared_origin_wallets: list[str]    # wallets con mismo origen que ésta
    total_portfolio_usd: float          # capital total apostado en Polymarket
    market_position_usd: float          # capital en este mercado


async def score_wallet(ctx: WalletContext) -> ScoreResult:
    result = ScoreResult(wallet=ctx.wallet)

    # 1. Wallet creada hace menos de 7 días
    if ctx.age_days < 7:
        result.add(
            "wallet_new_7d",
            SCORE_WALLET_NEW_7D,
            f"Wallet creada hace {ctx.age_days:.1f} días (< 7)",
        )

    # 2. Pocos trades históricos en Polymarket
    if ctx.poly_trade_count < 5:
        result.add(
            "few_trades",
            SCORE_FEW_TRADES,
            f"Solo {ctx.poly_trade_count} trades previos en Polymarket (< 5)",
        )

    # 3. Fondos desde CEX justo antes de apostar
    if ctx.funding_source:
        result.add(
            "funds_from_cex",
            SCORE_FUNDS_FROM_CEX,
            f"Fondos recibidos desde {ctx.funding_source} antes de apostar",
        )

    # 4. Sin actividad DeFi previa
    if not ctx.has_defi:
        result.add(
            "no_defi",
            SCORE_NO_DEFI,
            "Wallet sin actividad DeFi previa (Uniswap, Aave, etc.)",
        )

    # 5. Apuesta en una sola dirección (sin hedge)
    if not ctx.has_hedge:
        result.add(
            "one_direction",
            SCORE_ONE_DIRECTION,
            f"Apuesta solo {ctx.direction}, sin posición contraria (hedge)",
        )

    # 6. Entrada en ventana grupal de 2 horas con otras wallets
    if len(ctx.group_wallets) > 0:
        result.add(
            "group_entry_2h",
            SCORE_GROUP_ENTRY_2H,
            f"Entrada coordinada con {len(ctx.group_wallets)} wallet(s) "
            f"en ventana de 2h: {ctx.group_wallets[:3]}",
        )

    # 7. Fondos del mismo address origen que otras wallets
    if len(ctx.shared_origin_wallets) > 0:
        result.add(
            "shared_origin",
            SCORE_SHARED_ORIGIN,
            f"Mismo address de fondos que {len(ctx.shared_origin_wallets)} "
            f"wallet(s) sospechosas: {ctx.shared_origin_wallets[:3]}",
        )

    # 8. Primera apuesta < 48h después de creación de wallet
    if ctx.wallet_created and ctx.first_poly_date:
        delta_h = (ctx.first_poly_date - ctx.wallet_created).total_seconds() / 3600
        if 0 <= delta_h < 48:
            result.add(
                "fast_first_bet",
                SCORE_FAST_FIRST_BET,
                f"Primera apuesta {delta_h:.0f}h después de creación de wallet (< 48h)",
            )

    # 9. Posición mayor de 5000 USD
    if ctx.amount_usd > 5000:
        result.add(
            "large_position",
            SCORE_LARGE_POSITION,
            f"Posición de ${ctx.amount_usd:,.0f} (> $5,000)",
        )

    # 10. Más del 70% del capital en un solo mercado
    if ctx.total_portfolio_usd > 0:
        pct = ctx.market_position_usd / ctx.total_portfolio_usd * 100
        if pct > 70:
            result.add(
                "concentrated",
                SCORE_CONCENTRATED,
                f"{pct:.0f}% del capital en este mercado (> 70%)",
            )

    result.finalize()
    logger.debug(
        "Score %s: %d pts (%s) → %s",
        ctx.wallet, result.total, list(result.breakdown.keys()), result.level,
    )
    return result


def breakdown_text(breakdown: dict) -> str:
    """Formatea el desglose de puntos para Telegram."""
    lines = []
    for key, v in breakdown.items():
        lines.append(f"  • +{v['points']} pts — {v['reason']}")
    return "\n".join(lines) if lines else "  (sin detalles)"
