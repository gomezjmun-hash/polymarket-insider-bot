"""Configuración centralizada del bot."""
import os
from dotenv import load_dotenv

load_dotenv()

# ── Telegram ──────────────────────────────────────────────────────────────────
TELEGRAM_BOT_TOKEN: str = os.getenv("TELEGRAM_BOT_TOKEN", "")
TELEGRAM_CHAT_ID: str = os.getenv("TELEGRAM_CHAT_ID", "")

# ── Blockchain ────────────────────────────────────────────────────────────────
POLYGON_RPC_URL: str = os.getenv("POLYGON_RPC_URL", "https://polygon-rpc.com")

# ── Aplicación ────────────────────────────────────────────────────────────────
LOG_LEVEL: str = os.getenv("LOG_LEVEL", "INFO")
DB_PATH: str = os.getenv("DB_PATH", "insider_bot.db")
POLL_INTERVAL_SECONDS: int = int(os.getenv("POLL_INTERVAL_SECONDS", "300"))

# ── Keywords geopolíticos ─────────────────────────────────────────────────────
GEO_KEYWORDS = [
    "israel", "iran", "gaza", "ceasefire", "hostilities",
    "ukraine", "russia", "war", "peace", "troops",
    "attack", "nuclear", "sanctions",
]

# Palabras que indican contexto deportivo, electoral u otro no geopolítico.
# Si alguna aparece en el título/descripción, el mercado se descarta.
GEO_EXCLUDE_KEYWORDS = [
    "fifa", "world cup", "olympics", "olympic",
    "tournament", "championship", "league", "match",
    "game", "sport", "sports",
    "win the election", "election", "ballot", "vote", "votes", "polling",
    "nba", "nfl", "nhl", "mlb", "ufc", "mma",
    "soccer", "football", "basketball", "baseball", "tennis",
    "cup final", "super bowl", "world series",
]

# ── Umbrales de score ─────────────────────────────────────────────────────────
SCORE_HIGH = 80
SCORE_MEDIUM = 50

# ── Umbral mínimo de posición para generar alerta ─────────────────────────────
MIN_POSITION_USD = 10_000

# ── Polymarket ────────────────────────────────────────────────────────────────
GAMMA_API_BASE = "https://gamma-api.polymarket.com"
CLOB_API_BASE = "https://clob.polymarket.com"
DATA_API_BASE = "https://data-api.polymarket.com"

# ── Scoring weights ───────────────────────────────────────────────────────────
SCORE_WALLET_NEW_7D = 30
SCORE_FEW_TRADES = 25
SCORE_FUNDS_FROM_CEX = 20
SCORE_NO_DEFI = 10
SCORE_ONE_DIRECTION = 10
SCORE_GROUP_ENTRY_2H = 25
SCORE_SHARED_ORIGIN = 40
SCORE_FAST_FIRST_BET = 15     # wallet creada y apuesta < 48h
SCORE_LARGE_POSITION = 15     # > 5000 USD
SCORE_CONCENTRATED = 10       # > 70% capital en un mercado

# Heurístico: dirección de contratos DeFi conocidos en Polygon
DEFI_CONTRACTS = {
    "uniswap_v3_router": "0xE592427A0AEce92De3Edee1F18E0157C05861564",
    "aave_v3_pool":       "0x794a61358D6845594F94dc1DB02A252b5b4814aD",
    "quickswap_router":   "0xa5E0829CaCEd8fFDD4De3c43696c57F7D7A678ff",
    "sushiswap_router":   "0x1b02dA8Cb0d097eB8D57A175b88c7D8b47997506",
}

# ── Hyperliquid ───────────────────────────────────────────────────────────────
# Assets fijos monitorizados por categoría.
# Los altcoins con > $1M de volumen 24h se añaden automáticamente como CRYPTO.
HL_CRYPTO_ASSETS: list[str] = [
    "BTC", "ETH", "SOL", "BNB", "AVAX", "LINK", "ARB", "OP",
    "MATIC", "DOGE", "ADA", "DOT", "ATOM", "UNI", "AAVE",
    "SUI", "APT", "INJ", "TIA", "SEI",
]
HL_BOLSA_ASSETS: list[str] = [
    # Índices
    "SPX", "NDX", "DJI",
    # Acciones individuales (si Hyperliquid las lista)
    "TSLA", "AAPL", "NVDA", "AMZN", "MSFT", "GOOGL", "META",
]
HL_COMMODITY_ASSETS: list[str] = [
    "WTI", "BRENT", "XAU", "XAG", "NG",
]

# Umbrales de nocional mínimo para generar alerta (USD)
HL_MIN_USD_CRYPTO: int    = 500_000   # crypto y bolsa
HL_MIN_USD_BOLSA: int     = 500_000
HL_MIN_USD_COMMODITY: int = 100_000   # commodities / geopolítica

# Exchanges CEX conocidos (simplificado; se amplía en polygon_rpc.py)
CEX_LABELS = {
    "0xb5d85cbf7cb3ee0d56b3bb207d5fc4b82f43f511": "Coinbase",
    "0xa9d1e08c7793af67e9d92fe308d5697fb81d3e43": "Coinbase",
    "0x0d0707963952f2fba59dd06f2b425ace40b492fe": "Gate.io",
    "0xe93685f3bba03016f02bd1828badd6195988d950": "Binance",
    "0xf977814e90da44bfa03b6295a0616a897441acec": "Binance",
    "0x1ab4973a48dc892cd9971ece8e01dcc7688f8f23": "Binance",
    "0x28c6c06298d514db089934071355e5743bf21d60": "Binance",
}
