"""Capa de persistencia SQLite (async)."""
import json
import logging
from datetime import datetime, timedelta
from typing import Optional

import aiosqlite

from config import DB_PATH

logger = logging.getLogger(__name__)


async def init_db() -> None:
    async with aiosqlite.connect(DB_PATH) as db:
        await db.executescript("""
            CREATE TABLE IF NOT EXISTS alerts (
                id          INTEGER PRIMARY KEY AUTOINCREMENT,
                created_at  TEXT    NOT NULL,
                market_id   TEXT    NOT NULL,
                market_name TEXT    NOT NULL,
                wallet      TEXT    NOT NULL,
                score       INTEGER NOT NULL,
                breakdown   TEXT    NOT NULL,   -- JSON
                amount_usd  REAL    NOT NULL,
                direction   TEXT    NOT NULL,   -- YES / NO
                level       TEXT    NOT NULL,   -- HIGH / MEDIUM
                event_state TEXT    DEFAULT 'pending',
                notified    INTEGER DEFAULT 0
            );

            CREATE TABLE IF NOT EXISTS markets_seen (
                market_id   TEXT PRIMARY KEY,
                question    TEXT,
                last_seen   TEXT
            );

            CREATE TABLE IF NOT EXISTS wallet_cache (
                wallet      TEXT PRIMARY KEY,
                first_seen  TEXT,
                age_days    REAL,
                trade_count INTEGER,
                has_defi    INTEGER,
                no_defi     INTEGER,
                raw_data    TEXT    -- JSON
            );

            CREATE INDEX IF NOT EXISTS idx_alerts_wallet    ON alerts(wallet);
            CREATE INDEX IF NOT EXISTS idx_alerts_market    ON alerts(market_id);
            CREATE INDEX IF NOT EXISTS idx_alerts_created   ON alerts(created_at);
        """)
        await db.commit()
    logger.info("Base de datos inicializada: %s", DB_PATH)


async def insert_alert(
    market_id: str,
    market_name: str,
    wallet: str,
    score: int,
    breakdown: dict,
    amount_usd: float,
    direction: str,
    level: str,
) -> int:
    async with aiosqlite.connect(DB_PATH) as db:
        cursor = await db.execute(
            """INSERT INTO alerts
               (created_at, market_id, market_name, wallet, score, breakdown,
                amount_usd, direction, level)
               VALUES (?,?,?,?,?,?,?,?,?)""",
            (
                datetime.utcnow().isoformat(),
                market_id, market_name, wallet, score,
                json.dumps(breakdown), amount_usd, direction, level,
            ),
        )
        await db.commit()
        return cursor.lastrowid


async def alert_exists(market_id: str, wallet: str) -> bool:
    """Evita duplicar alertas para el mismo (mercado, wallet) en 24h."""
    cutoff = (datetime.utcnow() - timedelta(hours=24)).isoformat()
    async with aiosqlite.connect(DB_PATH) as db:
        async with db.execute(
            "SELECT 1 FROM alerts WHERE market_id=? AND wallet=? AND created_at>? LIMIT 1",
            (market_id, wallet, cutoff),
        ) as cur:
            return await cur.fetchone() is not None


async def get_history(days: int = 7) -> list[dict]:
    cutoff = (datetime.utcnow() - timedelta(days=days)).isoformat()
    async with aiosqlite.connect(DB_PATH) as db:
        db.row_factory = aiosqlite.Row
        async with db.execute(
            "SELECT * FROM alerts WHERE created_at>? ORDER BY created_at DESC",
            (cutoff,),
        ) as cur:
            rows = await cur.fetchall()
    return [dict(r) for r in rows]


async def get_top_wallets(days: int = 7, limit: int = 10) -> list[dict]:
    cutoff = (datetime.utcnow() - timedelta(days=days)).isoformat()
    async with aiosqlite.connect(DB_PATH) as db:
        db.row_factory = aiosqlite.Row
        async with db.execute(
            """SELECT wallet, COUNT(*) as appearances, MAX(score) as max_score,
                      SUM(amount_usd) as total_usd
               FROM alerts WHERE created_at>?
               GROUP BY wallet ORDER BY appearances DESC LIMIT ?""",
            (cutoff, limit),
        ) as cur:
            rows = await cur.fetchall()
    return [dict(r) for r in rows]


async def mark_notified(alert_id: int) -> None:
    async with aiosqlite.connect(DB_PATH) as db:
        await db.execute("UPDATE alerts SET notified=1 WHERE id=?", (alert_id,))
        await db.commit()


async def update_event_state(alert_id: int, state: str) -> None:
    async with aiosqlite.connect(DB_PATH) as db:
        await db.execute("UPDATE alerts SET event_state=? WHERE id=?", (state, alert_id))
        await db.commit()


async def get_unnotified_alerts() -> list[dict]:
    async with aiosqlite.connect(DB_PATH) as db:
        db.row_factory = aiosqlite.Row
        async with db.execute(
            "SELECT * FROM alerts WHERE notified=0 ORDER BY created_at ASC"
        ) as cur:
            rows = await cur.fetchall()
    return [dict(r) for r in rows]


async def save_wallet_cache(wallet: str, data: dict) -> None:
    async with aiosqlite.connect(DB_PATH) as db:
        await db.execute(
            """INSERT OR REPLACE INTO wallet_cache
               (wallet, first_seen, age_days, trade_count, has_defi, no_defi, raw_data)
               VALUES (?,?,?,?,?,?,?)""",
            (
                wallet,
                data.get("first_seen"),
                data.get("age_days", 0),
                data.get("trade_count", 0),
                int(data.get("has_defi", False)),
                int(data.get("no_defi", True)),
                json.dumps(data),
            ),
        )
        await db.commit()


async def get_wallet_cache(wallet: str) -> Optional[dict]:
    async with aiosqlite.connect(DB_PATH) as db:
        db.row_factory = aiosqlite.Row
        async with db.execute(
            "SELECT * FROM wallet_cache WHERE wallet=?", (wallet,)
        ) as cur:
            row = await cur.fetchone()
    if row:
        d = dict(row)
        d.update(json.loads(d.get("raw_data") or "{}"))
        return d
    return None


async def get_alerts_for_backtest(market_id: str) -> list[dict]:
    async with aiosqlite.connect(DB_PATH) as db:
        db.row_factory = aiosqlite.Row
        async with db.execute(
            "SELECT * FROM alerts WHERE market_id=? ORDER BY created_at ASC",
            (market_id,),
        ) as cur:
            rows = await cur.fetchall()
    return [dict(r) for r in rows]
