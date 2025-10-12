# utils/db.py
import os
import logging
import asyncpg
from typing import Optional

logger = logging.getLogger("recap-bot.db")

DATABASE_URL = os.environ.get("DATABASE_URL")
if not DATABASE_URL:
    raise RuntimeError("DATABASE_URL must be set in environment")

db_pool: Optional[asyncpg.pool.Pool] = None

async def init_db():
    """
    Initialize DB pool and ensure required tables exist.
    This will be called at bot startup.
    """
    global db_pool
    if db_pool:
        return
    db_pool = await asyncpg.create_pool(DATABASE_URL, min_size=1, max_size=6)
    logger.info("Postgres pool created")
    await _ensure_schema()

async def _ensure_schema():
    assert db_pool is not None
    async with db_pool.acquire() as conn:
        # schema for settings, recaps, bets, import_progress
        await conn.execute("""
        CREATE TABLE IF NOT EXISTS settings (
            guild_id BIGINT PRIMARY KEY,
            recap_channel_id BIGINT,
            automation_enabled BOOLEAN DEFAULT FALSE,
            automation_channel_id BIGINT NULL,
            automation_cron TEXT NULL
        );

        CREATE TABLE IF NOT EXISTS daily_recaps (
            id SERIAL PRIMARY KEY,
            guild_id BIGINT NOT NULL,
            channel_id BIGINT NOT NULL,
            message_id BIGINT UNIQUE NOT NULL,
            recap_date DATE NOT NULL,
            wins INT NOT NULL DEFAULT 0,
            losses INT NOT NULL DEFAULT 0,
            pushes INT NOT NULL DEFAULT 0,
            hooks INT NOT NULL DEFAULT 0,
            total_units NUMERIC NOT NULL DEFAULT 0,
            created_at TIMESTAMP WITH TIME ZONE DEFAULT NOW(),
            updated_at TIMESTAMP WITH TIME ZONE DEFAULT NOW()
        );

        CREATE TABLE IF NOT EXISTS bets (
            id SERIAL PRIMARY KEY,
            recap_id INT REFERENCES daily_recaps(id) ON DELETE CASCADE,
            sport TEXT,
            units NUMERIC NOT NULL DEFAULT 1,
            description TEXT,
            odds TEXT,
            result TEXT,
            created_at TIMESTAMP WITH TIME ZONE DEFAULT NOW()
        );

        -- Used to resume history imports without re-processing everything
        CREATE TABLE IF NOT EXISTS import_progress (
            guild_id BIGINT NOT NULL,
            channel_id BIGINT NOT NULL,
            last_message_id BIGINT,
            PRIMARY KEY (guild_id, channel_id)
        );
        """)
    logger.info("DB schema ensured")
