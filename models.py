# models.py
from db import exec_safe, conn

def bootstrap_schema():
    exec_safe("""
    CREATE TABLE IF NOT EXISTS bets (
        id SERIAL PRIMARY KEY,
        bet_text TEXT,
        units REAL,
        odds TEXT,
        status TEXT,
        result REAL,
        date DATE,
        guild_id BIGINT,
        sport TEXT,
        bet_type TEXT,
        posted_line REAL,
        posted_side TEXT,
        closing_line REAL,
        closing_odds TEXT
    )
    """)
    exec_safe("""
    CREATE TABLE IF NOT EXISTS settings (
        guild_id BIGINT PRIMARY KEY,
        channel_id BIGINT,
        override_date DATE
    )
    """)
    exec_safe("""
    CREATE TABLE IF NOT EXISTS closings (
        id SERIAL PRIMARY KEY,
        guild_id BIGINT,
        event_key TEXT,
        closing_line REAL,
        closing_odds TEXT,
        source TEXT,
        fetched_at TIMESTAMP
    )
    """)
    exec_safe("""
    CREATE TABLE IF NOT EXISTS clv_fixes (
        id SERIAL PRIMARY KEY,
        bet_id INT REFERENCES bets(id) ON DELETE CASCADE,
        guild_id BIGINT,
        candidates TEXT[],
        created_at TIMESTAMP DEFAULT NOW(),
        resolved BOOLEAN DEFAULT FALSE
    )
    """)
    conn.commit()

# --- Settings helpers ---
def get_channel_id(guild_id):
    row = exec_safe("SELECT channel_id FROM settings WHERE guild_id=%s", (guild_id,), fetch="one")
    return row[0] if row and row[0] else None

def get_override_date(guild_id):
    row = exec_safe("SELECT override_date FROM settings WHERE guild_id=%s", (guild_id,), fetch="one")
    return row[0] if row and row[0] else None

def clear_override_date(guild_id):
    exec_safe("UPDATE settings SET override_date=NULL WHERE guild_id=%s", (guild_id,))
    conn.commit()
