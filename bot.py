import os
import re
import sqlite3
import discord
import datetime

# --- Discord setup ---
intents = discord.Intents.default()
intents.message_content = True
client = discord.Client(intents=intents)

# --- Database setup ---
conn = sqlite3.connect("bets.db")
c = conn.cursor()
c.execute("""
CREATE TABLE IF NOT EXISTS bets (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    bet_text TEXT,
    units REAL,
    odds TEXT,
    status TEXT,
    result REAL,
    date DATE
)
""")
conn.commit()

# --- Config ---
CHANNEL_ID = int(os.getenv("CHANNEL_ID", "0"))  # set in Railway env vars
TOKEN = os.getenv("DISCORD_TOKEN")

# --- Helpers ---
def parse_bet(text: str):
    """Parse a bet line and return (units, odds, status, result)."""
    # Units (default 1)
    unit_match = re.search(r"(\d+(\.\d+)?)\s*u", text.lower())
    units = float(unit_match.group(1)) if unit_match else 1.0

    # Odds (optional)
    odds_match = re.search(r"([+-]\d+)", text)
    odds = odds_match.group(1) if odds_match else None

    # Result
    if "✅" in text:
        status = "win"
        result = units
    elif "❌" in text:
        status = "loss"
        result = -units
    else:
        return None

    return units, odds, status, result

async def log_bet(message, parsed):
    units, odds, status, result = parsed
    c.execute(
        "INSERT INTO bets (bet_text, units, odds, status, result, date) VALUES (?, ?, ?, ?, ?, ?)",
        (message.content.strip(), units, odds, status, result, message.created_at.date())
    )
    conn.commit()
    await message.channel.send(f"Logged: {message.content.strip()} ({result:+}u)")

# --- Events ---
@client.event
async def on_ready():
    print(f"Logged in as {client.user}")
    if CHANNEL_ID:
        channel = client.get_channel(CHANNEL_ID)
        # Backfill history on startup
        async for message in channel.history(limit=None, oldest_first=True):
            parsed = parse_bet(message.content)
            if parsed:
                # Avoid double logging if already in DB
                c.execute("SELECT 1 FROM bets WHERE bet_text=? AND date=?", 
                          (message.content.strip(), message.created_at.date()))
                if not c.fetchone():
                    await log_bet(message, parsed)
        print("Backfill complete.")

@client.event
async def on_message(message):
    if message.author.bot or message.channel.id != CHANNEL_ID:
        return

    parsed = parse_bet(message.content)
    if parsed:
        await log_bet(message, parsed)

    # Optional: simple command for all-time record
    if message.content.strip().lower() == "!record":
        c.execute("SELECT COUNT(*), SUM(result) FROM bets")
        total, pnl = c.fetchone()
        pnl = pnl if pnl else 0
        await message.channel.send(f"All-time record: {total} bets, Net {pnl:+}u")

# --- Run ---
if __name__ == "__main__":
    if not TOKEN:
        raise RuntimeError("DISCORD_TOKEN not set in environment variables")
    client.run(TOKEN)
