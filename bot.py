import os
import re
import psycopg2
from urllib.parse import urlparse
import discord
import datetime

# --- Discord setup ---
intents = discord.Intents.default()
intents.message_content = True
client = discord.Client(intents=intents)

# --- Database setup ---
db_url = os.getenv("DATABASE_URL")
if not db_url:
    raise RuntimeError("DATABASE_URL not set in environment variables")

url = urlparse(db_url)
conn = psycopg2.connect(
    dbname=url.path[1:],
    user=url.username,
    password=url.password,
    host=url.hostname,
    port=url.port
)
c = conn.cursor()

c.execute("""
CREATE TABLE IF NOT EXISTS bets (
    id SERIAL PRIMARY KEY,
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
CHANNEL_ID = int(os.getenv("CHANNEL_ID", "0"))
TOKEN = os.getenv("DISCORD_TOKEN")

# --- Helpers ---
def parse_bet(line: str):
    line = line.strip()
    if not line:
        return None
    if "✅" not in line and "❌" not in line:
        return None

    # Units (default 1)
    unit_match = re.search(r"(\d+(\.\d+)?)\s*u", line.lower())
    units = float(unit_match.group(1)) if unit_match else 1.0

    # Odds (optional)
    odds_match = re.search(r"([+-]\d+)", line)
    odds = odds_match.group(1) if odds_match else None

    # Result
    if "✅" in line:
        status = "win"
        result = units
    elif "❌" in line:
        status = "loss"
        result = -units
    else:
        return None

    return units, odds, status, result

async def log_bet(line_text, parsed, date, channel):
    units, odds, status, result = parsed
    c.execute(
        "INSERT INTO bets (bet_text, units, odds, status, result, date) VALUES (%s, %s, %s, %s, %s, %s)",
        (line_text.strip(), units, odds, status, result, date)
    )
    conn.commit()
    return f"{line_text.strip()} ({result:+}u)"

def get_record(start_date=None, end_date=None):
    query = "SELECT COUNT(*), SUM(result), SUM(CASE WHEN status='win' THEN 1 ELSE 0 END), SUM(CASE WHEN status='loss' THEN 1 ELSE 0 END) FROM bets"
    params = []
    if start_date and end_date:
        query += " WHERE date BETWEEN %s AND %s"
        params = [start_date, end_date]
    c.execute(query, params)
    total, pnl, wins, losses = c.fetchone()
    pnl = pnl if pnl else 0
    return total or 0, wins or 0, losses or 0, pnl

# --- Events ---
@client.event
async def on_ready():
    print(f"Logged in as {client.user}")

@client.event
async def on_message(message):
    if message.author.bot or message.channel.id != CHANNEL_ID:
        return

    # Commands
    if message.content.strip().lower() == "!daily":
        today = datetime.date.today()
        total, wins, losses, pnl = get_record(today, today)
        await message.channel.send(f"📅 Today: {wins}-{losses} ({total} plays), Net {pnl:+}u")
        return

    if message.content.strip().lower() == "!mtd":
        today = datetime.date.today()
        start = today.replace(day=1)
        total, wins, losses, pnl = get_record(start, today)
        await message.channel.send(f"📆 Month-to-date: {wins}-{losses} ({total} plays), Net {pnl:+}u")
        return

    if message.content.strip().lower() == "!alltime":
        total, wins, losses, pnl = get_record()
        await message.channel.send(f"🌍 All-time: {wins}-{losses} ({total} plays), Net {pnl:+}u")
        return

    # Parse bets line by line
    lines = message.content.splitlines()
    logged = []
    for line in lines:
        parsed = parse_bet(line)
        if parsed:
            logged_line = await log_bet(line, parsed, message.created_at.date(), message.channel)
            logged.append(logged_line)

    # If bets were logged, paste them back + daily record
    if logged:
        today = message.created_at.date()
        total, wins, losses, pnl = get_record(today, today)
        response = "Logged plays:\n" + "\n".join(logged)
        response += f"\n\n📅 {today}: {wins}-{losses} ({total} plays), Net {pnl:+}u"
        await message.channel.send(response)

# --- Run ---
if __name__ == "__main__":
    if not TOKEN:
        raise RuntimeError("DISCORD_TOKEN not set in environment variables")
    client.run(TOKEN)
