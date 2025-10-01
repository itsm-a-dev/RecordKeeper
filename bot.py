import os
import re
import psycopg2
from urllib.parse import urlparse
import discord
import datetime
import regex
import unicodedata

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

# Ensure tables exist
c.execute("""
CREATE TABLE IF NOT EXISTS bets (
    id SERIAL PRIMARY KEY,
    bet_text TEXT,
    units REAL,
    odds TEXT,
    status TEXT,
    result REAL,
    date DATE,
    guild_id BIGINT
)
""")
c.execute("""
CREATE TABLE IF NOT EXISTS settings (
    guild_id BIGINT PRIMARY KEY,
    channel_id BIGINT
)
""")
conn.commit()

TOKEN = os.getenv("DISCORD_TOKEN")

# --- Emoji + sentiment helpers ---
EMOJI_PATTERN = regex.compile(r"\p{Emoji}", flags=regex.UNICODE)

def extract_emojis(text: str):
    return EMOJI_PATTERN.findall(text)

def classify_emoji(e: str):
    """Classify emoji sentiment using its Unicode name + domain overrides."""
    name = unicodedata.name(e, "").upper()

    # Positive cues
    if any(word in name for word in ["CHECK", "GREEN", "SMILE", "PARTY", "TROPHY", "STAR", "FIRE"]):
        return "win"

    # Negative cues
    if any(word in name for word in ["CROSS", "SKULL", "ANGRY", "SICK", "CRY", "VOMIT"]):
        return "loss"

    # Hook emojis (🪝 or 🎣) → loss
    if e in {"🪝", "🎣"} or "HOOK" in name:
        return "loss"

    return None

def classify_line(line: str, units: float):
    """Decide win/loss/neutral from emojis or text keywords."""
    emojis = extract_emojis(line)
    for e in emojis:
        sentiment = classify_emoji(e)
        if sentiment == "win":
            return "win", units
        if sentiment == "loss":
            return "loss", -units

    # Fallback to text keywords
    lower = line.lower()
    if any(w in lower for w in ["win", "cash", "hit", "w "]):
        return "win", units
    if any(w in lower for w in ["loss", "miss", "l ", "lose"]):
        return "loss", -units
    if any(w in lower for w in ["push", "void", "cancel"]):
        return "push", 0

    return None, 0

# --- Date extraction ---
def extract_date_from_text(text: str):
    """Try to extract MM/DD from recap text."""
    date_match = re.search(r"(\d{1,2})/(\d{1,2})", text)
    if date_match:
        month, day = map(int, date_match.groups())
        year = datetime.date.today().year
        try:
            return datetime.date(year, month, day)
        except ValueError:
            return None
    return None

# --- Parsing ---
def parse_bet(line: str):
    line = line.strip()
    if not line:
        return None

    # Units (default 1)
    unit_match = re.search(r"(\d+(\.\d+)?)\s*u", line.lower())
    units = float(unit_match.group(1)) if unit_match else 1.0

    # Odds (optional)
    odds_match = re.search(r"([+-]\d+)", line)
    odds = odds_match.group(1) if odds_match else None

    # Result classification
    status, result = classify_line(line, units)
    if not status:
        return None

    return units, odds, status, result

async def log_bet(line_text, parsed, date, channel, guild_id):
    units, odds, status, result = parsed
    c.execute(
        "INSERT INTO bets (bet_text, units, odds, status, result, date, guild_id) VALUES (%s, %s, %s, %s, %s, %s, %s)",
        (line_text.strip(), units, odds, status, result, date, guild_id)
    )
    conn.commit()
    return f"{line_text.strip()} ({result:+}u)"

def get_record(guild_id, start_date=None, end_date=None):
    query = """
    SELECT COUNT(*), SUM(result),
           SUM(CASE WHEN status='win' THEN 1 ELSE 0 END),
           SUM(CASE WHEN status='loss' THEN 1 ELSE 0 END)
    FROM bets WHERE guild_id=%s
    """
    params = [guild_id]
    if start_date and end_date:
        query += " AND date BETWEEN %s AND %s"
        params.extend([start_date, end_date])
    c.execute(query, params)
    total, pnl, wins, losses = c.fetchone()
    pnl = pnl if pnl else 0
    return total or 0, wins or 0, losses or 0, pnl

def get_channel_id(guild_id):
    c.execute("SELECT channel_id FROM settings WHERE guild_id=%s", (guild_id,))
    row = c.fetchone()
    return row[0] if row else None

# --- Events ---
@client.event
async def on_ready():
    print(f"Logged in as {client.user}")

@client.event
async def on_message(message):
    if message.author.bot or not message.guild:
        return

    guild_id = message.guild.id
    channel_id = get_channel_id(guild_id)
    cmd = message.content.strip().lower()

    # --- Admin-only commands ---
    if cmd.startswith("!setchannel"):
        if not message.author.guild_permissions.administrator:
            await message.channel.send("🚫 Only administrators can set the recap channel.")
            return
        if message.channel_mentions:
            new_channel_id = message.channel_mentions[0].id
            c.execute(
                "INSERT INTO settings (guild_id, channel_id) VALUES (%s, %s) "
                "ON CONFLICT (guild_id) DO UPDATE SET channel_id = EXCLUDED.channel_id",
                (guild_id, new_channel_id)
            )
            conn.commit()
            await message.channel.send(f"✅ Recap channel set to <#{new_channel_id}>")
        return

    if cmd == "!backfill":
        if not message.author.guild_permissions.administrator:
            await message.channel.send("🚫 Only administrators can run backfill.")
            return
        if not channel_id:
            await message.channel.send("No recap channel set. Use !setchannel first.")
            return
        channel = client.get_channel(channel_id)
        count = 0
        async for msg in channel.history(limit=None, oldest_first=True):
            bet_date = extract_date_from_text(msg.content) or msg.created_at.date()
            for line in msg.content.splitlines():
                parsed = parse_bet(line)
                if parsed:
                    c.execute("SELECT 1 FROM bets WHERE bet_text=%s AND date=%s AND guild_id=%s",
                              (line.strip(), bet_date, guild_id))
                    if not c.fetchone():
                        await log_bet(line, parsed, bet_date, channel, guild_id)
                        count += 1
        await message.channel.send(f"Backfill complete. Logged {count} plays.")
        return
    # --- Public record commands ---
    if cmd == "!daily":
        today = datetime.date.today()
        total, wins, losses, pnl = get_record(guild_id, today, today)
        await message.channel.send(f"📅 Today: {wins}-{losses} ({total} plays), Net {pnl:+}u")
        return

    if cmd == "!mtd":
        today = datetime.date.today()
        start = today.replace(day=1)
        total, wins, losses, pnl = get_record(guild_id, start, today)
        await message.channel.send(f"📆 Month-to-date: {wins}-{losses} ({total} plays), Net {pnl:+}u")
        return

    if cmd == "!alltime":
        total, wins, losses, pnl = get_record(guild_id)
        await message.channel.send(f"🌍 All-time: {wins}-{losses} ({total} plays), Net {pnl:+}u")
        return

    # --- Bet logging (only in configured channel) ---
    if channel_id and message.channel.id == channel_id:
        bet_date = extract_date_from_text(message.content) or message.created_at.date()
        lines = message.content.splitlines()
        logged = []
        for line in lines:
            parsed = parse_bet(line)
            if parsed:
                logged_line = await log_bet(line, parsed, bet_date, message.channel, guild_id)
                logged.append(logged_line)

        if logged:
            total, wins, losses, pnl = get_record(guild_id, bet_date, bet_date)
            response = "Logged plays:\n" + "\n".join(logged)
            response += f"\n\n📅 {bet_date}: {wins}-{losses} ({total} plays), Net {pnl:+}u"
            await message.channel.send(response)

# --- Run ---
if __name__ == "__main__":
    if not TOKEN:
        raise RuntimeError("DISCORD_TOKEN not set in environment variables")
    client.run(TOKEN)
