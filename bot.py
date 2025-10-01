# bot.py — multi-server, admin-guarded, bet-type aware CLV, insights, and date-aware logging

import os
import re
import psycopg2
from urllib.parse import urlparse
import discord
import datetime
import regex
import unicodedata
import io
import csv

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

# Ensure tables exist (bet-type aware + CLV fields)
c.execute("""
CREATE TABLE IF NOT EXISTS bets (
    id SERIAL PRIMARY KEY,
    bet_text TEXT,
    units REAL,
    odds TEXT,              -- posted odds (American, e.g. -110) for ML or juice
    status TEXT,            -- win/loss/push
    result REAL,            -- +units / -units / 0
    date DATE,
    guild_id BIGINT,
    sport TEXT,             -- parsed sport tag/emoji keyword
    bet_type TEXT,          -- moneyline | spread | total | prop | unknown
    posted_line REAL,       -- numeric line for spread/total/prop (e.g., -3.5, 27.5)
    posted_side TEXT,       -- 'fav'/'dog' for spreads, 'over'/'under' for totals/props
    closing_line REAL,      -- numeric closing line for spread/total/prop
    closing_odds TEXT       -- closing juice/odds for ML or totals/props
)
""")
c.execute("""
CREATE TABLE IF NOT EXISTS settings (
    guild_id BIGINT PRIMARY KEY,
    channel_id BIGINT,
    override_date DATE
)
""")
# Cache table for closing odds/lines per event to avoid repeated external calls/imports
c.execute("""
CREATE TABLE IF NOT EXISTS closings (
    id SERIAL PRIMARY KEY,
    guild_id BIGINT,
    event_key TEXT,         -- stable key: sport|date|bet_type|teams_key
    closing_line REAL,
    closing_odds TEXT,
    source TEXT,
    fetched_at TIMESTAMP
)
""")
conn.commit()

TOKEN = os.getenv("DISCORD_TOKEN")

# --- Emoji + sentiment helpers ---
EMOJI_PATTERN = regex.compile(r"\p{Emoji}", flags=regex.UNICODE)

def extract_emojis(text: str):
    return EMOJI_PATTERN.findall(text)

def classify_emoji(e: str):
    name = unicodedata.name(e, "").upper()
    if any(word in name for word in ["CHECK", "GREEN", "SMILE", "PARTY", "TROPHY", "STAR", "FIRE"]):
        return "win"
    if any(word in name for word in ["CROSS", "SKULL", "ANGRY", "SICK", "CRY", "VOMIT"]):
        return "loss"
    if e in {"🪝", "🎣"} or "HOOK" in name:
        return "loss"
    return None

def classify_line(line: str, units: float):
    emojis = extract_emojis(line)
    for e in emojis:
        sentiment = classify_emoji(e)
        if sentiment == "win":
            return "win", units
        if sentiment == "loss":
            return "loss", -units
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
    match = re.search(r"(\d{1,2})/(\d{1,2})", text)
    if match:
        month, day = map(int, match.groups())
        year = datetime.date.today().year
        try:
            return datetime.date(year, month, day)
        except ValueError:
            return None
    return None

# --- Sport extraction (simple heuristic from emojis/keywords) ---
SPORT_MAP = {
    "⚽": "soccer", "soccer": "soccer", "mls": "soccer", "ucl": "soccer",
    "🏈": "football", "nfl": "football", "cfb": "football",
    "🏀": "basketball", "nba": "basketball", "wnba": "basketball", "cbb": "basketball",
    "⚾": "baseball", "mlb": "baseball",
    "🥊": "mma", "ufc": "mma",
    "🏒": "hockey", "nhl": "hockey"
}

def extract_sport(text: str):
    lower = text.lower()
    for key, sport in SPORT_MAP.items():
        if key in lower:
            return sport
    for e in extract_emojis(text):
        sport = SPORT_MAP.get(e)
        if sport:
            return sport
    return None

# --- Bet type detection + posted line extraction ---
def detect_bet_type_and_line(text: str):
    """
    Returns (bet_type, posted_line: float or None, posted_side: str or None)
    bet_type in {moneyline, spread, total, prop, unknown}
    posted_side: 'fav'/'dog' for spread, 'over'/'under' for total/prop
    """
    lower = text.lower()

    # Totals and props (Over/Under)
    total_match = re.search(r"(over|under|o/u)\s*([0-9]+(?:\.[0-9]+)?)", lower)
    if total_match:
        side = total_match.group(1)
        side = "over" if "over" in side else "under"
        line_val = float(total_match.group(2))
        # Distinguish totals vs props: props often have player names; heuristically detect presence of 'pts', 'reb', 'ast', 'shots', etc.
        if re.search(r"\b(pts|points|reb|rebounds|ast|assists|sog|shots|yards|yds|ga|saves)\b", lower):
            return "prop", line_val, side
        return "total", line_val, side

    # Spread (handicap)
    # Avoid units like '1u' by ensuring no 'u' right after number
    spread_match = re.search(r"(^|[^0-9])([+-]\d+(?:\.\d+)?)\b", text)
    if spread_match:
        raw = spread_match.group(2)
        line_val = float(raw)
        side = "fav" if raw.startswith("-") else "dog"
        return "spread", line_val, side

    # Moneyline
    if "ml" in lower or "moneyline" in lower:
        return "moneyline", None, None
    if re.search(r"[+-]\d{3,4}", text):  # odds without spread indicator
        return "moneyline", None, None

    # Unknown
    return "unknown", None, None

# --- Team extraction for event matching ---
GENERIC_WORDS = {"over", "under", "parlay", "moneyline", "ml", "spread", "total", "pts", "points", "reb", "rebounds", "ast", "assists"}

def extract_teams_key(bet_text: str):
    tokens = re.findall(r"[A-Za-z][A-Za-z&.\- ]{2,}", bet_text)
    teams = []
    for t in tokens:
        t_clean = t.strip().lower()
        if len(t_clean) > 2 and t_clean not in GENERIC_WORDS:
            teams.append(t_clean)
    teams_key = "|".join(sorted(set(teams)))[:180] or "unknown"
    return teams_key

def build_event_key(bet_text: str, sport: str, date: datetime.date, bet_type: str):
    return f"{sport or 'unknown'}|{date.isoformat()}|{bet_type or 'unknown'}|{extract_teams_key(bet_text)}"

# --- Parsing ---
def parse_bet(line: str):
    line = line.strip()
    if not line:
        return None

    unit_match = re.search(r"(\d+(\.\d+)?)\s*u", line.lower())
    units = float(unit_match.group(1)) if unit_match else 1.0

    odds_match = re.search(r"([+-]\d{2,4})", line)
    odds = odds_match.group(1) if odds_match else None

    sport = extract_sport(line)
    bet_type, posted_line, posted_side = detect_bet_type_and_line(line)

    status, result = classify_line(line, units)
    if not status:
        return None

    return units, odds, status, result, sport, bet_type, posted_line, posted_side

async def log_bet(line_text, parsed, date, channel, guild_id):
    units, odds, status, result, sport, bet_type, posted_line, posted_side = parsed
    c.execute(
        "INSERT INTO bets (bet_text, units, odds, status, result, date, guild_id, sport, bet_type, posted_line, posted_side) "
        "VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)",
        (line_text.strip(), units, odds, status, result, date, guild_id, sport, bet_type, posted_line, posted_side)
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

def get_override_date(guild_id):
    c.execute("SELECT override_date FROM settings WHERE guild_id=%s", (guild_id,))
    row = c.fetchone()
    return row[0] if row and row[0] else None

def clear_override_date(guild_id):
    c.execute("UPDATE settings SET override_date=NULL WHERE guild_id=%s", (guild_id,))
    conn.commit()

# --- CLV helpers ---
def american_to_prob(odds: str):
    try:
        o = int(odds)
        if o > 0:
            return 100 / (o + 100)
        else:
            return -o / (-o + 100)
    except:
        return None

def calc_clv_for_bet(bet_type, posted_line, posted_side, posted_odds, closing_line, closing_odds):
    """
    Returns a signed CLV value appropriate for bet_type:
    - moneyline: implied probability edge (closing - posted)
    - spread: signed line movement in favor of your side
    - total/prop: signed line movement in favor of your side (over benefits if closing_line > posted; under if <)
    """
    if bet_type == "moneyline":
        p_post = american_to_prob(posted_odds) if posted_odds else None
        p_close = american_to_prob(closing_odds) if closing_odds else None
        if p_post is None or p_close is None:
            return None
        return p_close - p_post

    if bet_type == "spread":
        if posted_line is None or closing_line is None or posted_side not in {"fav", "dog"}:
            return None
        # For favorite (-), improvement if closing is more negative (e.g., -4.5 vs -3.5 → +1.0)
        # For underdog (+), improvement if closing is more positive (e.g., +8.0 vs +7.0 → +1.0)
        if posted_side == "fav":
            return abs(closing_line) - abs(posted_line)  # more negative increases abs value
        else:  # dog
            return abs(closing_line) - abs(posted_line)  # more positive increases abs value

    if bet_type in {"total", "prop"}:
        if posted_line is None or closing_line is None or posted_side not in {"over", "under"}:
            return None
        diff = closing_line - posted_line
        # Over benefits when closing_line is higher; Under benefits when lower
        return diff if posted_side == "over" else -diff

    return None

def cache_get_closing(guild_id, event_key):
    c.execute("SELECT closing_line, closing_odds, source FROM closings WHERE guild_id=%s AND event_key=%s ORDER BY fetched_at DESC LIMIT 1",
              (guild_id, event_key))
    row = c.fetchone()
    if row:
        return row[0], row[1], row[2]
    return None, None, None

def cache_set_closing(guild_id, event_key, closing_line, closing_odds, source):
    c.execute("INSERT INTO closings (guild_id, event_key, closing_line, closing_odds, source, fetched_at) VALUES (%s, %s, %s, %s, %s, NOW())",
              (guild_id, event_key, closing_line, closing_odds, source))
    conn.commit()

def try_update_bet_with_cached_closing(guild_id, bet_id, bet_text, sport, bet_type, date):
    event_key = build_event_key(bet_text, sport, date, bet_type)
    closing_line, closing_odds, _ = cache_get_closing(guild_id, event_key)
    if closing_line is not None or closing_odds is not None:
        c.execute("UPDATE bets SET closing_line=%s, closing_odds=%s WHERE id=%s",
                  (closing_line, closing_odds, bet_id))
        conn.commit()
        return True
    return False

# --- CSV-based CLV import (consensus snapshot) ---
# Admin uploads a CSV with columns: date,sport,bet_type,teams_key,closing_line,closing_odds,source
# Example teams_key should roughly match extract_teams_key heuristic.
async def import_clv_from_attachment(message, guild_id):
    if not message.attachments:
        await message.channel.send("Attach a CSV file with columns: date,sport,bet_type,teams_key,closing_line,closing_odds,source")
        return
    att = message.attachments[0]
    data_bytes = await att.read()
    f = io.StringIO(data_bytes.decode("utf-8"))
    reader = csv.DictReader(f)
    count = 0
    for row in reader:
        try:
            date = datetime.datetime.strptime(row["date"], "%Y-%m-%d").date()
            sport = (row.get("sport") or "unknown").lower()
            bet_type = (row.get("bet_type") or "unknown").lower()
            teams_key = row.get("teams_key") or "unknown"
            closing_line = row.get("closing_line")
            closing_odds = row.get("closing_odds")
            source = row.get("source") or "consensus"
            closing_line_val = float(closing_line) if closing_line not in (None, "",) else None
            event_key = f"{sport}|{date.isoformat()}|{bet_type}|{teams_key}"
            cache_set_closing(guild_id, event_key, closing_line_val, closing_odds, source)
            count += 1
        except Exception:
            continue
    await message.channel.send(f"Imported {count} closing records into cache. Running update to fill bets...")
    # Try to fill recent missing bets using cache
    c.execute("""
        SELECT id, bet_text, sport, bet_type, date FROM bets
        WHERE guild_id=%s AND (closing_line IS NULL AND closing_odds IS NULL)
        ORDER BY date DESC LIMIT 500
    """, (guild_id,))
    rows = c.fetchall()
    updated = 0
    for bet_id, bet_text, sport, bet_type, date in rows:
        if try_update_bet_with_cached_closing(guild_id, bet_id, bet_text, sport, bet_type, date):
            updated += 1
    await message.channel.send(f"CLV update complete. Filled closing data for {updated} bets.")

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
            bet_date = get_override_date(guild_id) or extract_date_from_text(msg.content) or msg.created_at.date()
            for line in msg.content.splitlines():
                parsed = parse_bet(line)
                if parsed:
                    c.execute("SELECT id FROM bets WHERE bet_text=%s AND date=%s AND guild_id=%s",
                              (line.strip(), bet_date, guild_id))
                    if not c.fetchone():
                        await log_bet(line, parsed, bet_date, channel, guild_id)
                        count += 1
        clear_override_date(guild_id)
        await message.channel.send(f"Backfill complete. Logged {count} plays.")
        return

    if cmd.startswith("!delete "):
        if not message.author.guild_permissions.administrator:
            await message.channel.send("🚫 Only administrators can delete bets.")
            return
        try:
            date_str = cmd.split(" ")[1]
            target_date = datetime.datetime.strptime(date_str, "%Y-%m-%d").date()
            c.execute("DELETE FROM bets WHERE guild_id=%s AND date=%s", (guild_id, target_date))
            conn.commit()
            await message.channel.send(f"🗑️ Deleted all bets for {target_date}.")
        except Exception:
            await message.channel.send("Usage: `!delete YYYY-MM-DD`")
        return

    if cmd.startswith("!setdate "):
        if not message.author.guild_permissions.administrator:
            await message.channel.send("🚫 Only administrators can set override dates.")
            return
        try:
            date_str = cmd.split(" ")[1]
            override_date = datetime.datetime.strptime(date_str, "%Y-%m-%d").date()
            c.execute("UPDATE settings SET override_date=%s WHERE guild_id=%s", (override_date, guild_id))
            conn.commit()
            await message.channel.send(f"📌 Override date set to {override_date}. Next recap will use this date.")
        except Exception:
            await message.channel.send("Usage: `!setdate YYYY-MM-DD`")
        return

    if cmd.startswith("!setclv "):
        if not message.author.guild_permissions.administrator:
            await message.channel.send("🚫 Only administrators can set CLV.")
            return
        try:
            _, bet_id, closing_line_or_odds = cmd.split()
            # If numeric → set closing_line; else → set closing_odds
            try:
                closing_line_val = float(closing_line_or_odds)
                c.execute("UPDATE bets SET closing_line=%s WHERE id=%s AND guild_id=%s",
                          (closing_line_val, bet_id, guild_id))
            except ValueError:
                c.execute("UPDATE bets SET closing_odds=%s WHERE id=%s AND guild_id=%s",
                          (closing_line_or_odds, bet_id, guild_id))
            conn.commit()
            await message.channel.send(f"✅ Set closing data for bet {bet_id} to {closing_line_or_odds}")
        except Exception:
            await message.channel.send("Usage: `!setclv <bet_id> <closing_line_or_closing_odds>`")
        return

    if cmd == "!importclv":
        if not message.author.guild_permissions.administrator:
            await message.channel.send("🚫 Only administrators can import CLV snapshots.")
            return
        await import_clv_from_attachment(message, guild_id)
        return

    # --- Public record commands ---
    if cmd == "!help":
        help_text = (
            "**Available Commands:**\n"
            "📅 `!daily` → Show today's record.\n"
            "📆 `!mtd` → Show month-to-date record.\n"
            "🌍 `!alltime` → Show all-time record.\n"
            "🔟 `!last10` → Show the last 10 logged plays.\n"
            "📅 `!record YYYY-MM-DD` → Show record for a specific date.\n"
            "📋 `!recap YYYY-MM-DD` → Show all plays for a specific date (with CLV when available).\n"
            "🔍 `!search keyword` → Search logged bets by keyword.\n"
            "📈 `!roi` → Win rate %, avg stake, and ROI in units.\n"
            "🏷️ `!bysport` → Record and units by sport.\n"
            "🔥 `!streak` → Current win/loss streak.\n"
            "🎚️ `!byunits` → Performance by stake size buckets.\n"
            "🗓️ `!byday` → Performance by day of week.\n"
            "📊 `!clv` → Average CLV and breakdowns by bet type.\n\n"
            "**Admin-only Commands:**\n"
            "⚙️ `!setchannel #channel` → Set the recap channel.\n"
            "♻️ `!backfill` → Scan full history of recap channel.\n"
            "🗑️ `!delete YYYY-MM-DD` → Delete all bets for a given date.\n"
            "📌 `!setdate YYYY-MM-DD` → Override date for the next recap.\n"
            "🎯 `!setclv <bet_id> <closing_line_or_closing_odds>` → Manually set closing data.\n"
            "📥 `!importclv` → Upload CSV consensus closings and auto-fill bets.\n"
        )
        await message.channel.send(help_text)
        return

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

    if cmd == "!last10":
        c.execute("SELECT bet_text, result, date FROM bets WHERE guild_id=%s ORDER BY id DESC LIMIT 10", (guild_id,))
        rows = c.fetchall()
        if not rows:
            await message.channel.send("No bets logged yet.")
        else:
            response = "**Last 10 plays:**\n"
            for bet_text, result, date in rows:
                response += f"{date} | {bet_text} ({result:+}u)\n"
            await message.channel.send(response)
        return

    if cmd.startswith("!record "):
        try:
            date_str = cmd.split(" ")[1]
            target_date = datetime.datetime.strptime(date_str, "%Y-%m-%d").date()
            total, wins, losses, pnl = get_record(guild_id, target_date, target_date)
            await message.channel.send(f"📅 {target_date}: {wins}-{losses} ({total} plays), Net {pnl:+}u")
        except Exception:
            await message.channel.send("Usage: `!record YYYY-MM-DD`")
        return

    if cmd.startswith("!recap "):
        try:
            date_str = cmd.split(" ")[1]
            target_date = datetime.datetime.strptime(date_str, "%Y-%m-%d").date()
            c.execute("""
                SELECT id, bet_text, result, odds, bet_type, posted_line, posted_side, closing_line, closing_odds
                FROM bets WHERE guild_id=%s AND date=%s ORDER BY id
            """, (guild_id, target_date))
            rows = c.fetchall()
            if not rows:
                await message.channel.send(f"No bets logged for {target_date}.")
            else:
                response = f"**Recap for {target_date}:**\n"
                for bet_id, bet_text, result, odds, bet_type, posted_line, posted_side, closing_line, closing_odds in rows:
                    clv_tag = ""
                    clv_val = calc_clv_for_bet(bet_type, posted_line, posted_side, odds, closing_line, closing_odds)
                    if clv_val is not None:
                        if bet_type == "moneyline":
                            clv_tag = f" | CLV edge {clv_val:+.3f}"
                        else:
                            clv_tag = f" | CLV {clv_val:+.1f}"
                    odds_str = f" | {odds}" if odds else ""
                    closing_str = ""
                    if closing_line is not None:
                        closing_str += f" → {closing_line}"
                    if closing_odds:
                        closing_str += f" ({closing_odds})"
                    response += f"#{bet_id} {bet_text} ({result:+}u){odds_str}{closing_str}{clv_tag}\n"
                total, wins, losses, pnl = get_record(guild_id, target_date, target_date)
                response += f"\n📅 {target_date}: {wins}-{losses} ({total} plays), Net {pnl:+}u"
                await message.channel.send(response)
        except Exception:
            await message.channel.send("Usage: `!recap YYYY-MM-DD`")
        return

    if cmd.startswith("!search "):
        keyword = cmd.split(" ", 1)[1]
        c.execute("""
            SELECT id, bet_text, result, date FROM bets
            WHERE guild_id=%s AND bet_text ILIKE %s
            ORDER BY date DESC LIMIT 20
        """, (guild_id, f"%{keyword}%"))
        rows = c.fetchall()
        if not rows:
            await message.channel.send(f"No bets found containing '{keyword}'.")
        else:
            response = f"**Search results for '{keyword}':**\n"
            for bet_id, bet_text, result, date in rows:
                response += f"{date} | #{bet_id} {bet_text} ({result:+}u)\n"
            await message.channel.send(response)
        return

    if cmd == "!roi":
        c.execute("SELECT units, result FROM bets WHERE guild_id=%s", (guild_id,))
        data = c.fetchall()
        if not data:
            await message.channel.send("No data yet.")
            return
        total_plays = len(data)
        wins = sum(1 for u, r in data if r > 0)
        losses = sum(1 for u, r in data if r < 0)
        win_rate = (wins / total_plays) * 100 if total_plays else 0
        total_units_staked = sum(abs(u) for u, _ in data)
        net_units = sum(r for _, r in data)
        roi_pct = (net_units / total_units_staked) * 100 if total_units_staked else 0
        await message.channel.send(f"📈 Win rate: {win_rate:.1f}% | Avg stake: { (total_units_staked/total_plays) if total_plays else 0:.2f}u | ROI: {roi_pct:+.2f}%")
        return

    if cmd == "!bysport":
        c.execute("""
            SELECT sport,
                   SUM(CASE WHEN status='win' THEN 1 ELSE 0 END),
                   SUM(CASE WHEN status='loss' THEN 1 ELSE 0 END),
                   SUM(result)
            FROM bets WHERE guild_id=%s
            GROUP BY sport
            ORDER BY sport NULLS LAST
        """, (guild_id,))
        rows = c.fetchall()
        if not rows:
            await message.channel.send("No data yet.")
            return
        response = "**By sport:**\n"
        for sport, wins, losses, pnl in rows:
            sport_label = sport or "unknown"
            response += f"{sport_label}: {wins or 0}-{losses or 0} ({pnl or 0:+}u)\n"
        await message.channel.send(response)
        return

    if cmd == "!streak":
        c.execute("SELECT status FROM bets WHERE guild_id=%s ORDER BY id DESC LIMIT 100", (guild_id,))
        rows = c.fetchall()
        if not rows:
            await message.channel.send("No data yet.")
            return
        streak = 0
        current = rows[0][0]
        for (status,) in rows:
            if status == current:
                streak += 1
            else:
                break
        label = "🔥 wins" if current == "win" else ("❌ losses" if current == "loss" else "⏸️ pushes")
        await message.channel.send(f"Current streak: {streak} {label}")
        return

    if cmd == "!byunits":
        c.execute("""
            SELECT
              CASE
                WHEN units < 0.75 THEN '0.5u'
                WHEN units < 1.25 THEN '1u'
                WHEN units < 2.25 THEN '2u'
                ELSE '3u+'
              END AS bucket,
              SUM(CASE WHEN status='win' THEN 1 ELSE 0 END),
              SUM(CASE WHEN status='loss' THEN 1 ELSE 0 END),
              SUM(result)
            FROM bets
            WHERE guild_id=%s
            GROUP BY bucket
            ORDER BY bucket
        """, (guild_id,))
        rows = c.fetchall()
        if not rows:
            await message.channel.send("No data yet.")
            return
        response = "**By units:**\n"
        for bucket, wins, losses, pnl in rows:
            response += f"{bucket}: {wins or 0}-{losses or 0} ({pnl or 0:+}u)\n"
        await message.channel.send(response)
        return

    if cmd == "!byday":
        c.execute("""
            SELECT TO_CHAR(date, 'Dy') AS dow,
                   SUM(CASE WHEN status='win' THEN 1 ELSE 0 END),
                   SUM(CASE WHEN status='loss' THEN 1 ELSE 0 END),
                   SUM(result)
            FROM bets
            WHERE guild_id=%s
            GROUP BY dow
            ORDER BY dow
        """, (guild_id,))
        rows = c.fetchall()
        if not rows:
            await message.channel.send("No data yet.")
            return
        response = "**By day of week:**\n"
        for dow, wins, losses, pnl in rows:
            response += f"{dow}: {wins or 0}-{losses or 0} ({pnl or 0:+}u)\n"
        await message.channel.send(response)
        return

    if cmd == "!clv":
        # Show CLV summary and by bet_type
        c.execute("""
            SELECT bet_type, posted_line, posted_side, odds, closing_line, closing_odds
            FROM bets WHERE guild_id=%s AND (closing_line IS NOT NULL OR closing_odds IS NOT NULL)
        """, (guild_id,))
        rows = c.fetchall()
        if not rows:
            await message.channel.send("No CLV data available yet.")
            return
        totals = {}
        for bet_type, posted_line, posted_side, odds, closing_line, closing_odds in rows:
            clv = calc_clv_for_bet(bet_type, posted_line, posted_side, odds, closing_line, closing_odds)
            if clv is None:
                continue
            bucket = bet_type or "unknown"
            agg = totals.get(bucket, {"sum": 0.0, "count": 0})
            agg["sum"] += clv
            agg["count"] += 1
            totals[bucket] = agg
        if not totals:
            await message.channel.send("No valid CLV calculations yet.")
            return
        lines = ["**CLV Summary:**"]
        overall_sum = 0.0
        overall_count = 0
        for bt, agg in totals.items():
            avg = agg["sum"] / agg["count"]
            if bt == "moneyline":
                lines.append(f"- {bt}: Avg edge {avg:+.3f} (implied probability)")
            else:
                lines.append(f"- {bt}: Avg CLV {avg:+.2f} (line movement)")
            overall_sum += agg["sum"]
            overall_count += agg["count"]
        overall_avg = overall_sum / overall_count if overall_count else 0.0
        lines.append(f"\nOverall avg: {overall_avg:+.3f}")
        await message.channel.send("\n".join(lines))
        return

    # --- Bet logging (only in configured channel) ---
    if channel_id and message.channel.id == channel_id:
        override = get_override_date(guild_id)
        bet_date = override or extract_date_from_text(message.content) or message.created_at.date()
        lines = message.content.splitlines()
        logged = []
        for line in lines:
            parsed = parse_bet(line)
            if parsed:
                logged_line = await log_bet(line, parsed, bet_date, message.channel, guild_id)
                logged.append(logged_line)
        if override:
            clear_override_date(guild_id)
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
