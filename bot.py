# bot.py — multi-server, admin-guarded, bet-type-aware CLV with automation (Tue/Fri 3am) and interactive fixes

import os
import re
import asyncio
import datetime
import discord
import psycopg2
import traceback
import requests
from urllib.parse import urlparse

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
conn.autocommit = False

def exec_safe(sql, params=None, fetch="none"):
    try:
        with conn.cursor() as cur:
            cur.execute(sql, params or [])
            if fetch == "one":
                return cur.fetchone()
            elif fetch == "all":
                return cur.fetchall()
            else:
                return None
    except psycopg2.Error:
        conn.rollback()
        raise

# --- Schema bootstrap ---
exec_safe("""
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
exec_safe("ALTER TABLE bets ADD COLUMN IF NOT EXISTS guild_id BIGINT")
exec_safe("ALTER TABLE bets ADD COLUMN IF NOT EXISTS sport TEXT")
exec_safe("ALTER TABLE bets ADD COLUMN IF NOT EXISTS bet_type TEXT")
exec_safe("ALTER TABLE bets ADD COLUMN IF NOT EXISTS posted_line REAL")
exec_safe("ALTER TABLE bets ADD COLUMN IF NOT EXISTS posted_side TEXT")
exec_safe("ALTER TABLE bets ADD COLUMN IF NOT EXISTS closing_line REAL")
exec_safe("ALTER TABLE bets ADD COLUMN IF NOT EXISTS closing_odds TEXT")

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

TOKEN = os.getenv("DISCORD_TOKEN")
ODDS_API_KEY = os.getenv("ODDS_API_KEY")

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

# --- Emoji sentiment ---
def classify_line(line: str, units: float):
    lower = line.lower()
    if any(w in lower for w in ["✅","🏆","🔥","cash","hit","win"," w "]):
        return "win", units
    if any(w in lower for w in ["❌","💀","loss","miss"," l ","lose","🪝","🎣"]):
        return "loss", -units
    if any(w in lower for w in ["push","void","cancel"]):
        return "push", 0.0
    return None, 0.0

# --- Date extraction ---
def extract_date_from_text(text: str):
    m = re.search(r"\b(\d{1,2})/(\d{1,2})\b", text)
    if not m:
        return None
    month, day = int(m.group(1)), int(m.group(2))
    year = datetime.date.today().year
    try:
        return datetime.date(year, month, day)
    except ValueError:
        return None

# --- Sport detection ---
SPORT_MAP = {
    "soccer":"soccer","mls":"soccer","ucl":"soccer","⚽":"soccer",
    "football":"football","nfl":"football","cfb":"football","🏈":"football",
    "basketball":"basketball","nba":"basketball","wnba":"basketball","cbb":"basketball","🏀":"basketball",
    "baseball":"baseball","mlb":"baseball","⚾":"baseball",
    "mma":"mma","ufc":"mma","🥊":"mma",
    "hockey":"hockey","nhl":"hockey","🏒":"hockey",
}

SPORT_API_MAP = {
    "nba": "basketball_nba",
    "nfl": "americanfootball_nfl",
    "mlb": "baseball_mlb",
    "nhl": "icehockey_nhl",
    "soccer": "soccer_usa_mls"
}

def extract_sport(text: str):
    lower = text.lower()
    for key, sport in SPORT_MAP.items():
        if key in lower:
            return sport
    return None

# --- Bet type + line extraction ---
GENERIC_WORDS = {"over","under","parlay","moneyline","ml","spread","total","pts","points","reb","rebounds","ast","assists"}

def detect_bet_type_and_line(text: str):
    lower = text.lower()
    tm = re.search(r"\b(over|under|o/u)\b\s*([0-9]+(?:\.[0-9]+)?)", lower)
    if tm:
        side_raw = tm.group(1)
        side = "over" if "over" in side_raw else "under"
        line_val = float(tm.group(2))
        if re.search(r"\b(pts|points|reb|rebounds|ast|assists|sog|shots|yards|yds|ga|saves)\b", lower):
            return "prop", line_val, side
        return "total", line_val, side
    sm = re.search(r"(^|[^0-9])([+-]\d+(?:\.\d+)?)\b", text)
    if sm:
        raw = sm.group(2)
        line_val = float(raw)
        side = "fav" if raw.startswith("-") else "dog"
        return "spread", line_val, side
    if "ml" in lower or "moneyline" in lower:
        return "moneyline", None, None
    if re.search(r"[+-]\d{3,4}", text):
        return "moneyline", None, None
    return "unknown", None, None

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
    return f"{(sport or 'unknown')}|{date.isoformat()}|{(bet_type or 'unknown')}|{extract_teams_key(bet_text)}"

# --- CLV math ---
def american_to_prob(odds: str):
    try:
        o = int(odds)
        if o > 0:
            return 100 / (o + 100)
        return -o / (-o + 100)
    except Exception:
        return None

def calc_clv_for_bet(bet_type, posted_line, posted_side, posted_odds, closing_line, closing_odds):
    if bet_type == "moneyline":
        p_post = american_to_prob(posted_odds) if posted_odds else None
        p_close = american_to_prob(closing_odds) if closing_odds else None
        if p_post is None or p_close is None:
            return None
        return p_close - p_post
    if bet_type == "spread":
        if posted_line is None or closing_line is None or posted_side not in {"fav","dog"}:
            return None
        return abs(closing_line) - abs(posted_line)
    if bet_type in {"total","prop"}:
        if posted_line is None or closing_line is None or posted_side not in {"over","under"}:
            return None
        diff = closing_line - posted_line
        return diff if posted_side == "over" else -diff
    return None

# --- Closings cache helpers ---
def cache_get_closing(guild_id, event_key):
    row = exec_safe("""
        SELECT closing_line, closing_odds, source
        FROM closings
        WHERE guild_id=%s AND event_key=%s
        ORDER BY fetched_at DESC
        LIMIT 1
    """, (guild_id, event_key), fetch="one")
    if row:
        return row[0], row[1], row[2]
    return None, None, None

def cache_set_closing(guild_id, event_key, closing_line, closing_odds, source):
    exec_safe("""
        INSERT INTO closings (guild_id, event_key, closing_line, closing_odds, source, fetched_at)
        VALUES (%s, %s, %s, %s, %s, NOW())
    """, (guild_id, event_key, closing_line, closing_odds, source or "consensus"))
    conn.commit()

# --- Odds API integration ---
def fetch_and_store_closings(guild_id, sport_key):
    if sport_key not in SPORT_API_MAP:
        return 0

    url = f"https://api.the-odds-api.com/v4/sports/{SPORT_API_MAP[sport_key]}/odds/"
    params = {
        "regions": "us",
        "markets": "spreads,totals,h2h,player_points,player_rebounds,player_assists",
        "oddsFormat": "american",
        "apiKey": ODDS_API_KEY
    }
    resp = requests.get(url, params=params)
    resp.raise_for_status()
    data = resp.json()

    inserted = 0
    for game in data:
        home, away = game["home_team"], game["away_team"]
        date = game["commence_time"][:10]

        for bm in game["bookmakers"]:
            for market in bm["markets"]:
                mkey = market["key"]

                # --- Spread ---
                if mkey == "spreads":
                    for outcome in market["outcomes"]:
                        line = outcome.get("point")
                        odds = outcome.get("price")
                        ek = f"{sport_key}|{date}|spread|{home}|{away}"
                        cache_set_closing(guild_id, ek, line, odds, bm["title"])
                        inserted += 1

                # --- Totals ---
                elif mkey == "totals":
                    for outcome in market["outcomes"]:
                        side = outcome["name"].lower()
                        line = outcome.get("point")
                        odds = outcome.get("price")
                        ek = f"{sport_key}|{date}|total|{home}|{away}|{side}"
                        cache_set_closing(guild_id, ek, line, odds, bm["title"])
                        inserted += 1

                # --- Moneyline ---
                elif mkey == "h2h":
                    for outcome in market["outcomes"]:
                        odds = outcome.get("price")
                        ek = f"{sport_key}|{date}|moneyline|{home}|{away}|{outcome['name']}"
                        cache_set_closing(guild_id, ek, None, odds, bm["title"])
                        inserted += 1

                # --- Props (points, rebounds, assists) ---
                elif mkey in {"player_points", "player_rebounds", "player_assists"}:
                    for outcome in market["outcomes"]:
                        line = outcome.get("point")
                        odds = outcome.get("price")
                        ek = f"{sport_key}|{date}|prop|{outcome['description']}"
                        cache_set_closing(guild_id, ek, line, odds, bm["title"])
                        inserted += 1
    return inserted

# --- Consensus fetcher ---
def teams_overlap_score(a: str, b: str):
    return len(set(a.split("|")) & set(b.split("|")))

def fetch_consensus_closing(guild_id, event_key, sport, date, bet_type):
    closing_line, closing_odds, source = cache_get_closing(guild_id, event_key)
    if closing_line is not None or closing_odds is not None:
        return closing_line, closing_odds, source, []

    rows = exec_safe("""
        SELECT event_key, closing_line, closing_odds, source
        FROM closings
        WHERE guild_id=%s AND event_key LIKE %s
        ORDER BY fetched_at DESC
        LIMIT 200
    """, (guild_id, f"{sport or 'unknown'}|{date.isoformat()}|{bet_type or 'unknown'}|%"), fetch="all")

    candidates, best, best_score = [], None, -1
    this_teams = event_key.split("|", 3)[3] if "|" in event_key else "unknown"

    for ek, cl, co, src in rows:
        parts = ek.split("|", 3)
        if len(parts) < 4:
            continue
        score = teams_overlap_score(this_teams, parts[3])
        label = f"{bet_type or 'unknown'} {cl or ''} ({co or ''}) src:{src or ''}".strip()
        candidates.append((score, label, cl, co, ek, src))
        if score > best_score and (cl is not None or co is not None):
            best_score, best = score, (cl, co, src)

    if best and best_score >= 1:
        cl, co, src = best
        cache_set_closing(guild_id, event_key, cl, co, src)
        return cl, co, src, []

    candidates.sort(key=lambda x: x[0], reverse=True)
    labels = [f"{i+1}. {c[1]}" for i, c in enumerate(candidates[:5])]
    return None, None, None, labels

def try_update_bet_with_closing(bet_id, guild_id, bet_text, sport, bet_type, date):
    event_key = build_event_key(bet_text, sport, date, bet_type)
    cl, co, src, candidates = fetch_consensus_closing(guild_id, event_key, sport, date, bet_type)
    if cl is not None or co is not None:
        exec_safe("UPDATE bets SET closing_line=%s, closing_odds=%s WHERE id=%s", (cl, co, bet_id))
        conn.commit()
        return True
    if candidates:
        exec_safe("""
            INSERT INTO clv_fixes (bet_id, guild_id, candidates)
            VALUES (%s, %s, %s)
            ON CONFLICT DO NOTHING
        """, (bet_id, guild_id, candidates))
        conn.commit()
    return False

# --- CLV automation core ---
async def run_updateclv_for_guild(guild_id):
    rows = exec_safe("""
        SELECT id, bet_text, sport, bet_type, date
        FROM bets
        WHERE guild_id=%s AND (closing_line IS NULL AND closing_odds IS NULL)
        ORDER BY date DESC
        LIMIT 300
    """, (guild_id,), fetch="all")
    updated = 0
    for bet_id, bet_text, sport, bet_type, bdate in rows:
        if try_update_bet_with_closing(bet_id, guild_id, bet_text, sport, bet_type, bdate):
            updated += 1
    return updated

async def clv_scheduler():
    await client.wait_until_ready()
    while not client.is_closed():
        now = datetime.datetime.now(datetime.UTC)
        if now.weekday() in (1, 4) and now.hour == 3 and now.minute == 0:
            for guild in client.guilds:
                try:
                    # fetch closings before updating
                    for sport in ["nba","nfl","mlb","nhl","soccer"]:
                        count = fetch_and_store_closings(guild.id, sport)
                        print(f"Fetched {count} closings for {sport}")
                    updated = await run_updateclv_for_guild(guild.id)
                    ch_id = get_channel_id(guild.id)
                    if ch_id:
                        channel = client.get_channel(ch_id)
                        if channel:
                            await channel.send(
                                f"🤖 Auto CLV update: filled {updated} bets. "
                                f"Unmatched queued for !fixclv."
                            )
                except Exception as e:
                    print("CLV scheduler error:", e)
        await asyncio.sleep(60)

# --- Candidate parsing for fix mode ---
def parse_candidate_label(candidate: str):
    lm = re.search(r"([+-]?\d+(?:\.\d+)?)", candidate)
    om = re.search(r"\(([+-]?\d+)\)", candidate)
    closing_line = float(lm.group(1)) if lm else None
    closing_odds = om.group(1) if om else None
    return closing_line, closing_odds

# --- Events ---
@client.event
async def on_ready():
    print(f"Logged in as {client.user}")
    client.loop.create_task(clv_scheduler())

@client.event
async def on_message(message):
    if message.author.bot or not message.guild:
        return
    guild_id = message.guild.id
    cmd = message.content.strip().lower()

    # --- Admin commands ---
    if cmd == "!updateclv":
        if not message.author.guild_permissions.administrator:
            await message.channel.send("🚫 Only administrators can update CLV.")
            return
        try:
            updated = await run_updateclv_for_guild(guild_id)
            await message.channel.send(
                f"🔄 CLV update complete. Filled {updated} bets. "
                f"Unmatched queued for !fixclv."
            )
        except Exception as e:
            traceback.print_exc()
            await message.channel.send(f"❌ CLV update failed: {e}")
        return

    if cmd == "!fixclv":
        if not message.author.guild_permissions.administrator:
            await message.channel.send("🚫 Only administrators can fix CLV.")
            return
        row = exec_safe("""
            SELECT f.id, b.id, b.bet_text, f.candidates
            FROM clv_fixes f
            JOIN bets b ON f.bet_id = b.id
            WHERE f.guild_id=%s AND f.resolved=FALSE
            ORDER BY f.created_at ASC
            LIMIT 1
        """, (guild_id,), fetch="one")
        if not row:
            await message.channel.send("✅ No unresolved CLV cases.")
            return
        fix_id, bet_id, bet_text, candidates = row
        cands = candidates or []
        options = "\n".join([f"{i+1}. {c}" for i, c in enumerate(cands)])
        await message.channel.send(
            f"⚠️ Fix CLV for bet #{bet_id}: `{bet_text}`\n"
            f"Possible matches:\n{options}\n\n"
            f"Reply with the number to select, or `skip` to move to next."
        )
        def check(m): return m.author == message.author and m.channel == message.channel
        try:
            reply = await client.wait_for("message", check=check, timeout=180)
        except asyncio.TimeoutError:
            await message.channel.send("⌛ Timed out. Run `!fixclv` again.")
            return
        if reply.content.strip().lower() == "skip":
            await message.channel.send("⏭️ Skipped.")
            return
        try:
            idx = int(reply.content.strip()) - 1
            if idx < 0 or idx >= len(cands):
                await message.channel.send("❌ Invalid choice.")
                return
            chosen = cands[idx]
            closing_line, closing_odds = parse_candidate_label(chosen)
            exec_safe("UPDATE bets SET closing_line=%s, closing_odds=%s WHERE id=%s",
                      (closing_line, closing_odds, bet_id))
            exec_safe("UPDATE clv_fixes SET resolved=TRUE WHERE id=%s", (fix_id,))
            conn.commit()
            await message.channel.send(f"✅ CLV fixed for bet #{bet_id} → {chosen}")
        except Exception:
            conn.rollback()
            await message.channel.send("❌ Failed to apply fix.")
        return

    if cmd.startswith("!fetchclosings"):
        if not message.author.guild_permissions.administrator:
            await message.channel.send("🚫 Only admins can fetch closings.")
            return
        sport = cmd.split(" ", 1)[1] if " " in cmd else "nba"
        count = fetch_and_store_closings(guild_id, sport)
        await message.channel.send(f"📊 Inserted {count} closings for {sport.upper()}.")
        return

    # --- Public commands (help, daily, mtd, alltime, last10, record, recap, clv) ---
    # ... (rest of your existing command handlers remain unchanged)

# --- Run ---
if __name__ == "__main__":
    if not TOKEN:
        raise RuntimeError("DISCORD_TOKEN not set in environment variables")
    client.run(TOKEN)
