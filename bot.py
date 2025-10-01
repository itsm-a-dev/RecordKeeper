# bot.py — multi-server, admin-guarded, bet-type-aware CLV with automation (Tue/Fri 3am) and interactive fixes

import os
import re
import asyncio
import datetime
import discord
import psycopg2
import traceback
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
conn.autocommit = False  # we'll commit explicitly

def exec_safe(sql, params=None):
    """Execute SQL with rollback on errors to avoid poisoning the transaction."""
    try:
        with conn.cursor() as cur:
            cur.execute(sql, params or [])
            return cur
    except psycopg2.Error:
        conn.rollback()
        raise

# --- Schema bootstrap (create + alter if missing) ---
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

# --- Helpers: settings ---
def get_channel_id(guild_id):
    cur = exec_safe("SELECT channel_id FROM settings WHERE guild_id=%s", (guild_id,))
    row = cur.fetchone()
    return row[0] if row and row[0] else None

def get_override_date(guild_id):
    cur = exec_safe("SELECT override_date FROM settings WHERE guild_id=%s", (guild_id,))
    row = cur.fetchone()
    return row[0] if row and row[0] else None

def clear_override_date(guild_id):
    exec_safe("UPDATE settings SET override_date=NULL WHERE guild_id=%s", (guild_id,))
    conn.commit()

# --- Emoji sentiment (lightweight, using built-in re) ---
def classify_line(line: str, units: float):
    lower = line.lower()
    # Emoji keywords (fallback to text markers)
    if any(w in lower for w in ["✅", "🏆", "🔥", "cash", "hit", "win", " w "]):
        return "win", units
    if any(w in lower for w in ["❌", "💀", "loss", "miss", " l ", "lose", "🪝", "🎣"]):
        return "loss", -units
    if any(w in lower for w in ["push", "void", "cancel"]):
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
    "soccer": "soccer", "mls": "soccer", "ucl": "soccer", "⚽": "soccer",
    "football": "football", "nfl": "football", "cfb": "football", "🏈": "football",
    "basketball": "basketball", "nba": "basketball", "wnba": "basketball", "cbb": "basketball", "🏀": "basketball",
    "baseball": "baseball", "mlb": "baseball", "⚾": "baseball",
    "mma": "mma", "ufc": "mma", "🥊": "mma",
    "hockey": "hockey", "nhl": "hockey", "🏒": "hockey",
}

def extract_sport(text: str):
    lower = text.lower()
    for key, sport in SPORT_MAP.items():
        if key in lower:
            return sport
    return None

# --- Bet type + line extraction ---
GENERIC_WORDS = {
    "over", "under", "parlay", "moneyline", "ml", "spread", "total",
    "pts", "points", "reb", "rebounds", "ast", "assists"
}

def detect_bet_type_and_line(text: str):
    """
    Returns (bet_type, posted_line: float or None, posted_side: str or None)
    bet_type in {moneyline, spread, total, prop, unknown}
    posted_side: 'fav'/'dog' for spreads, 'over'/'under' for total/prop
    """
    lower = text.lower()

    # Totals/props
    tm = re.search(r"\b(over|under|o/u)\b\s*([0-9]+(?:\.[0-9]+)?)", lower)
    if tm:
        side_raw = tm.group(1)
        side = "over" if "over" in side_raw else "under"
        line_val = float(tm.group(2))
        if re.search(r"\b(pts|points|reb|rebounds|ast|assists|sog|shots|yards|yds|ga|saves)\b", lower):
            return "prop", line_val, side
        return "total", line_val, side

    # Spread (avoid units '1u')
    sm = re.search(r"(^|[^0-9])([+-]\d+(?:\.\d+)?)\b", text)
    if sm:
        raw = sm.group(2)
        line_val = float(raw)
        side = "fav" if raw.startswith("-") else "dog"
        return "spread", line_val, side

    # Moneyline
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
        if posted_line is None or closing_line is None or posted_side not in {"fav", "dog"}:
            return None
        return abs(closing_line) - abs(posted_line)
    if bet_type in {"total", "prop"}:
        if posted_line is None or closing_line is None or posted_side not in {"over", "under"}:
            return None
        diff = closing_line - posted_line
        return diff if posted_side == "over" else -diff
    return None

# --- Closings cache helpers ---
def cache_get_closing(guild_id, event_key):
    cur = exec_safe("""
        SELECT closing_line, closing_odds, source FROM closings
        WHERE guild_id=%s AND event_key=%s
        ORDER BY fetched_at DESC LIMIT 1
    """, (guild_id, event_key))
    row = cur.fetchone()
    if row:
        return row[0], row[1], row[2]
    return None, None, None

def cache_set_closing(guild_id, event_key, closing_line, closing_odds, source):
    exec_safe("""
        INSERT INTO closings (guild_id, event_key, closing_line, closing_odds, source, fetched_at)
        VALUES (%s, %s, %s, %s, %s, NOW())
    """, (guild_id, event_key, closing_line, closing_odds, source or "consensus"))
    conn.commit()

# --- Consensus fetcher (hands-off, cache-first, with candidates) ---
def teams_overlap_score(a: str, b: str):
    set_a = set(a.split("|"))
    set_b = set(b.split("|"))
    return len(set_a & set_b)

def fetch_consensus_closing(guild_id, event_key, sport, date, bet_type):
    """
    Try to find a confident closing match from cache; if not, return candidate suggestions.
    Returns: (closing_line, closing_odds, source, candidates:list[str])
    """
    # 1) Exact event_key in cache
    closing_line, closing_odds, source = cache_get_closing(guild_id, event_key)
    if closing_line is not None or closing_odds is not None:
        return closing_line, closing_odds, source, []

    # 2) Fuzzy candidates from same sport/date/bet_type
    # Pull recent closings and compute overlap scores
    cur = exec_safe("""
        SELECT event_key, closing_line, closing_odds, source
        FROM closings
        WHERE guild_id=%s AND event_key LIKE %s
        ORDER BY fetched_at DESC LIMIT 200
    """, (guild_id, f"{sport or 'unknown'}|{date.isoformat()}|{bet_type or 'unknown'}|%"))
    rows = cur.fetchall()

    # Rank by team-key overlap
    candidates = []
    ek_parts = event_key.split("|", 3)
    this_teams = ek_parts[3] if len(ek_parts) >= 4 else "unknown"
    best = None
    best_score = -1

    for ek, cl, co, src in rows:
        parts = ek.split("|", 3)
        if len(parts) < 4:
            continue
        other_teams = parts[3]
        score = teams_overlap_score(this_teams, other_teams)
        label = f"{bet_type or 'unknown'} {cl if cl is not None else ''} ({co or ''}) src:{src or ''}".strip()
        candidates.append((score, label, cl, co, ek, src))
        if score > best_score and (cl is not None or co is not None):
            best_score = score
            best = (cl, co, src)

    # 3) Confident threshold: at least 1 overlapping token
    if best and best_score >= 1:
        cl, co, src = best
        cache_set_closing(guild_id, event_key, cl, co, src)
        return cl, co, src, []

    # Return top 5 candidates (labels) for admin fix mode
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

# --- Record helpers ---
def get_record(guild_id, start_date=None, end_date=None):
    sql = """
    SELECT COUNT(*), SUM(result),
           SUM(CASE WHEN status='win' THEN 1 ELSE 0 END),
           SUM(CASE WHEN status='loss' THEN 1 ELSE 0 END)
    FROM bets WHERE guild_id=%s
    """
    params = [guild_id]
    if start_date and end_date:
        sql += " AND date BETWEEN %s AND %s"
        params.extend([start_date, end_date])
    cur = exec_safe(sql, tuple(params))
    total, pnl, wins, losses = cur.fetchone()
    pnl = pnl if pnl else 0
    return total or 0, wins or 0, losses or 0, pnl

# --- Parsing & logging ---
def parse_bet(line: str):
    line = line.strip()
    if not line:
        return None
    um = re.search(r"(\d+(\.\d+)?)\s*u\b", line.lower())
    units = float(um.group(1)) if um else 1.0
    om = re.search(r"([+-]\d{2,4})\b", line)
    odds = om.group(1) if om else None
    sport = extract_sport(line)
    bet_type, posted_line, posted_side = detect_bet_type_and_line(line)
    status, result = classify_line(line, units)
    if not status:
        return None
    return units, odds, status, result, sport, bet_type, posted_line, posted_side

async def log_bet(line_text, parsed, date, guild_id):
    units, odds, status, result, sport, bet_type, posted_line, posted_side = parsed
    exec_safe(
        "INSERT INTO bets (bet_text, units, odds, status, result, date, guild_id, sport, bet_type, posted_line, posted_side) "
        "VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)",
        (line_text.strip(), units, odds, status, result, date, guild_id, sport, bet_type, posted_line, posted_side)
    )
    conn.commit()
    return f"{line_text.strip()} ({result:+}u)"

# --- CLV automation core ---
async def run_updateclv_for_guild(guild_id):
    cur = exec_safe("""
        SELECT id, bet_text, sport, bet_type, date
        FROM bets
        WHERE guild_id=%s AND (closing_line IS NULL AND closing_odds IS NULL)
        ORDER BY date DESC
        LIMIT 300
    """, (guild_id,))
    rows = cur.fetchall()
    updated = 0
    for bet_id, bet_text, sport, bet_type, bdate in rows:
        if try_update_bet_with_closing(bet_id, guild_id, bet_text, sport, bet_type, bdate):
            updated += 1
    return updated

async def clv_scheduler():
    await client.wait_until_ready()
    while not client.is_closed():
        now = datetime.datetime.utcnow()
        # Tuesday=1, Friday=4 at 03:00 UTC
        if now.weekday() in (1, 4) and now.hour == 3 and now.minute == 0:
            for guild in client.guilds:
                try:
                    updated = await run_updateclv_for_guild(guild.id)
                    ch_id = get_channel_id(guild.id)
                    if ch_id:
                        channel = client.get_channel(ch_id)
                        if channel:
                            await channel.send(f"🤖 Auto CLV update: filled {updated} bets. Unmatched queued for !fixclv.")
                except Exception:
                    # keep scheduler alive on errors
                    continue
            await asyncio.sleep(60)  # prevent double-trigger within the minute
        await asyncio.sleep(30)

# --- Candidate parsing for fix mode ---
def parse_candidate_label(candidate: str):
    # candidate format: "<bet_type> <closing_line> (<closing_odds>) src:<source>"
    lm = re.search(r"([+-]?\d+(?:\.\d+)?)", candidate)
    om = re.search(r"\(([+-]?\d+)\)", candidate)
    closing_line = float(lm.group(1)) if lm else None
    closing_odds = om.group(1) if om else None
    return closing_line, closing_odds

# --- Events ---
@client.event
async def on_ready():
    print(f"Logged in as {client.user}")
    # start CLV scheduler
    client.loop.create_task(clv_scheduler())

@client.event
async def on_message(message):
    if message.author.bot or not message.guild:
        return

    guild_id = message.guild.id
    cmd = message.content.strip()

    # Admin-only: set channel
    if cmd.startswith("!setchannel"):
        if not message.author.guild_permissions.administrator:
            await message.channel.send("🚫 Only administrators can set the recap channel.")
            return
        if message.channel_mentions:
            new_channel_id = message.channel_mentions[0].id
            exec_safe("""
                INSERT INTO settings (guild_id, channel_id) VALUES (%s, %s)
                ON CONFLICT (guild_id) DO UPDATE SET channel_id = EXCLUDED.channel_id
            """, (guild_id, new_channel_id))
            conn.commit()
            await message.channel.send(f"✅ Recap channel set to <#{new_channel_id}>")
        return

    # Admin-only: override date
    if cmd.startswith("!setdate "):
        if not message.author.guild_permissions.administrator:
            await message.channel.send("🚫 Only administrators can set override dates.")
            return
        try:
            date_str = cmd.split(" ", 1)[1]
            override_date = datetime.datetime.strptime(date_str, "%Y-%m-%d").date()
            exec_safe("INSERT INTO settings (guild_id, override_date) VALUES (%s, %s) ON CONFLICT (guild_id) DO UPDATE SET override_date=%s",
                      (guild_id, override_date, override_date))
            conn.commit()
            await message.channel.send(f"📌 Override date set to {override_date}. Next recap will use this date.")
        except Exception:
            await message.channel.send("Usage: `!setdate YYYY-MM-DD`")
        return

    # Admin-only: manual update trigger (same routine as scheduler)
    if cmd.strip().lower() == "!updateclv":
        if not message.author.guild_permissions.administrator:
            await message.channel.send("🚫 Only administrators can update CLV.")
            return
        try:
            updated = await run_updateclv_for_guild(guild_id)
            await message.channel.send(f"🔄 CLV update complete. Filled {updated} bets. Unmatched queued for !fixclv.")
        except Exception as e:
            traceback.print_exc()
            await message.channel.send(f"❌ CLV update failed: {e}")

    # Admin-only: interactive fix mode
    if cmd.strip().lower() == "!fixclv":
        if not message.author.guild_permissions.administrator:
            await message.channel.send("🚫 Only administrators can fix CLV.")
            return

        # fetch one unresolved fix
        cur = exec_safe("""
            SELECT f.id, b.id, b.bet_text, f.candidates
            FROM clv_fixes f
            JOIN bets b ON f.bet_id = b.id
            WHERE f.guild_id=%s AND f.resolved=FALSE
            ORDER BY f.created_at ASC
            LIMIT 1
        """, (guild_id,))
        row = cur.fetchone()
        if not row:
            await message.channel.send("✅ No unresolved CLV cases.")
            return

        fix_id, bet_id, bet_text, candidates = row
        cands = candidates or []
        if not cands:
            await message.channel.send(f"⚠️ Bet #{bet_id} `{bet_text}` has no suggestions. Use `!setclv {bet_id} <line_or_odds>`.")
            return

        options = "\n".join([f"{i+1}. {c}" for i, c in enumerate(cands)])
        await message.channel.send(
            f"⚠️ Fix CLV for bet #{bet_id}: `{bet_text}`\n"
            f"Possible matches:\n{options}\n\n"
            f"Reply with the number to select, or `skip` to move to next."
        )

        def check(m):
            return m.author == message.author and m.channel == message.channel

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
            exec_safe("UPDATE bets SET closing_line=%s, closing_odds=%s WHERE id=%s", (closing_line, closing_odds, bet_id))
            exec_safe("UPDATE clv_fixes SET resolved=TRUE WHERE id=%s", (fix_id,))
            conn.commit()
            await message.channel.send(f"✅ CLV fixed for bet #{bet_id} → {chosen}")
        except Exception:
            conn.rollback()
            await message.channel.send("❌ Failed to apply fix.")
        return

    # Public commands
    if cmd.strip().lower() == "!help":
        await message.channel.send(
            "**Commands:**\n"
            "📅 `!daily` → Show today's record.\n"
            "📆 `!mtd` → Show month-to-date record.\n"
            "🌍 `!alltime` → Show all-time record.\n"
            "🔟 `!last10` → Show last 10 plays.\n"
            "📅 `!record YYYY-MM-DD` → Record for a date.\n"
            "📋 `!recap YYYY-MM-DD` → Plays for a date (with CLV when available).\n"
            "📊 `!clv` → Average CLV and breakdown by bet type.\n\n"
            "**Admin:**\n"
            "⚙️ `!setchannel #channel` → Set recap channel.\n"
            "📌 `!setdate YYYY-MM-DD` → Override recap date.\n"
            "🔄 `!updateclv` → Trigger CLV automation now.\n"
            "🛠️ `!fixclv` → Interactive fix for unmatched CLV."
        )
        return

    if cmd.strip().lower() == "!daily":
        today = datetime.date.today()
        total, wins, losses, pnl = get_record(guild_id, today, today)
        await message.channel.send(f"📅 Today: {wins}-{losses} ({total} plays), Net {pnl:+}u")
        return

    if cmd.strip().lower() == "!mtd":
        today = datetime.date.today()
        start = today.replace(day=1)
        total, wins, losses, pnl = get_record(guild_id, start, today)
        await message.channel.send(f"📆 Month-to-date: {wins}-{losses} ({total} plays), Net {pnl:+}u")
        return

    if cmd.strip().lower() == "!alltime":
        total, wins, losses, pnl = get_record(guild_id)
        await message.channel.send(f"🌍 All-time: {wins}-{losses} ({total} plays), Net {pnl:+}u")
        return

    if cmd.strip().lower() == "!last10":
        cur = exec_safe("SELECT bet_text, result, date FROM bets WHERE guild_id=%s ORDER BY id DESC LIMIT 10", (guild_id,))
        rows = cur.fetchall()
        if not rows:
            await message.channel.send("No bets logged yet.")
            return
        msg = "**Last 10 plays:**\n" + "\n".join(f"{d} | {t} ({r:+}u)" for t, r, d in rows)
        await message.channel.send(msg)
        return

    if cmd.lower().startswith("!record "):
        try:
            date_str = cmd.split(" ", 1)[1]
            target_date = datetime.datetime.strptime(date_str, "%Y-%m-%d").date()
            total, wins, losses, pnl = get_record(guild_id, target_date, target_date)
            await message.channel.send(f"📅 {target_date}: {wins}-{losses} ({total} plays), Net {pnl:+}u")
        except Exception:
            await message.channel.send("Usage: `!record YYYY-MM-DD`")
        return

    if cmd.lower().startswith("!recap "):
        try:
            date_str = cmd.split(" ", 1)[1]
            target_date = datetime.datetime.strptime(date_str, "%Y-%m-%d").date()
            cur = exec_safe("""
                SELECT id, bet_text, result, odds, bet_type, posted_line, posted_side, closing_line, closing_odds
                FROM bets WHERE guild_id=%s AND date=%s ORDER BY id
            """, (guild_id, target_date))
            rows = cur.fetchall()
            if not rows:
                await message.channel.send(f"No bets logged for {target_date}.")
                return
            lines = [f"**Recap for {target_date}:**"]
            for bet_id, bet_text, result, odds, bet_type, posted_line, posted_side, closing_line, closing_odds in rows:
                clv = calc_clv_for_bet(bet_type, posted_line, posted_side, odds, closing_line, closing_odds)
                clv_tag = ""
                if clv is not None:
                    clv_tag = f" | CLV {'edge ' if bet_type=='moneyline' else ''}{(f'{clv:+.3f}' if bet_type=='moneyline' else f'{clv:+.1f}')}"
                parts = []
                if odds: parts.append(odds)
                if closing_line is not None: parts.append(str(closing_line))
                if closing_odds: parts.append(f"({closing_odds})")
                suffix = (" | " + " ".join(parts)) if parts else ""
                lines.append(f"#{bet_id} {bet_text} ({result:+}u){suffix}{clv_tag}")
            total, wins, losses, pnl = get_record(guild_id, target_date, target_date)
            lines.append(f"\n📅 {target_date}: {wins}-{losses} ({total} plays), Net {pnl:+}u")
            await message.channel.send("\n".join(lines))
        except Exception:
            await message.channel.send("Usage: `!recap YYYY-MM-DD`")
        return

    if cmd.strip().lower() == "!clv":
        cur = exec_safe("""
            SELECT bet_type, posted_line, posted_side, odds, closing_line, closing_odds
            FROM bets WHERE guild_id=%s AND (closing_line IS NOT NULL OR closing_odds IS NOT NULL)
        """, (guild_id,))
        rows = cur.fetchall()
        if not rows:
            await message.channel.send("No CLV data available yet.")
            return
        agg = {}
        for bt, pl, ps, o, cl, co in rows:
            val = calc_clv_for_bet(bt, pl, ps, o, cl, co)
            if val is None:
                continue
            key = bt or "unknown"
            a = agg.get(key, {"sum": 0.0, "cnt": 0})
            a["sum"] += val
            a["cnt"] += 1
            agg[key] = a
        if not agg:
            await message.channel.send("No valid CLV calculations yet.")
            return
        lines = ["**CLV Summary:**"]
        total_sum = 0.0
        total_cnt = 0
        for key, a in agg.items():
            avg = a["sum"] / a["cnt"]
            if key == "moneyline":
                lines.append(f"- {key}: Avg edge {avg:+.3f}")
            else:
                lines.append(f"- {key}: Avg CLV {avg:+.2f}")
            total_sum += a["sum"]
            total_cnt += a["cnt"]
        overall = total_sum / total_cnt if total_cnt else 0.0
        lines.append(f"\nOverall avg: {overall:+.3f}")
        await message.channel.send("\n".join(lines))
        return

    # --- Bet logging (only in configured recap channel) ---
    ch_id = get_channel_id(guild_id)
    if ch_id and message.channel.id == ch_id:
        override = get_override_date(guild_id)
        bet_date = override or extract_date_from_text(message.content) or message.created_at.date()
        lines = message.content.splitlines()
        logged = []
        for line in lines:
            parsed = parse_bet(line)
            if parsed:
                try:
                    logged_line = await log_bet(line, parsed, bet_date, guild_id)
                    logged.append(logged_line)
                except Exception:
                    conn.rollback()
                    continue
        if override:
            clear_override_date(guild_id)
        if logged:
            total, wins, losses, pnl = get_record(guild_id, bet_date, bet_date)
            msg = "Logged plays:\n" + "\n".join(logged)
            msg += f"\n\n📅 {bet_date}: {wins}-{losses} ({total} plays), Net {pnl:+}u"
            await message.channel.send(msg)

# --- Run ---
if __name__ == "__main__":
    if not TOKEN:
        raise RuntimeError("DISCORD_TOKEN not set in environment variables")
    client.run(TOKEN)
