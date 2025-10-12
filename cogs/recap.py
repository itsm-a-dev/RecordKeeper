# cogs/recap.py
import re
import logging
from decimal import Decimal
from datetime import datetime
from typing import Any, Dict, List, Optional, Tuple

import discord
from discord.ext import commands

from utils.db import db_pool

logger = logging.getLogger("recap-bot.recap")

# Regexes
RE_SUMMARY = re.compile(r'(?P<date>\d{1,2}/\d{1,2}(?:/\d{2,4})?)\s*[:\-]\s*(?P<wins>\d+)\s*[-–]\s*(?P<losses>\d+)', re.IGNORECASE)
RE_UNITS = re.compile(r'(?P<units>\d+(?:\.\d+)?)\s*u\b', re.IGNORECASE)
RE_ODDS = re.compile(r'([+-]?\d{1,4}(?:/\d{1,4})?)')
RE_PUSH = re.compile(r'\bPUSH\b', re.IGNORECASE)
RE_HOOK_WORD = re.compile(r'\bhook\b', re.IGNORECASE)
WIN_EMOJI = "✅"
LOSS_EMOJI = "❌"
HOOK_EMOJI = "🪝"

SPORT_EMOJIS = set(["🏈","⚾️","🏒","🏀","⚽️","🎾","🎱","🏐","🥎","🏉"])

def find_any_emoji(text: str) -> Optional[str]:
    for ch in text:
        if ch in SPORT_EMOJIS:
            return ch
    return None

def parse_bet_line(line: str) -> Optional[Dict[str, Any]]:
    original = line.strip()
    if not original:
        return None
    result = None
    if WIN_EMOJI in original:
        result = "win"
        original = original.replace(WIN_EMOJI, "")
    if LOSS_EMOJI in original:
        result = "loss"
        original = original.replace(LOSS_EMOJI, "")
    if HOOK_EMOJI in original or RE_HOOK_WORD.search(original):
        result = "hook"
        original = original.replace(HOOK_EMOJI, "")
    if RE_PUSH.search(original):
        result = "push"
        original = RE_PUSH.sub("", original)
    sport = find_any_emoji(original)
    if sport:
        original = original.replace(sport, "").strip()
    m_units = RE_UNITS.search(original)
    if m_units:
        units = Decimal(m_units.group("units"))
        original = RE_UNITS.sub("", original).strip()
    else:
        units = Decimal("1")
    m_odds = RE_ODDS.search(original)
    odds = None
    if m_odds:
        odds = m_odds.group(1)
        original = original.replace(odds, "", 1).strip()
    description = original.strip() or None
    return {
        "units": units,
        "sport": sport,
        "description": description,
        "odds": odds,
        "result": result
    }

def extract_summary(lines: List[str]) -> Optional[Tuple[datetime.date, int, int]]:
    for line in reversed(lines):
        m = RE_SUMMARY.search(line)
        if m:
            date_str = m.group("date")
            wins = int(m.group("wins"))
            losses = int(m.group("losses"))
            parts = date_str.split("/")
            mm = int(parts[0]); dd = int(parts[1])
            if len(parts) == 3:
                yy = int(parts[2]); 
                if yy < 100: yy += 2000
            else:
                yy = datetime.utcnow().year
            return datetime(yy, mm, dd).date(), wins, losses
    return None

def collapse_parlay_blocks(lines: List[str]) -> List[str]:
    out = []
    i = 0
    while i < len(lines):
        line = lines[i].strip()
        lower = line.lower()
        if "parlay" in lower or line.rstrip().endswith(":"):
            block = [line]
            j = i + 1
            while j < len(lines):
                nxt = lines[j].strip()
                if not nxt:
                    break
                if RE_UNITS.search(nxt) or any(e in nxt for e in SPORT_EMOJIS) or RE_SUMMARY.search(nxt) or "parlay" in nxt.lower():
                    break
                block.append(nxt)
                j += 1
            out.append(" | ".join(block))
            i = j
        else:
            out.append(line)
            i += 1
    return out

def parse_message_content(content: str) -> Dict[str, Any]:
    raw_lines = [l.rstrip() for l in content.splitlines() if l.strip()]
    if not raw_lines:
        return {"recap_date": None, "bets": [], "summary_wins": None, "summary_losses": None}
    summary = extract_summary(raw_lines)
    if summary:
        recap_date, summary_wins, summary_losses = summary
        idx = next((i for i,l in enumerate(raw_lines) if RE_SUMMARY.search(l)), len(raw_lines))
        relevant = raw_lines[:idx]
    else:
        recap_date = None
        summary_wins = None
        summary_losses = None
        relevant = raw_lines
    collapsed = collapse_parlay_blocks(relevant)
    bets = []
    for line in collapsed:
        p = parse_bet_line(line)
        if p:
            bets.append(p)
    return {"recap_date": recap_date, "bets": bets, "summary_wins": summary_wins, "summary_losses": summary_losses}

def validate_parsed(parsed: Dict[str, Any]) -> Tuple[bool, str]:
    if parsed["recap_date"] is None:
        return False, "No summary date (MM/DD: W-L) found."
    bets = parsed["bets"]
    if not bets:
        return False, "No bet lines found above summary."
    wins = losses = pushes = hooks = 0
    for b in bets:
        r = (b.get("result") or "").lower()
        if r == "win": wins += 1
        elif r == "loss": losses += 1
        elif r == "hook": hooks += 1; losses += 1
        elif r == "push": pushes += 1
        else:
            return False, f"Missing result on bet line: {b.get('description')}"
    if parsed["summary_wins"] is not None:
        if wins != parsed["summary_wins"] or losses != parsed["summary_losses"]:
            return False, f"Summary mismatch: parsed {wins}-{losses} != summary {parsed['summary_wins']}-{parsed['summary_losses']}"
    return True, "OK"

async def upsert_recap_record(conn, guild_id: int, channel_id: int, message_id: int, parsed: Dict[str, Any]):
    """
    Insert or update daily_recaps and bets. Uses existing connection/transaction.
    """
    recap_date = parsed["recap_date"]
    bets = parsed["bets"]
    wins = losses = pushes = hooks = 0
    total_units = Decimal("0")
    for b in bets:
        units = Decimal(str(b["units"]))
        r = (b.get("result") or "").lower()
        if r == "win":
            wins += 1
            total_units += units
        elif r == "loss":
            losses += 1
            total_units -= units
        elif r == "hook":
            hooks += 1
            losses += 1
            total_units -= units
        elif r == "push":
            pushes += 1
    existing = await conn.fetchrow("SELECT id FROM daily_recaps WHERE message_id = $1", message_id)
    if existing:
        recap_id = existing["id"]
        await conn.execute("""
            UPDATE daily_recaps SET recap_date=$1, wins=$2, losses=$3, pushes=$4, hooks=$5, total_units=$6, updated_at=NOW()
            WHERE id=$7
        """, recap_date, wins, losses, pushes, hooks, str(total_units), recap_id)
        await conn.execute("DELETE FROM bets WHERE recap_id = $1", recap_id)
    else:
        row = await conn.fetchrow("""
            INSERT INTO daily_recaps (guild_id, channel_id, message_id, recap_date, wins, losses, pushes, hooks, total_units)
            VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9) RETURNING id
        """, guild_id, channel_id, message_id, recap_date, wins, losses, pushes, hooks, str(total_units))
        recap_id = row["id"]
    for b in bets:
        await conn.execute("""
            INSERT INTO bets (recap_id, sport, units, description, odds, result)
            VALUES ($1,$2,$3,$4,$5,$6)
        """, recap_id, b.get("sport"), str(b.get("units")), b.get("description"), b.get("odds"), b.get("result"))

class RecapCog(commands.Cog):
    def __init__(self, bot: commands.Bot):
        self.bot = bot

    async def cog_load(self):
        logger.info("RecapCog loaded")

    async def is_recap_channel(self, guild_id: int, channel_id: int) -> bool:
        async with db_pool.acquire() as conn:
            row = await conn.fetchrow("SELECT recap_channel_id FROM settings WHERE guild_id = $1", guild_id)
            if not row or row["recap_channel_id"] is None:
                return False
            return int(row["recap_channel_id"]) == int(channel_id)

    async def update_import_progress(self, conn, guild_id: int, channel_id: int, message_id: int):
        await conn.execute("""
            INSERT INTO import_progress (guild_id, channel_id, last_message_id)
            VALUES ($1,$2,$3)
            ON CONFLICT (guild_id, channel_id) DO UPDATE SET last_message_id = EXCLUDED.last_message_id
        """, guild_id, channel_id, message_id)

    async def get_import_progress(self, conn, guild_id: int, channel_id: int) -> Optional[int]:
        row = await conn.fetchrow("SELECT last_message_id FROM import_progress WHERE guild_id = $1 AND channel_id = $2", guild_id, channel_id)
        return row["last_message_id"] if row else None

    @commands.Cog.listener()
    async def on_message(self, message: discord.Message):
        if message.author.bot:
            return
        if not message.guild:
            return
        if not await self.is_recap_channel(message.guild.id, message.channel.id):
            return
        parsed = parse_message_content(message.content)
        ok, reason = validate_parsed(parsed)
        if not ok:
            await message.channel.send(f"⚠️ Recap not stored: {reason}")
            return
        async with db_pool.acquire() as conn:
            async with conn.transaction():
                await upsert_recap_record(conn, message.guild.id, message.channel.id, message.id, parsed)
                await self.update_import_progress(conn, message.guild.id, message.channel.id, message.id)
        await message.channel.send(f"✅ Logged recap for {parsed['recap_date'].isoformat()}")

    @commands.Cog.listener()
    async def on_message_edit(self, before: discord.Message, after: discord.Message):
        if after.author.bot:
            return
        if not after.guild:
            return
        if not await self.is_recap_channel(after.guild.id, after.channel.id):
            return
        parsed = parse_message_content(after.content)
        ok, reason = validate_parsed(parsed)
        if not ok:
            await after.channel.send(f"⚠️ Edited recap not stored: {reason}")
            return
        async with db_pool.acquire() as conn:
            async with conn.transaction():
                await upsert_recap_record(conn, after.guild.id, after.channel.id, after.id, parsed)
                await self.update_import_progress(conn, after.guild.id, after.channel.id, after.id)
        await after.channel.send(f"🔁 Recap updated for {parsed['recap_date'].isoformat()}")

    async def import_history(self, channel: discord.TextChannel, limit: Optional[int] = None, resume=True, batch_size: int = 200):
        """
        Import channel history into DB. Checkpointing/resume supported via import_progress table.
        - resume: if True, uses import_progress to skip already-processed messages
        - processes in chronological order (oldest_first=True)
        - respects batch_size to avoid memory spikes
        """
        guild_id = channel.guild.id
        channel_id = channel.id
        imported = skipped = processed = 0

        async with db_pool.acquire() as conn:
            last_id = None
            if resume:
                last_id_row = await conn.fetchrow("SELECT last_message_id FROM import_progress WHERE guild_id=$1 AND channel_id=$2", guild_id, channel_id)
                last_id = last_id_row["last_message_id"] if last_id_row else None

        # Use discord history pagination with oldest_first=True and "after" if resuming
        kwargs = {"limit": None, "oldest_first": True}
        if last_id and resume:
            # resume after last processed message id
            try:
                after_msg = await channel.fetch_message(last_id)
                kwargs["after"] = after_msg
            except Exception:
                # if fetch_message fails, ignore and start from beginning
                pass

        # Process in batches to avoid large memory usage
        buffer = []
        async for message in channel.history(**kwargs):
            if message.author.bot:
                continue
            buffer.append(message)
            if len(buffer) >= batch_size:
                # process batch
                async with db_pool.acquire() as conn:
                    async with conn.transaction():
                        for msg in buffer:
                            parsed = parse_message_content(msg.content)
                            ok, reason = validate_parsed(parsed)
                            if not ok:
                                skipped += 1
                                continue
                            await upsert_recap_record(conn, guild_id, channel_id, msg.id, parsed)
                            await self.update_import_progress(conn, guild_id, channel_id, msg.id)
                            imported += 1
                            processed += 1
                buffer.clear()
        # leftover
        if buffer:
            async with db_pool.acquire() as conn:
                async with conn.transaction():
                    for msg in buffer:
                        parsed = parse_message_content(msg.content)
                        ok, reason = validate_parsed(parsed)
                        if not ok:
                            skipped += 1
                            continue
                        await upsert_recap_record(conn, guild_id, channel_id, msg.id, parsed)
                        await self.update_import_progress(conn, guild_id, channel_id, msg.id)
                        imported += 1
                        processed += 1
            buffer.clear()

        return {"processed": processed, "imported": imported, "skipped": skipped}

async def setup(bot: commands.Bot):
    await bot.add_cog(RecapCog(bot))
