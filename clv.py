# clv.py
import datetime, asyncio
from db import exec_safe, conn
from models import get_channel_id
from odds_api import fetch_and_store_closings

def try_update_bet_with_closing(bet_id, guild_id, bet_text, sport, bet_type, date):
    # simplified: build event_key and try to match closings
    event_key = f"{sport}|{date.isoformat()}|{bet_type}|{bet_text[:50]}"
    rows = exec_safe("SELECT closing_line, closing_odds FROM closings WHERE guild_id=%s AND event_key=%s",
                     (guild_id, event_key), fetch="all")
    if rows:
        cl, co = rows[0]
        exec_safe("UPDATE bets SET closing_line=%s, closing_odds=%s WHERE id=%s", (cl, co, bet_id))
        conn.commit()
        return True
    return False

async def run_updateclv_for_guild(guild_id):
    rows = exec_safe("SELECT id, bet_text, sport, bet_type, date FROM bets WHERE guild_id=%s", (guild_id,), fetch="all")
    updated = 0
    for bet_id, bet_text, sport, bet_type, bdate in rows:
        if try_update_bet_with_closing(bet_id, guild_id, bet_text, sport, bet_type, bdate):
            updated += 1
    return updated

async def clv_scheduler(client):
    await client.wait_until_ready()
    while not client.is_closed():
        now = datetime.datetime.now(datetime.UTC)
        if now.weekday() in (1,4) and now.hour == 3 and now.minute == 0:
            for guild in client.guilds:
                for sport in ["nba","nfl","mlb","nhl","soccer"]:
                    fetch_and_store_closings(guild.id, sport)
                updated = await run_updateclv_for_guild(guild.id)
                ch_id = get_channel_id(guild.id)
                if ch_id:
                    channel = client.get_channel(ch_id)
                    if channel:
                        await channel.send(f"🤖 Auto CLV update: filled {updated} bets.")
        await asyncio.sleep(60)
