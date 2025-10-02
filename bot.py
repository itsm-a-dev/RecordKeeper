# bot.py
import os, discord
from models import bootstrap_schema
from clv import clv_scheduler, run_updateclv_for_guild
from odds_api import fetch_and_store_closings

intents = discord.Intents.default()
intents.message_content = True
client = discord.Client(intents=intents)

bootstrap_schema()

@client.event
async def on_ready():
    print(f"Logged in as {client.user}")
    client.loop.create_task(clv_scheduler(client))

@client.event
async def on_message(message):
    if message.author.bot: return
    cmd = message.content.strip().lower()
    if cmd == "!ping":
        await message.channel.send("pong")
    if cmd == "!updateclv":
        updated = await run_updateclv_for_guild(message.guild.id)
        await message.channel.send(f"🔄 CLV update complete. Filled {updated} bets.")
    if cmd.startswith("!fetchclosings"):
        sport = cmd.split(" ",1)[1] if " " in cmd else "nba"
        count = fetch_and_store_closings(message.guild.id, sport)
        await message.channel.send(f"📊 Inserted {count} closings for {sport.upper()}.")

TOKEN = os.getenv("DISCORD_TOKEN")
if not TOKEN:
    raise RuntimeError("DISCORD_TOKEN not set")
client.run(TOKEN)
