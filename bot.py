# bot.py
import os
import logging
import asyncio

import discord
from discord.ext import commands

from utils.db import init_db, db_pool  # pool will be assigned after init
# cogs will be loaded dynamically

LOG_LEVEL = os.environ.get("LOG_LEVEL", "INFO")
DISCORD_TOKEN = os.environ.get("DISCORD_TOKEN")
if not DISCORD_TOKEN:
    raise RuntimeError("DISCORD_TOKEN must be set in environment")

logging.basicConfig(level=LOG_LEVEL)
logger = logging.getLogger("recap-bot")

intents = discord.Intents.default()
intents.messages = True
intents.guilds = True
intents.message_content = True  # required if you need message content in newer bots (depends on privileges)

bot = commands.Bot(command_prefix="!", intents=intents, help_command=None)

@bot.event
async def on_ready():
    logger.info(f"Bot ready. Logged in as {bot.user} (id={bot.user.id})")
    # Initialize DB
    await init_db()
    # Load cogs
    try:
        await bot.load_extension("cogs.recap")
        await bot.load_extension("cogs.stats")
        await bot.load_extension("cogs.reports")
        await bot.load_extension("cogs.admin")
    except Exception as e:
        logger.exception("Error loading cogs: %s", e)

    # Sync app commands (slash commands)
    try:
        await bot.tree.sync()
        logger.info("Synced application commands (slash commands).")
    except Exception as e:
        logger.exception("Failed to sync app commands: %s", e)

if __name__ == "__main__":
    bot.run(DISCORD_TOKEN)
