# cogs/admin.py
import io
import csv
import logging
from typing import Optional

import discord
from discord import app_commands
from discord.ext import commands

from utils.db import db_pool

logger = logging.getLogger("recap-bot.admin")

class Admin(commands.Cog):
    def __init__(self, bot: commands.Bot):
        self.bot = bot

    async def fetchrow(self, query, *args):
        async with db_pool.acquire() as conn:
            return await conn.fetchrow(query, *args)

    async def execute(self, query, *args):
        async with db_pool.acquire() as conn:
            return await conn.execute(query, *args)

    @app_commands.command(name="setrecap", description="Set the current channel as the recap channel and import history.")
    @app_commands.checks.has_permissions(administrator=True)
    async def setrecap(self, interaction: discord.Interaction):
        guild_id = interaction.guild_id
        channel_id = interaction.channel_id
        # store setting
        await self.execute("INSERT INTO settings (guild_id, recap_channel_id) VALUES ($1,$2) ON CONFLICT (guild_id) DO UPDATE SET recap_channel_id = EXCLUDED.recap_channel_id", guild_id, channel_id)
        await interaction.response.send_message("✅ This channel is set as the recap channel. Importing history (background)...", ephemeral=True)
        # kick off background import using the Recap cog's import_history
        recap_cog = self.bot.get_cog("RecapCog")
        channel = interaction.channel
        # perform import in background to avoid timeout; run in separate task
        async def _do_import():
            result = await recap_cog.import_history(channel, resume=True)
            await channel.send(f"✅ History import complete. Processed {result['processed']} messages — imported {result['imported']}, skipped {result['skipped']}.")
        self.bot.loop.create_task(_do_import())

    @app_commands.command(name="automation", description="Toggle automation (auto-post) for this server.")
    @app_commands.describe(action="'on' or 'off', channel='channel to auto-post into (optional)')")
    @app_commands.checks.has_permissions(administrator=True)
    async def automation(self, interaction: discord.Interaction, action: str, channel: Optional[discord.TextChannel] = None):
        guild_id = interaction.guild_id
        action = action.lower()
        if action not in ("on", "off"):
            await interaction.response.send_message("Action must be 'on' or 'off'.", ephemeral=True)
            return
        if action == "on" and channel is None:
            await interaction.response.send_message("Please specify a channel to post automation into.", ephemeral=True)
            return
        if action == "on":
            await self.execute("INSERT INTO settings (guild_id, automation_enabled, automation_channel_id) VALUES ($1, TRUE, $2) ON CONFLICT (guild_id) DO UPDATE SET automation_enabled = TRUE, automation_channel_id = EXCLUDED.automation_channel_id", guild_id, channel.id)
            await interaction.response.send_message(f"✅ Automation enabled. Will post into {channel.mention}.", ephemeral=True)
        else:
            await self.execute("UPDATE settings SET automation_enabled = FALSE, automation_channel_id = NULL WHERE guild_id = $1", guild_id)
            await interaction.response.send_message("🛑 Automation disabled for this server.", ephemeral=True)

    @app_commands.command(name="reimport", description="Force reimport of channel history (admin only).")
    @app_commands.checks.has_permissions(administrator=True)
    async def reimport(self, interaction: discord.Interaction, limit: Optional[int] = None):
        channel = interaction.channel
        recap_cog = self.bot.get_cog("RecapCog")
        await interaction.response.send_message("🔁 Re-import started (background)...", ephemeral=True)
        async def _do_reimport():
            result = await recap_cog.import_history(channel, resume=False)
            await channel.send(f"✅ Reimport complete. Processed {result['processed']} messages — imported {result['imported']}, skipped {result['skipped']}.")
        self.bot.loop.create_task(_do_reimport())

    @app_commands.command(name="export", description="Export recaps to CSV for this server.")
    @app_commands.checks.has_permissions(administrator=True)
    async def export(self, interaction: discord.Interaction):
        guild_id = interaction.guild_id
        await interaction.response.send_message("📤 Exporting data (background)...", ephemeral=True)
        async def _do_export():
            async with db_pool.acquire() as conn:
                rows = await conn.fetch("SELECT d.recap_date, d.wins, d.losses, d.pushes, d.hooks, d.total_units, b.sport, b.units, b.description, b.odds, b.result FROM daily_recaps d LEFT JOIN bets b ON b.recap_id = d.id WHERE d.guild_id = $1 ORDER BY d.recap_date ASC", guild_id)
            # write CSV in memory
            buf = io.StringIO()
            writer = csv.writer(buf)
            writer.writerow(["recap_date","wins","losses","pushes","hooks","total_units","sport","units","description","odds","result"])
            for r in rows:
                writer.writerow([r["recap_date"], r["wins"], r["losses"], r["pushes"], r["hooks"], str(r["total_units"]), r["sport"], str(r["units"]), r["description"], r["odds"], r["result"]])
            buf.seek(0)
            await interaction.followup.send(file=discord.File(io.BytesIO(buf.getvalue().encode()), filename=f"recaps_{guild_id}.csv"))
        self.bot.loop.create_task(_do_export())

async def setup(bot: commands.Bot):
    await bot.add_cog(Admin(bot))
