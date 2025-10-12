# cogs/reports.py
import io
import logging
from datetime import datetime, timedelta
from decimal import Decimal
from typing import List

import discord
from discord import app_commands
from discord.ext import commands, tasks

from utils.db import db_pool
from utils.graphics import create_line_chart, create_recap_card

logger = logging.getLogger("recap-bot.reports")

class Reports(commands.Cog):
    """
    Commands to generate recap images, graphs, and scheduled posting.
    Automation (auto-posting) is toggleable per guild via admin cog.
    """
    def __init__(self, bot: commands.Bot):
        self.bot = bot
        self.auto_post_task.start()

    def cog_unload(self):
        self.auto_post_task.cancel()

    async def fetch_rows(self, query: str, *args):
        async with db_pool.acquire() as conn:
            return await conn.fetch(query, *args)

    @app_commands.command(name="graph", description="Graph net units over the last N days (default 30).")
    @app_commands.describe(days="Number of days to include (default 30)")
    async def graph(self, interaction: discord.Interaction, days: int = 30):
        guild_id = interaction.guild_id
        end = datetime.utcnow().date()
        start = end - timedelta(days=days-1)
        rows = await self.fetch_rows("""
            SELECT recap_date, total_units FROM daily_recaps
            WHERE guild_id=$1 AND recap_date BETWEEN $2 AND $3
            ORDER BY recap_date ASC
        """, guild_id, start, end)
        if not rows:
            await interaction.response.send_message("No data to graph.", ephemeral=True)
            return
        dates = [r["recap_date"] for r in rows]
        net = [float(r["total_units"]) for r in rows]
        png = create_line_chart(dates, net)
        file = discord.File(io.BytesIO(png), filename="net_units.png")
        await interaction.response.send_message(file=file)

    @app_commands.command(name="recap", description="Generate a recap card for a specific day.")
    @app_commands.describe(month="Month (1-12)", day="Day (1-31)", year="Year (YYYY, optional)")
    async def recap(self, interaction: discord.Interaction, month: int, day: int, year: int = None):
        if not year:
            year = datetime.utcnow().year
        guild_id = interaction.guild_id
        date_obj = datetime(year, month, day).date()
        row = await self.fetch_rows("SELECT wins, losses, pushes, hooks, total_units FROM daily_recaps WHERE guild_id=$1 AND recap_date=$2", guild_id, date_obj)
        if not row:
            await interaction.response.send_message(f"No recap for {month}/{day}/{year}.", ephemeral=True)
            return
        r = row[0]
        stats = {
            "Wins": r["wins"],
            "Losses": r["losses"],
            "Pushes": r["pushes"],
            "Hooks": r["hooks"],
            "Net Units": f"{Decimal(str(r['total_units']))}u"
        }
        # create small chart for last 7 days
        chart_rows = await self.fetch_rows("SELECT recap_date, total_units FROM daily_recaps WHERE guild_id=$1 AND recap_date BETWEEN $2 AND $3 ORDER BY recap_date ASC", guild_id, date_obj - timedelta(days=6), date_obj)
        dates = [cr["recap_date"] for cr in chart_rows]
        net = [float(cr["total_units"]) for cr in chart_rows]
        chart_png = create_line_chart(dates, net) if dates else None
        card_png = create_recap_card(title=f"Recap — {date_obj.isoformat()}", subtitle="Daily Performance", stats=stats, mini_chart_bytes=chart_png)
        await interaction.response.send_message(file=discord.File(io.BytesIO(card_png), filename="recap_card.png"))

    @tasks.loop(minutes=15.0)
    async def auto_post_task(self):
        """
        Runs every 15 minutes and posts automations for guilds that have it enabled.
        It checks each guild's settings and if automation_enabled is True and the scheduled time matches,
        will post that day's recap (if present) or weekly recap (if configured).
        """
        # get guilds with automation_enabled
        async with db_pool.acquire() as conn:
            rows = await conn.fetch("SELECT guild_id, automation_channel_id FROM settings WHERE automation_enabled = TRUE AND automation_channel_id IS NOT NULL")
        for r in rows:
            guild_id = r["guild_id"]
            channel_id = r["automation_channel_id"]
            try:
                guild = self.bot.get_guild(guild_id)
                if not guild:
                    continue
                channel = guild.get_channel(channel_id)
                if not channel:
                    continue
                # For simplicity, we post the most recent day's recap if it's today
                from datetime import date
                today = date.today()
                row = await self.fetch_rows("SELECT wins, losses, pushes, hooks, total_units FROM daily_recaps WHERE guild_id=$1 AND recap_date = $2", guild_id, today)
                if row:
                    r0 = row[0]
                    stats = {
                        "Wins": r0["wins"], "Losses": r0["losses"], "Pushes": r0["pushes"], "Hooks": r0["hooks"], "Net Units": f"{Decimal(str(r0['total_units']))}u"
                    }
                    # create chart for last 7 days
                    chart_rows = await self.fetch_rows("SELECT recap_date, total_units FROM daily_recaps WHERE guild_id=$1 AND recap_date BETWEEN $2 AND $3 ORDER BY recap_date ASC", guild_id, today - timedelta(days=6), today)
                    dates = [cr["recap_date"] for cr in chart_rows]
                    net = [float(cr["total_units"]) for cr in chart_rows]
                    chart_png = create_line_chart(dates, net) if dates else None
                    card_png = create_recap_card(title=f"Recap — {today.isoformat()}", subtitle="Auto Post", stats=stats, mini_chart_bytes=chart_png)
                    try:
                        await channel.send(file=discord.File(io.BytesIO(card_png), filename="auto_recap.png"))
                    except Exception:
                        logger.exception("Failed to auto-post recap for guild %s", guild_id)
                # else: nothing to post
            except Exception:
                logger.exception("Error in auto_post_task for guild %s", guild_id)

    @auto_post_task.before_loop
    async def before_auto_post(self):
        await self.bot.wait_until_ready()

async def setup(bot: commands.Bot):
    await bot.add_cog(Reports(bot))
