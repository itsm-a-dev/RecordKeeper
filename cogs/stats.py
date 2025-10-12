# cogs/stats.py
import discord
from discord import app_commands
from discord.ext import commands
from datetime import datetime, timedelta
from decimal import Decimal
import calendar
import asyncpg
import logging

from utils.db import db_pool

logger = logging.getLogger("recap-bot.stats")

def format_units(u: Decimal) -> str:
    # normalize keeps trailing zeros tidy
    u = u.quantize(Decimal('0.01')) if isinstance(u, Decimal) else Decimal(u)
    if u > 0:
        return f"+{u}u"
    elif u < 0:
        return f"{u}u"
    return "0u"

class Stats(commands.Cog):
    def __init__(self, bot: commands.Bot):
        self.bot = bot

    async def fetchrow(self, query, *args):
        async with db_pool.acquire() as conn:
            return await conn.fetchrow(query, *args)

    async def fetch(self, query, *args):
        async with db_pool.acquire() as conn:
            return await conn.fetch(query, *args)

    def month_bounds(self, month: int, year: int):
        first_day = datetime(year, month, 1).date()
        last_day = datetime(year, month, calendar.monthrange(year, month)[1]).date()
        return first_day, last_day

    @app_commands.command(name="alltime", description="Show all-time record and performance stats.")
    async def alltime(self, interaction: discord.Interaction):
        guild_id = interaction.guild_id
        row = await self.fetchrow("""
            SELECT 
                SUM(wins) as wins,
                SUM(losses) as losses,
                SUM(pushes) as pushes,
                SUM(hooks) as hooks,
                SUM(total_units) as net_units
            FROM daily_recaps WHERE guild_id = $1
        """, guild_id)
        if not row or all(v is None for v in row.values()):
            await interaction.response.send_message("No recaps found for this server yet.", ephemeral=True)
            return
        wins = row["wins"] or 0
        losses = row["losses"] or 0
        pushes = row["pushes"] or 0
        hooks = row["hooks"] or 0
        net_units = Decimal(str(row["net_units"] or "0"))
        total = wins + losses
        win_pct = (wins / total * 100) if total > 0 else 0.0
        roi = (net_units / (wins + losses) * 100) if (wins + losses) > 0 else Decimal(0)

        embed = discord.Embed(title="🏆 All-Time Record", color=discord.Color.gold(), timestamp=datetime.utcnow())
        embed.add_field(name="Wins", value=wins)
        embed.add_field(name="Losses", value=losses)
        embed.add_field(name="Pushes", value=pushes)
        embed.add_field(name="Hooks", value=hooks)
        embed.add_field(name="Win %", value=f"{win_pct:.1f}%")
        embed.add_field(name="ROI", value=f"{roi:.1f}%")
        embed.add_field(name="Net Units", value=format_units(net_units), inline=False)
        await interaction.response.send_message(embed=embed)

    @app_commands.command(name="month", description="Show record for a specific month (1-12) and year.")
    @app_commands.describe(month="Month (1-12)", year="Year (YYYY)")
    async def month(self, interaction: discord.Interaction, month: int, year: int):
        guild_id = interaction.guild_id
        start, end = self.month_bounds(month, year)
        row = await self.fetchrow("""
            SELECT SUM(wins) as wins, SUM(losses) as losses, SUM(pushes) as pushes,
                   SUM(hooks) as hooks, SUM(total_units) as net_units
            FROM daily_recaps WHERE guild_id = $1 AND recap_date BETWEEN $2 AND $3
        """, guild_id, start, end)
        if not row or all(v is None for v in row.values()):
            await interaction.response.send_message(f"No data found for {month}/{year}.", ephemeral=True)
            return
        wins = row["wins"] or 0
        losses = row["losses"] or 0
        pushes = row["pushes"] or 0
        hooks = row["hooks"] or 0
        net_units = Decimal(str(row["net_units"] or "0"))
        total = wins + losses
        win_pct = (wins / total * 100) if total > 0 else 0.0

        embed = discord.Embed(title=f"📅 {calendar.month_name[month]} {year} Performance", color=discord.Color.blue())
        embed.add_field(name="Wins", value=wins); embed.add_field(name="Losses", value=losses)
        embed.add_field(name="Pushes", value=pushes); embed.add_field(name="Hooks", value=hooks)
        embed.add_field(name="Win %", value=f"{win_pct:.1f}%"); embed.add_field(name="Net Units", value=format_units(net_units), inline=False)
        await interaction.response.send_message(embed=embed)

    @app_commands.command(name="day", description="Show record for a specific day (month, day, optional year).")
    @app_commands.describe(month="Month (1-12)", day="Day (1-31)", year="Year (YYYY, optional)")
    async def day(self, interaction: discord.Interaction, month: int, day: int, year: int = None):
        if not year:
            year = datetime.utcnow().year
        guild_id = interaction.guild_id
        date = datetime(year, month, day).date()
        row = await self.fetchrow("SELECT wins, losses, pushes, hooks, total_units FROM daily_recaps WHERE guild_id = $1 AND recap_date = $2", guild_id, date)
        if not row:
            await interaction.response.send_message(f"No record found for {month}/{day}/{year}.", ephemeral=True)
            return
        net = Decimal(str(row["total_units"]))
        color = discord.Color.green() if net > 0 else discord.Color.red() if net < 0 else discord.Color.light_grey()
        embed = discord.Embed(title=f"📅 Daily Recap — {month}/{day}/{year}", color=color)
        embed.add_field(name="Wins", value=row["wins"]); embed.add_field(name="Losses", value=row["losses"])
        embed.add_field(name="Pushes", value=row["pushes"]); embed.add_field(name="Hooks", value=row["hooks"])
        embed.add_field(name="Net Units", value=format_units(net), inline=False)
        await interaction.response.send_message(embed=embed)

    @app_commands.command(name="sport", description="Show performance for a sport keyword or emoji.")
    @app_commands.describe(sport="Sport emoji or keyword (e.g., 🏈 or 'MLB')")
    async def sport(self, interaction: discord.Interaction, sport: str):
        guild_id = interaction.guild_id
        sport_q = f"%{sport.strip().lower()}%"
        rows = await self.fetch("""
            SELECT b.sport, b.result, b.units
            FROM bets b
            JOIN daily_recaps d ON b.recap_id = d.id
            WHERE d.guild_id = $1 AND (LOWER(b.sport) LIKE $2 OR LOWER(b.description) LIKE $2)
        """, guild_id, sport_q)
        if not rows:
            await interaction.response.send_message(f"No bets found for {sport}.", ephemeral=True)
            return
        wins = sum(1 for r in rows if r["result"] == "win")
        losses = sum(1 for r in rows if r["result"] == "loss")
        pushes = sum(1 for r in rows if r["result"] == "push")
        hooks = sum(1 for r in rows if r["result"] == "hook")
        net = sum(Decimal(str(r["units"])) if r["result"] == "win" else -Decimal(str(r["units"])) for r in rows if r["result"] in ("win","loss","hook"))
        total = wins + losses
        win_pct = (wins / total * 100) if total > 0 else 0.0
        embed = discord.Embed(title=f"⚙️ Sport Breakdown: {sport}", color=discord.Color.orange())
        embed.add_field(name="Wins", value=wins); embed.add_field(name="Losses", value=losses)
        embed.add_field(name="Pushes", value=pushes); embed.add_field(name="Hooks", value=hooks)
        embed.add_field(name="Win %", value=f"{win_pct:.1f}%"); embed.add_field(name="Net Units", value=format_units(Decimal(str(net))))
        await interaction.response.send_message(embed=embed)

    @app_commands.command(name="streak", description="Show current winning/losing streak.")
    async def streak(self, interaction: discord.Interaction):
        guild_id = interaction.guild_id
        rows = await self.fetch("SELECT recap_date, total_units FROM daily_recaps WHERE guild_id = $1 ORDER BY recap_date DESC", guild_id)
        if not rows:
            await interaction.response.send_message("No recaps available.", ephemeral=True)
            return
        streak_type = None; streak_len = 0
        for r in rows:
            net = Decimal(str(r["total_units"]))
            result = "win" if net > 0 else "loss" if net < 0 else "push"
            if streak_type is None:
                streak_type = result; streak_len = 1
            elif result == streak_type:
                streak_len += 1
            else:
                break
        emoji = "🔥" if streak_type == "win" else "🥶" if streak_type == "loss" else "😐"
        await interaction.response.send_message(f"{emoji} Current {streak_type.upper()} streak: {streak_len} days")

    @app_commands.command(name="bestday", description="Show best day by net units.")
    async def bestday(self, interaction: discord.Interaction):
        guild_id = interaction.guild_id
        row = await self.fetchrow("SELECT recap_date, total_units FROM daily_recaps WHERE guild_id=$1 ORDER BY total_units DESC LIMIT 1", guild_id)
        if not row:
            await interaction.response.send_message("No records yet.", ephemeral=True)
            return
        await interaction.response.send_message(f"🏅 Best day: {row['recap_date']} ({format_units(Decimal(str(row['total_units'])))}).")

    @app_commands.command(name="worstday", description="Show worst day by net units.")
    async def worstday(self, interaction: discord.Interaction):
        guild_id = interaction.guild_id
        row = await self.fetchrow("SELECT recap_date, total_units FROM daily_recaps WHERE guild_id=$1 ORDER BY total_units ASC LIMIT 1", guild_id)
        if not row:
            await interaction.response.send_message("No records yet.", ephemeral=True)
            return
        await interaction.response.send_message(f"💀 Worst day: {row['recap_date']} ({format_units(Decimal(str(row['total_units'])))}).")

async def setup(bot: commands.Bot):
    await bot.add_cog(Stats(bot))
