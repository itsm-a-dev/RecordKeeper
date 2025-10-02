from db import exec_safe

def get_daily_record(guild_id):
    row = exec_safe("SELECT COUNT(*), SUM(result) FROM bets WHERE guild_id=%s AND date=CURRENT_DATE", (guild_id,), fetch="one")
    if not row: return "No bets today."
    count, profit = row
    return f"📅 Daily record: {count} plays, Net {profit:+.2f}u"

def get_mtd_record(guild_id):
    row = exec_safe("SELECT COUNT(*), SUM(result) FROM bets WHERE guild_id=%s AND date >= date_trunc('month', CURRENT_DATE)", (guild_id,), fetch="one")
    if not row: return "No bets this month."
    count, profit = row
    return f"📆 MTD record: {count} plays, Net {profit:+.2f}u"

def get_alltime_record(guild_id):
    row = exec_safe("SELECT COUNT(*), SUM(result) FROM bets WHERE guild_id=%s", (guild_id,), fetch="one")
    if not row: return "No bets
