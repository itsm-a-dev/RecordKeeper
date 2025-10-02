# odds_api.py
import os, requests
from db import exec_safe, conn

ODDS_API_KEY = os.getenv("ODDS_API_KEY")

SPORT_API_MAP = {
    "nba": "basketball_nba",
    "nfl": "americanfootball_nfl",
    "mlb": "baseball_mlb",
    "nhl": "icehockey_nhl",
    "soccer": "soccer_usa_mls"
}

def cache_set_closing(guild_id, event_key, closing_line, closing_odds, source):
    exec_safe("""
        INSERT INTO closings (guild_id, event_key, closing_line, closing_odds, source, fetched_at)
        VALUES (%s, %s, %s, %s, %s, NOW())
    """, (guild_id, event_key, closing_line, closing_odds, source or "oddsapi"))
    conn.commit()

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
                for outcome in market["outcomes"]:
                    line = outcome.get("point")
                    odds = outcome.get("price")
                    if mkey == "spreads":
                        ek = f"{sport_key}|{date}|spread|{home}|{away}"
                    elif mkey == "totals":
                        ek = f"{sport_key}|{date}|total|{home}|{away}|{outcome['name'].lower()}"
                    elif mkey == "h2h":
                        ek = f"{sport_key}|{date}|moneyline|{home}|{away}|{outcome['name']}"
                    else:
                        ek = f"{sport_key}|{date}|prop|{outcome.get('description','')}"
                    cache_set_closing(guild_id, ek, line, odds, bm["title"])
                    inserted += 1
    return inserted
