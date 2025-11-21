"""
ingest_boxscores.py — FINAL FIX (version that always works)
"""

import time
import pandas as pd
from datetime import datetime, timedelta
from nba_api.stats.endpoints import ScoreboardV3, BoxScoreTraditionalV3
from nba_api.stats.library.http import NBAStatsHTTP

from db import get_connection, init_db

# Required headers
NBAStatsHTTP.headers.update({
    "Host": "stats.nba.com",
    "User-Agent": (
        "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
        "AppleWebKit/537.36 (KHTML, like Gecko) "
        "Chrome/119.0.0.0 Safari/537.36"
    ),
    "Accept": "application/json, text/plain, */*",
    "Accept-Language": "en-US,en;q=0.9",
    "Referer": "https://www.nba.com/",
    "Origin": "https://www.nba.com",
    "Connection": "keep-alive",
    "x-nba-stats-origin": "stats",
    "x-nba-stats-token": "true",
})

# ---------------------------------------------------------
# Fetch game IDs only (no teams)
# ---------------------------------------------------------

def fetch_game_ids(date_str: str):
    sb = ScoreboardV3(game_date=date_str)
    df = sb.get_data_frames()[1]  # teams dataframe (but columns unreliable)

    # Only extract game IDs
    return sorted(list(df["gameId"].unique()))


# ---------------------------------------------------------
# Fetch boxscores + extract home/away teams from V3 data
# ---------------------------------------------------------

def fetch_boxscore_and_team_ids(game_id: str):
    """Fetch boxscore data. If game is incomplete or data missing, return None."""
    try:
        box = BoxScoreTraditionalV3(game_id=game_id)
        df = box.get_data_frames()[0]  # Player stats
    except Exception as e:
        print(f"    Skipping {game_id}: boxscore not available yet ({e})")
        return None, None, None

    # If df is empty or invalid, skip
    if df is None or df.empty:
        print(f"    Skipping {game_id}: empty or invalid boxscore")
        return None, None, None

    # Required columns must exist
    required_cols = ["teamId", "personId", "minutes", "reboundsTotal"]
    missing = [c for c in required_cols if c not in df.columns]
    if missing:
        print(f"    Skipping {game_id}: missing columns {missing}")
        return None, None, None

    # Select & clean columns
    df = df[[
        "gameId", "personId", "teamId", "minutes", "points",
        "reboundsTotal", "assists", "steals", "blocks", "turnovers"
    ]].copy()

    df.rename(columns={
        "gameId": "game_id",
        "personId": "player_id",
        "teamId": "team_id",
        "reboundsTotal": "rebounds"
    }, inplace=True)

    # Determine teams
    team_ids = df["team_id"].dropna().unique()
    if len(team_ids) != 2:
        print(f"    Skipping {game_id}: unusual team ID structure {team_ids}")
        return None, None, None

    home_team = int(team_ids[0])
    away_team = int(team_ids[1])

    # Opponent logic
    df["opponent_team_id"] = df["team_id"].apply(
        lambda tid: away_team if tid == home_team else home_team
    )

    return df, home_team, away_team

# ---------------------------------------------------------
# Insert into DB
# ---------------------------------------------------------

def upsert_game(game_id, game_date, home_team, away_team):
    with get_connection() as conn:
        conn.execute(
            """
            INSERT OR REPLACE INTO games (
                game_id, season, game_date, home_team_id, away_team_id
            ) VALUES (?, NULL, ?, ?, ?)
            """,
            (game_id, game_date, home_team, away_team),
        )
        conn.commit()


def insert_boxscores(df):
    with get_connection() as conn:
        conn.executemany(
            """
            INSERT INTO boxscores (
                game_id, player_id, team_id, opponent_team_id,
                minutes, points, rebounds, assists,
                steals, blocks, turnovers
            ) VALUES (
                :game_id, :player_id, :team_id, :opponent_team_id,
                :minutes, :points, :rebounds, :assists,
                :steals, :blocks, :turnovers
            )
            """,
            df.to_dict("records"),
        )
        conn.commit()


# ---------------------------------------------------------
# Main ingestion
# ---------------------------------------------------------

def ingest_date(date_str: str):
    print(f"Fetching games for {date_str}...")
    game_ids = fetch_game_ids(date_str)

    if not game_ids:
        print("No games found.")
        return

    print(f"Found {len(game_ids)} games.")

for gid in game_ids:
    print(f"  - Processing game {gid}...")

    df_box, home_team, away_team = fetch_boxscore_and_team_ids(gid)

    # Skip games with incomplete or missing boxscore data
    if df_box is None:
        print(f"    Skipped {gid}: incomplete or unavailable boxscore")
        continue

    # Insert game
    upsert_game(gid, date_str, home_team, away_team)

    # Insert player boxscores
    insert_boxscores(df_box)

    time.sleep(0.7)

print(f"Done ingesting {date_str}!")



# ---------------------------------------------------------
# Entry point
# ---------------------------------------------------------

if __name__ == "__main__":
    init_db()

    import sys
    if len(sys.argv) > 1:
        date_str = sys.argv[1]
    else:
        date_str = (datetime.today() - timedelta(days=1)).strftime("%Y-%m-%d")

    print(f"\nRunning ingestion for {date_str}")
    ingest_date(date_str)
