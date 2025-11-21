"""
ingest_boxscores.py — FINAL STABLE VERSION (NBA API v3 Compatible)

Reliable ingestion pipeline:
- Pulls game IDs using ScoreboardV3
- Pulls full player-level boxscores using BoxScoreTraditionalV3
- Extracts home/away teams from boxscore (NBA API team table is unstable)
- Skips incomplete games gracefully
"""

import time
from datetime import datetime, timedelta
from nba_api.stats.endpoints import ScoreboardV3, BoxScoreTraditionalV3
from nba_api.stats.library.http import NBAStatsHTTP
import pandas as pd

from db import get_connection, init_db

# Required headers for NBA Stats API (browser-like)
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

# ------------------------------------------------------
# Fetch game IDs for a date
# ------------------------------------------------------

def fetch_game_ids(date_str: str):
    """
    Use ScoreboardV3 to get the list of game IDs.
    """
    sb = ScoreboardV3(game_date=date_str)
    df_list = sb.get_data_frames()
    if len(df_list) < 2:
        print(f"  No dashboard results for {date_str}")
        return []

    teams_df = df_list[1]  # contains gameId for each team row
    if "gameId" not in teams_df.columns:
        print(f"  Scoreboard missing gameId column for {date_str}")
        return []

    game_ids = sorted(teams_df["gameId"].dropna().unique().tolist())
    return game_ids


# ------------------------------------------------------
# Fetch boxscore and derive home/away teams
# ------------------------------------------------------

def fetch_boxscore_and_team_ids(game_id: str):
    """
    Fetch player-level boxscores using BoxScoreTraditionalV3.
    Safely returns (df, home_team_id, away_team_id) or (None, None, None).
    """

    try:
        box = BoxScoreTraditionalV3(game_id=game_id)
        df = box.get_data_frames()[0]
    except Exception as e:
        print(f"    Skipping {game_id}: Boxscore not available ({e})")
        return None, None, None

    if df is None or df.empty:
        print(f"    Skipping {game_id}: empty boxscore")
        return None, None, None

    # Required columns present?
    required = [
        "gameId", "personId", "teamId", "minutes",
        "points", "reboundsTotal", "assists", "steals",
        "blocks", "turnovers"
    ]
    missing = [c for c in required if c not in df.columns]
    if missing:
        print(f"    Skipping {game_id}: missing columns {missing}")
        return None, None, None

    # Select fields and clean names
    df = df[required].copy()
    df.rename(columns={
        "gameId": "game_id",
        "personId": "player_id",
        "teamId": "team_id",
        "reboundsTotal": "rebounds"
    }, inplace=True)

    # Identify teams (boxscore always contains correct team IDs)
    team_ids = df["team_id"].dropna().unique()
    if len(team_ids) != 2:
        print(f"    Skipping {game_id}: unusual team structure {team_ids}")
        return None, None, None

    home_team = int(team_ids[0])
    away_team = int(team_ids[1])

    # Compute opponent team id
    df["opponent_team_id"] = df["team_id"].apply(
        lambda tid: away_team if tid == home_team else home_team
    )

    return df, home_team, away_team


# ------------------------------------------------------
# DB writes
# ------------------------------------------------------

def upsert_game(game_id, date_str, home_team, away_team):
    """
    Insert or replace game metadata.
    """
    with get_connection() as conn:
        conn.execute(
            """
            INSERT OR REPLACE INTO games (
                game_id, season, game_date, home_team_id, away_team_id
            ) VALUES (?, NULL, ?, ?, ?)
            """,
            (game_id, date_str, home_team, away_team)
        )
        conn.commit()


def insert_boxscores(df):
    """
    Bulk insert player boxscores.
    """
    with get_connection() as conn:
        conn.executemany(
            """
            INSERT INTO boxscores (
                game_id, player_id, team_id, opponent_team_id,
                minutes, points, rebounds, assists,
                steals, blocks, turnovers
            )
            VALUES (
                :game_id, :player_id, :team_id, :opponent_team_id,
                :minutes, :points, :rebounds, :assists,
                :steals, :blocks, :turnovers
            )
            """,
            df.to_dict("records")
        )
        conn.commit()


# ------------------------------------------------------
# Main ingestion routine
# ------------------------------------------------------

def ingest_date(date_str: str):
    print(f"\nRunning ingestion for {date_str}")
    print(f"Fetching games for {date_str}...")

    game_ids = fetch_game_ids(date_str)
    if not game_ids:
        print("No games found.")
        return

    print(f"Found {len(game_ids)} games.")

    for gid in game_ids:
        print(f"  - Processing game {gid}...")

        df_box, home_team, away_team = fetch_boxscore_and_team_ids(gid)

        # Skip incomplete or bad boxscores
        if df_box is None:
            print(f"    Skipped {gid}: incomplete/missing data")
            continue

        # Insert game metadata
        upsert_game(gid, date_str, home_team, away_team)

        # Insert player stats
        insert_boxscores(df_box)

        time.sleep(0.7)  # rate-limit API calls

    print(f"Done ingesting {date_str}!")


# ------------------------------------------------------
# Script entry
# ------------------------------------------------------

if __name__ == "__main__":
    init_db()

    import sys
    if len(sys.argv) > 1:
        date_str = sys.argv[1]
    else:
        date_str = (datetime.today() - timedelta(days=1)).strftime("%Y-%m-%d")

    ingest_date(date_str)
