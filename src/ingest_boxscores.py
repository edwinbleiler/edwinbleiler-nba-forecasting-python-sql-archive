"""
ingest_boxscores.py (V3 CLEAN FIX)

Fully functional ingestion using:
- ScoreboardV3 (games)
- BoxScoreTraditionalV3 (player stats)

Populates tables:
- games
- boxscores
"""

import time
import pandas as pd
from datetime import datetime
from pathlib import Path

from nba_api.stats.endpoints import ScoreboardV3, BoxScoreTraditionalV3
from nba_api.stats.library.http import NBAStatsHTTP

from db import get_connection, init_db


# --- REQUIRED HEADERS FOR NBA API ---
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


# ====================================================================
# GAME FETCHING USING ScoreboardV3
# ====================================================================

def fetch_games_for_date(date_str: str) -> pd.DataFrame:
    """
    Fetch games for a date using ScoreboardV3.

    Games are determined from teams_df (DataFrame #1),
    which contains home/away pairs with gameId + teamId.
    """

    sb = ScoreboardV3(game_date=date_str)

    # DataFrame 0: metadata (contains date, league)
    meta_df = sb.get_data_frames()[0]

    # DataFrame 1: team matchups (contains teamId, gameId, home/away rows)
    teams_df = sb.get_data_frames()[1]

    games = []

    # Teams come in pairs: home (0), away (1), next home (2), away (3), etc.
    for i in range(0, len(teams_df), 2):
        home = teams_df.iloc[i]
        away = teams_df.iloc[i + 1]

        games.append({
            "game_id": home["gameId"],
            "season": None,  # V3 doesn't provide season, this is optional anyway
            "game_date": meta_df["gameDate"].iloc[0],
            "home_team_id": int(home["teamId"]),
            "away_team_id": int(away["teamId"]),
        })

    return pd.DataFrame(games)


# ====================================================================
# BOX SCORE FETCHING USING BoxScoreTraditionalV3
# ====================================================================

def fetch_boxscores(game_id: str) -> pd.DataFrame:
    """
    Fetch player-level boxscores for a single game using V3.
    """

    box = BoxScoreTraditionalV3(game_id=game_id)
    df = box.get_data_frames()[0]  # This is the "player stats" DataFrame

    # We keep ONLY columns that actually exist in V3
    df = df[[
        "gameId",
        "personId",
        "teamId",
        "minutes",
        "points",
        "reboundsTotal",
        "assists",
        "steals",
        "blocks",
        "turnovers",
    ]].copy()

    # Rename to internal schema
    df.rename(columns={
        "gameId": "game_id",
        "personId": "player_id",
        "teamId": "team_id",
        "reboundsTotal": "rebounds",
    }, inplace=True)

    # V3 does NOT include opponent team → we compute in SQL later
    df["opponent_team_id"] = None

    return df


# ====================================================================
# DB WRITES
# ====================================================================

def upsert_games(df_games: pd.DataFrame):
    with get_connection() as conn:
        cursor = conn.cursor()
        cursor.executemany("""
            INSERT OR REPLACE INTO games (
                game_id, season, game_date, home_team_id, away_team_id
            ) VALUES (
                :game_id, :season, :game_date, :home_team_id, :away_team_id
            );
        """, df_games.to_dict("records"))
        conn.commit()


def insert_boxscores(df_box: pd.DataFrame):
    with get_connection() as conn:
        cursor = conn.cursor()
        cursor.executemany("""
            INSERT INTO boxscores (
                game_id, player_id, team_id, opponent_team_id,
                minutes, points, rebounds, assists,
                steals, blocks, turnovers
            ) VALUES (
                :game_id, :player_id, :team_id, :opponent_team_id,
                :minutes, :points, :rebounds, :assists,
                :steals, :blocks, :turnovers
            );
        """, df_box.to_dict("records"))
        conn.commit()


# ====================================================================
# MAIN INGEST
# ====================================================================

def ingest_date(date_str: str):
    print(f"Fetching games for {date_str}...")
    games = fetch_games_for_date(date_str)

    if games.empty:
        print("No games found.")
        return

    print(f"Found {len(games)} games. Inserting into DB...")
    upsert_games(games)

    print("Fetching boxscores...")
    for game_id in games["game_id"]:
        print(f"  - Game {game_id}")

        df_box = fetch_boxscores(game_id)
        insert_boxscores(df_box)

        time.sleep(0.7)  # polite to NBA API

    print(f"Ingestion complete for {date_str}")


# ====================================================================
# ENTRY POINT
# ====================================================================

if __name__ == "__main__":
    import sys
    from datetime import timedelta

    init_db()

    if len(sys.argv) > 1:
        date_str = sys.argv[1]
    else:
        date_str = (datetime.today() - timedelta(days=1)).strftime("%Y-%m-%d")

    print(f"\nRunning ingestion for {date_str}")
    ingest_date(date_str)
