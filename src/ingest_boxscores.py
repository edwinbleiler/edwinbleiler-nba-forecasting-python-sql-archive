#!/usr/bin/env python3
import time
import random
import sqlite3
import pandas as pd
from datetime import datetime
from nba_api.stats.endpoints import ScoreboardV3, BoxScoreTraditionalV3
from nba_api.stats.library.http import NBAStatsHTTP


DB_PATH = "data/nba_forecasting.db"


# ============================================================
# Logging Helpers
# ============================================================

def log(msg: str):
    ts = datetime.utcnow().strftime("%Y-%m-%d %H:%M:%S")
    print(f"[{ts} UTC] {msg}", flush=True)


# ============================================================
# Retry Wrappers
# ============================================================

def retry_api_call(func, *args, retries=6, base_delay=2, **kwargs):
    """
    Generic retry wrapper for NBA API calls.
    """
    for attempt in range(1, retries + 1):
        try:
            return func(*args, **kwargs)
        except Exception as e:
            wait = base_delay * attempt + random.random() * 2
            log(f"[WARN] API failure (attempt {attempt}/{retries}): {e}. Retrying in {wait:.1f}s...")
            time.sleep(wait)

    raise RuntimeError(f"API call failed after {retries} retries.")


def safe_scoreboard(game_date):
    return retry_api_call(ScoreboardV3, game_date=game_date)


def safe_boxscore(game_id):
    return retry_api_call(BoxScoreTraditionalV3, game_id=game_id)


# ============================================================
# Database Helpers
# ============================================================

def init_db():
    con = sqlite3.connect(DB_PATH)
    cur = con.cursor()

    # Games table
    cur.execute("""
        CREATE TABLE IF NOT EXISTS games (
            game_id TEXT PRIMARY KEY,
            game_date TEXT,
            home_team_id INTEGER,
            away_team_id INTEGER
        );
    """)

    # Boxscores table
    cur.execute("""
        CREATE TABLE IF NOT EXISTS boxscores (
            game_id TEXT,
            player_id INTEGER,
            team_id INTEGER,
            minutes REAL,
            points INTEGER,
            rebounds INTEGER,
            assists INTEGER,
            steals INTEGER,
            blocks INTEGER,
            turnovers INTEGER,
            field_goals_made INTEGER,
            field_goals_attempted INTEGER,
            three_points_made INTEGER,
            three_points_attempted INTEGER,
            free_throws_made INTEGER,
            free_throws_attempted INTEGER,
            dk_fp REAL,
            PRIMARY KEY (game_id, player_id)
        );
    """)

    con.commit()
    con.close()


# ============================================================
# Fetching Functions
# ============================================================

def fetch_game_ids(date_str):
    log(f"Fetching games for {date_str}...")
    sb = safe_scoreboard(date_str)
    meta, linescore, *_ = sb.get_data_frames()

    if "gameId" not in linescore.columns:
        raise RuntimeError(f"ScoreboardV3 did not return game list for {date_str}")

    game_ids = list(linescore["gameId"].unique())
    log(f"Found {len(game_ids)} games.")
    return game_ids, linescore


def fetch_boxscore_and_teams(game_id):
    log(f"  - Fetching boxscore {game_id}")
    box = safe_boxscore(game_id)

    dfs = box.get_data_frames()
    if len(dfs) == 0:
        raise RuntimeError(f"No boxscore data for game {game_id}")

    df = dfs[0]

    # Identify home/away teams
    teams = df[["teamId"]].drop_duplicates()["teamId"].tolist()
    if len(teams) != 2:
        raise RuntimeError(f"Invalid number of teams in boxscore: {teams}")

    return df, teams[0], teams[1]


# ============================================================
# DB Insert Functions
# ============================================================

def upsert_game(game_id, date_str, home_team, away_team):
    con = sqlite3.connect(DB_PATH)
    cur = con.cursor()
    cur.execute("""
        INSERT OR REPLACE INTO games (game_id, game_date, home_team_id, away_team_id)
        VALUES (?, ?, ?, ?);
    """, (game_id, date_str, home_team, away_team))
    con.commit()
    con.close()


def insert_boxscores(df):
    con = sqlite3.connect(DB_PATH)
    df = df.copy()

    # Convert "minutes" to float (if "MM:SS" convert → decimal)
    def parse_minutes(val):
        if isinstance(val, str) and ":" in val:
            mm, ss = val.split(":")
            return int(mm) + int(ss) / 60
        return float(val)

    df["minutes"] = df["minutes"].apply(parse_minutes)

    # Compute DraftKings FP
    df["dk_fp"] = (
        df["points"]
        + 1.25 * df["rebounds"]
        + 1.5 * df["assists"]
        + 2 * df["steals"]
        + 2 * df["blocks"]
        - df["turnovers"]
    )

    df.to_sql("boxscores", con, if_exists="append", index=False)
    con.close()


# ============================================================
# Master Ingestion Function
# ============================================================

def ingest_date(date_str):
    init_db()
    game_ids, meta = fetch_game_ids(date_str)

    for gid in game_ids:
        try:
            df_box, t1, t2 = fetch_boxscore_and_teams(gid)
            upsert_game(gid, date_str, t1, t2)
            insert_boxscores(df_box)
            time.sleep(0.6)

        except Exception as e:
            log(f"[ERROR] Failed to process game {gid}: {e}")

    log(f"Done ingesting {date_str}!")


# ============================================================
# Main Entry
# ============================================================

if __name__ == "__main__":
    import sys
    if len(sys.argv) < 2:
        raise RuntimeError("Usage: python ingest_boxscores.py YYYY-MM-DD")

    ingest_date(sys.argv[1])
