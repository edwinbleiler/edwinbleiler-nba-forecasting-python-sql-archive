"""
build_features_real.py

Build machine-learning features from the SQLite database.
Loads:
    - games
    - boxscores

Outputs:
    - /outputs/features.csv
"""

import pandas as pd
import numpy as np
from pathlib import Path
from db import get_connection, init_db
from math import radians, cos, sin, asin, sqrt

# ---------------------------------------------------------
# Correct Repo Root
# ---------------------------------------------------------
BASE_DIR = Path(__file__).resolve().parents[1]
OUTPUT_DIR = BASE_DIR / "outputs"
OUTPUT_DIR.mkdir(exist_ok=True, parents=True)

FEATURE_OUTPUT = OUTPUT_DIR / "features.csv"

# ---------------------------------------------------------
# Team coordinates (approx arenas)
# ---------------------------------------------------------
TEAM_LOCATIONS = {
    1610612737: (33.7573, -84.3963),
    1610612738: (42.3663, -71.0622),
    1610612739: (41.4965, -81.6882),
    1610612740: (29.9490, -90.0821),
    1610612741: (41.8807, -87.6742),
    1610612742: (32.7905, -96.8104),
    1610612743: (39.7487, -104.9955),
    1610612744: (37.7680, -122.3877),
    1610612745: (29.7508, -95.3621),
    1610612746: (38.8981, -77.0209),
    1610612747: (34.0430, -118.2673),
    1610612748: (25.7814, -80.1870),
    1610612749: (43.0436, -87.9090),
    1610612750: (44.9795, -93.2760),
    1610612751: (40.6827, -73.9757),
    1610612752: (40.7505, -73.9934),
    1610612753: (28.5392, -81.3839),
    1610612754: (39.7684, -86.1581),
    1610612755: (39.9012, -75.1720),
    1610612756: (33.4457, -112.0712),
    1610612757: (45.5316, -122.6668),
    1610612758: (38.5802, -121.4997),
    1610612759: (35.4634, -97.5151),
    1610612760: (43.6435, -79.3791),
    1610612762: (35.1382, -90.0506),
    1610612764: (38.8981, -77.0209),
    1610612765: (42.3411, -83.0553),
    1610612766: (35.2271, -80.8431),
}

# ---------------------------------------------------------
# Travel calculation
# ---------------------------------------------------------
def haversine(lat1, lon1, lat2, lon2):
    R = 6371  # km
    d_lat = radians(lat2 - lat1)
    d_lon = radians(lon2 - lon1)
    a = sin(d_lat/2)**2 + cos(radians(lat1))*cos(radians(lat2))*sin(d_lon/2)**2
    return 2 * R * asin(sqrt(a))

# ---------------------------------------------------------
# Minute parser
# ---------------------------------------------------------
def parse_minutes(m):
    """Convert 'mm:ss' to float minutes."""
    if m is None or pd.isna(m):
        return 0
    if isinstance(m, (int, float)):
        return float(m)
    try:
        mins, secs = map(int, str(m).split(":"))
        return mins + secs / 60
    except:
        return 0

# ---------------------------------------------------------
def load_raw():
    with get_connection() as conn:
        games = pd.read_sql("SELECT * FROM games", conn, parse_dates=["game_date"])
        box = pd.read_sql("SELECT * FROM boxscores", conn)
    return games, box

# ---------------------------------------------------------
def build_features():
    print("Loading raw DB data...")
    games, box = load_raw()

    print("Cleaning data...")

    df = box.merge(games, on="game_id", how="left")

    # --- Fix minutes column ---
    df["minutes"] = df["minutes"].apply(parse_minutes)

    df = df.sort_values(["player_id", "game_date"])

    # -------------------- Fantasy Points --------------------
    df["fantasy_points"] = (
        df["points"]
        + df["rebounds"] * 1.2
        + df["assists"] * 1.5
        + df["steals"] * 3
        + df["blocks"] * 3
        - df["turnovers"]
    )

    print("Computing rolling stats...")
    df["fp_last_5"] = df.groupby("player_id")["fantasy_points"].rolling(5).mean().reset_index(0, drop=True)
    df["fp_last_10"] = df.groupby("player_id")["fantasy_points"].rolling(10).mean().reset_index(0, drop=True)
    df["minutes_last_5"] = df.groupby("player_id")["minutes"].rolling(5).mean().reset_index(0, drop=True)

    # -------------------- Rest Days --------------------
    print("Computing rest days...")
    df["prev_game_date"] = df.groupby("team_id")["game_date"].shift(1)
    df["days_rest"] = (df["game_date"] - df["prev_game_date"]).dt.days

    # -------------------- Travel Distance --------------------
    print("Computing travel...")

    def compute_travel(row):
        team = row["team_id"]
        prev_date = row["prev_game_date"]

        if pd.isna(prev_date):
            return 0

        lat1, lon1 = TEAM_LOCATIONS.get(team, (0, 0))
        lat2, lon2 = TEAM_LOCATIONS.get(team, (0, 0))

        return haversine(lat1, lon1, lat2, lon2)

    df["travel_km"] = df.apply(compute_travel, axis=1)

    # -------------------- Save Output --------------------
    print(f"Saving features → {FEATURE_OUTPUT}")
    df.to_csv(FEATURE_OUTPUT, index=False)
    print("Done!")

# ---------------------------------------------------------
if __name__ == "__main__":
    init_db()
    build_features()
