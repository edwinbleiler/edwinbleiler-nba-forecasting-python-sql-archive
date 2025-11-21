import pandas as pd
import numpy as np
from pathlib import Path

BASE_DIR = Path(__file__).resolve().parents[1]
FEATURE_PATH = BASE_DIR / "outputs" / "features.csv"
OUTPUT_PATH = BASE_DIR / "outputs" / "model_dataset.csv"
TRAIN_PATH = BASE_DIR / "outputs" / "train.csv"
TEST_PATH = BASE_DIR / "outputs" / "test.csv"

# ----------------------------------------------------------
# 1. Load Features
# ----------------------------------------------------------

def load_features():
    if not FEATURE_PATH.exists():
        raise FileNotFoundError(f"Missing features: {FEATURE_PATH}")
    return pd.read_csv(FEATURE_PATH, parse_dates=["game_date"])

# ----------------------------------------------------------
# 2. Clean + Filter
# ----------------------------------------------------------

def clean_df(df):
    print("Initial rows:", len(df))

    # Remove games with no minutes (did not play)
    df = df[df["minutes"] > 0]

    # Usually remove tiny-minute flukes (< 5)
    df = df[df["minutes"] >= 5]

    # Drop duplicates if ingestion somehow duplicated
    df = df.drop_duplicates(subset=["game_id", "player_id"])

    print("After cleaning:", len(df))
    return df

# ----------------------------------------------------------
# 3. Define Target + Features
# ----------------------------------------------------------

def select_columns(df):
    # Target prediction = DraftKings fantasy points
    target = "dk_fp"

    # Ignore IDs/metadata during modeling
    ignore_cols = [
        "game_id", "player_id", "team_id", "opponent_team_id",
        "game_date", "prev_game_id", "prev_game_date"
    ]

    # Feature candidates = all numeric columns except target
    feature_cols = [c for c in df.columns if c not in ignore_cols]

    # Sanity: remove completely-NA columns
    feature_cols = [
        c for c in feature_cols
        if not df[c].isna().all()
    ]

    return df, target, feature_cols

# ----------------------------------------------------------
# 4. Train/Test Split
# ----------------------------------------------------------

def train_test_split(df, cutoff_date=None):
    """
    If cutoff_date=None:
       → split by time (80% old → train, 20% recent → test)
    """

    df = df.sort_values("game_date")

    if cutoff_date is None:
        # 80% date-based split
        cutoff_index = int(len(df) * 0.80)
        cutoff_date = df.iloc[cutoff_index]["game_date"]

    train = df[df["game_date"] <= cutoff_date].copy()
    test = df[df["game_date"] > cutoff_date].copy()

    print("Train rows:", len(train))
    print("Test rows:", len(test))
    print("Cutoff date:", cutoff_date)

    return train, test, cutoff_date

# ----------------------------------------------------------
# 5. Save
# ----------------------------------------------------------

def save(train, test, full):
    full.to_csv(OUTPUT_PATH, index=False)
    train.to_csv(TRAIN_PATH, index=False)
    test.to_csv(TEST_PATH, index=False)

    print(f"Saved full model dataset → {OUTPUT_PATH}")
    print(f"Saved train → {TRAIN_PATH}")
    print(f"Saved test → {TEST_PATH}")

# ----------------------------------------------------------
# Main
# ----------------------------------------------------------

def main():
    print("Loading features...")
    df = load_features()

    print("Cleaning...")
    df = clean_df(df)

    print("Selecting features...")
    df, target, feature_cols = select_columns(df)

    print(f"Target column: {target}")
    print(f"Feature count: {len(feature_cols)}")

    print("Splitting train/test...")
    train, test, cutoff_date = train_test_split(df)

    print("Saving...")
    save(train, test, df)

    print("Model dataset build complete.")


if __name__ == "__main__":
    main()
