# src/model_stats.py

"""
model_stats.py

Trains RandomForest models for:
    - points
    - rebounds
    - assists
    - fantasy_points

Saves models under models/ directory.
"""

from pathlib import Path

import pandas as pd
from sklearn.ensemble import RandomForestRegressor
from sklearn.model_selection import train_test_split
import joblib

BASE_DIR = Path(__file__).resolve().parents[1]
PROCESSED_DIR = BASE_DIR / "data" / "processed"
MODELS_DIR = BASE_DIR / "models"
MODELS_DIR.mkdir(parents=True, exist_ok=True)


def train_and_save(df: pd.DataFrame, target: str, feature_cols, model_name: str):
    X = df[feature_cols]
    y = df[target]

    X_train, X_test, y_train, y_test = train_test_split(
        X, y, test_size=0.2, random_state=42
    )

    model = RandomForestRegressor(
        n_estimators=200, max_depth=10, random_state=42, n_jobs=-1
    )
    model.fit(X_train, y_train)

    train_score = model.score(X_train, y_train)
    test_score = model.score(X_test, y_test)

    print(f"{target} model R^2 - train: {train_score:.3f}, test: {test_score:.3f}")

    model_path = MODELS_DIR / f"{model_name}.pkl"
    joblib.dump(model, model_path)
    print(f"Saved {target} model to {model_path}")


def main():
    data_path = PROCESSED_DIR / "model_dataset.csv"
    df = pd.read_csv(data_path, parse_dates=["game_date"])

    feature_cols = [
        "minutes_last_5",
        "minutes_last_10",
        "minutes_last_20",
        "fppg_last_5",
        "fppg_last_10",
        "fppg_last_20",
        "points_last_10",
        "rebounds_last_10",
        "assists_last_10",
        "usage_proxy",
        "dvp_last_20",
    ]

    for target, name in [
        ("points", "points_model"),
        ("rebounds", "rebounds_model"),
        ("assists", "assists_model"),
        ("fantasy_points", "fantasy_model"),
    ]:
        train_and_save(df, target, feature_cols, name)


if __name__ == "__main__":
    main()
