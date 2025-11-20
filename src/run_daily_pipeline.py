# src/run_daily_pipeline.py

"""
run_daily_pipeline.py

End-to-end pipeline runner for GitHub Actions:

1) Ingest yesterday's boxscores
2) Build real features
3) Build modeling dataset
4) Train minutes model
5) Train stats models (pts/reb/ast/fp)
6) Generate projections for today
"""

from datetime import datetime, timedelta
import subprocess
import sys
from pathlib import Path

BASE_DIR = Path(__file__).resolve().parents[1]


def run(cmd: list[str]):
    print(f"\n>>> Running: {' '.join(cmd)}")
    result = subprocess.run(cmd, cwd=BASE_DIR, capture_output=True, text=True)
    print(result.stdout)
    if result.returncode != 0:
        print(result.stderr, file=sys.stderr)
        raise SystemExit(result.returncode)


def main():
    today = datetime.today().date()
    yesterday = today - timedelta(days=1)

    # 1) Ingest yesterday's games
    run(["python", "src/ingest_boxscores.py", yesterday.strftime("%Y-%m-%d")])

    # 2) Build real features
    run(["python", "src/build_features_real.py"])

    # 3) Build modeling dataset
    run(["python", "src/build_model_dataset.py"])

    # 4) Train minutes model
    run(["python", "src/model_minutes.py"])

    # 5) Train stats models
    run(["python", "src/model_stats.py"])

    # 6) Generate projections for today
    run(["python", "src/projection_engine.py", today.strftime("%Y-%m-%d")])


if __name__ == "__main__":
    main()
