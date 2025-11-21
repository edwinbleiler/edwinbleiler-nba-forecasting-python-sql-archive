import os
import subprocess
import sys
from datetime import datetime, timezone


def run_cmd(cmd, check=True):
    print(f"\n>>> {cmd}")
    subprocess.run(cmd, shell=True, check=check)


def main(date_override=None):
    # Decide which date to ingest
    if date_override:
        ingest_date = date_override
    else:
        ingest_date = "yesterday"

    # Control whether we even try to ingest "yesterday"
    skip_yesterday_backfill = os.getenv("SKIP_YESTERDAY_BACKFILL", "0") == "1"

    if ingest_date == "yesterday" and skip_yesterday_backfill:
        # In CI pre-game runs, we do NOT want to block on yesterday's boxscores
        print(
            f"[{datetime.now(timezone.utc)}] "
            f"Skipping boxscore ingest for 'yesterday' "
            f"(SKIP_YESTERDAY_BACKFILL=1)."
        )
    else:
        print(f"Running boxscore ingest for: {ingest_date}")
        try:
            # This is where ingest_boxscores can hang on stats.nba.com.
            # We let it fail without killing the rest of the pipeline.
            run_cmd(f"python src/ingest_boxscores.py {ingest_date}")
        except subprocess.CalledProcessError as e:
            print(
                f"[{datetime.now(timezone.utc)}] [WARN] "
                f"Boxscore ingest failed with exit code {e.returncode}: {e}. "
                f"Continuing to feature build and modeling."
            )

    # These steps should always run so you still get projections
    print("Building features...")
    run_cmd("python src/build_features_real.py")

    print("Building model dataset...")
    run_cmd("python src/build_model_dataset.py")

    print("Running minutes model...")
    run_cmd("python src/model_minutes.py")


if __name__ == "__main__":
    date_override = sys.argv[1] if len(sys.argv) > 1 else None
    main(date_override)
