import requests
from nba_api.stats.endpoints import ScoreboardV3
from datetime import datetime, timedelta
import pytz
import os
import json
from pathlib import Path

# --- Paths ---
EVENT_LOG = Path("scripts/logs/prelock_events.json")
LOG_FILE = Path("scripts/logs/prelock_log.txt")

# --- Workflow / Repo ---
MAIN_WORKFLOW = "pipeline.yaml"
REPO = os.environ["REPO"]
TOKEN = os.environ["GH_TOKEN"]

# --- Timezones ---
EST = pytz.timezone("America/New_York")
UTC = pytz.utc


# -------------------------
#  Logging Helpers
# -------------------------
def log(msg: str):
    """Append timestamped logs to both console and log file."""
    ts = datetime.now(UTC).strftime("%Y-%m-%d %H:%M:%S UTC")
    line = f"[{ts}] {msg}"
    print(line)

    LOG_FILE.parent.mkdir(parents=True, exist_ok=True)
    with open(LOG_FILE, "a") as f:
        f.write(line + "\n")


# -------------------------
#  Persistent Event Log
# -------------------------
def load_event_log():
    if EVENT_LOG.exists():
        with open(EVENT_LOG, "r") as f:
            return json.load(f)
    return {}

def save_event_log(log):
    EVENT_LOG.parent.mkdir(parents=True, exist_ok=True)
    with open(EVENT_LOG, "w") as f:
        json.dump(log, f, indent=2)


# -------------------------
#  Trigger Pipeline Run
# -------------------------
def trigger_main_pipeline(game_id, tipoff):
    url = (
        f"https://api.github.com/repos/{REPO}/actions/workflows/"
        f"{MAIN_WORKFLOW}/dispatches"
    )

    headers = {
        "Accept": "application/vnd.github+json",
        "Authorization": f"Bearer {TOKEN}",
    }

    data = {
        "ref": "main",
        "inputs": {
            "game_id": game_id,
            "tipoff": tipoff,
        },
    }

    r = requests.post(url, headers=headers, json=data)
    log(f"Triggered pipeline for game {game_id}, HTTP {r.status_code}")
    if r.text:
        log(r.text)


# -------------------------
#  Main Logic
# -------------------------
def main():
    today_str = datetime.now(EST).strftime("%Y-%m-%d")
    log(f"Running pre-lock scan for {today_str}")

    # Load scheduler history
    log_dict = load_event_log()

    # Get NBA schedule
    sb = ScoreboardV3(game_date=today_str)
    meta_df = sb.get_data_frames()[0]

    now_utc = datetime.now(UTC)

    for _, row in meta_df.iterrows():
        game_id = row.get("gameId")
        tip = row.get("gameTimeUTC")

        if not tip:
            continue

        # Convert tipoff string to UTC datetime
        try:
            tipoff_utc = datetime.fromisoformat(tip.replace("Z", "+00:00"))
        except:
            continue

        # Skip if already triggered
        if game_id in log_dict.get(today_str, []):
            continue

        # Window: between 30 min before tipoff and tipoff
        if now_utc >= tipoff_utc - timedelta(minutes=30) and now_utc < tipoff_utc:
            log(f"Triggering game {game_id}, tipoff={tipoff_utc}")

            trigger_main_pipeline(game_id, str(tipoff_utc))

            # Update log
            log_dict.setdefault(today_str, []).append(game_id)
            save_event_log(log_dict)

    log("Scan complete.")


if __name__ == "__main__":
    main()
