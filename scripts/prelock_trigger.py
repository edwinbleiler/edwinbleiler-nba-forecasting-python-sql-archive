import requests
from nba_api.stats.endpoints import ScoreboardV3
from datetime import datetime, timedelta
import pytz
import os
import json

# Constants
EVENT_LOG = "prelock_events.json"
MAIN_WORKFLOW = "pipeline.yaml"  # main workflow file name
REPO = os.environ["REPO"]
TOKEN = os.environ["GH_TOKEN"]

EST = pytz.timezone("America/New_York")
UTC = pytz.utc

def load_event_log():
    """Track which games we've already triggered today."""
    if os.path.exists(EVENT_LOG):
        with open(EVENT_LOG, "r") as f:
            return json.load(f)
    return {}

def save_event_log(log):
    with open(EVENT_LOG, "w") as f:
        json.dump(log, f, indent=2)

def trigger_main_pipeline(game_id, tipoff):
    """Dispatch the main pipeline workflow."""
    url = f"https://api.github.com/repos/{REPO}/actions/workflows/{MAIN_WORKFLOW}/dispatches"

    headers = {
        "Accept": "application/vnd.github+json",
        "Authorization": f"Bearer {TOKEN}"
    }

    data = {
        "ref": "main",
        "inputs": {
            "game_id": game_id,
            "tipoff": tipoff
        }
    }

    r = requests.post(url, headers=headers, json=data)
    print(f"Triggered workflow for game {game_id}: status={r.status_code}")
    print(r.text)

def main():
    today_str = datetime.now(EST).strftime("%Y-%m-%d")

    sb = ScoreboardV3(game_date=today_str)
    games_df = sb.get_data_frames()[0]  # meta table

    log = load_event_log()

    now_utc = datetime.now(UTC)

    for _, row in games_df.iterrows():
        game_id = row["gameId"]
        tip = row.get("gameTimeUTC") or row.get("gameTime") or None

        if tip is None:
            continue

        # Parse tipoff time (UTC ISO string)
        try:
            tipoff_utc = datetime.fromisoformat(tip.replace("Z", "+00:00"))
        except Exception:
            continue

        # Convert to UTC datetime
        tipoff_utc = tipoff_utc.astimezone(UTC)

        # Check if we already triggered this game today
        if game_id in log.get(today_str, []):
            continue

        # Check window: 30 minutes before tip to tipoff
        if now_utc >= tipoff_utc - timedelta(minutes=30) and now_utc < tipoff_utc:
            print(f"Triggering prelock for game {game_id} (tipoff={tipoff_utc})")
            trigger_main_pipeline(game_id, str(tipoff_utc))

            # Log trigger
            if today_str not in log:
                log[today_str] = []
            log[today_str].append(game_id)
            save_event_log(log)

if __name__ == "__main__":
    main()
