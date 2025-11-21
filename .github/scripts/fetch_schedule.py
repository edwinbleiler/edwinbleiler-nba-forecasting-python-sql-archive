import requests
import datetime
import pytz

def get_today_games():
    today = datetime.datetime.now(pytz.timezone("US/Eastern")).strftime("%Y-%m-%d")
    url = f"https://cdn.nba.com/static/json/staticData/scheduleLeagueV2.json"

    r = requests.get(url, timeout=10)
    data = r.json()

    games_today = []

    for game in data["leagueSchedule"]["gameDates"]:
        if game["gameDate"] == today:
            for g in game["games"]:
                if g["gameStatus"] == 1:  # scheduled, not started
                    tip = g["gameTimeUTC"]
                    games_today.append({
                        "game_id": g["gameId"],
                        "tipoff_utc": tip,
                        "date": today
                    })

    return games_today

if __name__ == "__main__":
    print(get_today_games())
