-- Teams table
CREATE TABLE IF NOT EXISTS teams (
    team_id INTEGER PRIMARY KEY,
    full_name TEXT,
    abbreviation TEXT,
    conference TEXT,
    division TEXT
);

-- Players table
CREATE TABLE IF NOT EXISTS players (
    player_id INTEGER PRIMARY KEY,
    full_name TEXT,
    team_id INTEGER,
    position TEXT,
    FOREIGN KEY(team_id) REFERENCES teams(team_id)
);

-- Games table
CREATE TABLE IF NOT EXISTS games (
    game_id TEXT PRIMARY KEY,
    game_date TEXT,
    home_team_id INTEGER,
    away_team_id INTEGER,
    season INTEGER,
    FOREIGN KEY(home_team_id) REFERENCES teams(team_id),
    FOREIGN KEY(away_team_id) REFERENCES teams(team_id)
);

-- Box Scores table
CREATE TABLE IF NOT EXISTS boxscores (
    row_id INTEGER PRIMARY KEY AUTOINCREMENT,
    game_id TEXT,
    player_id INTEGER,
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
    free_throws_made INTEGER,
    FOREIGN KEY(game_id) REFERENCES games(game_id),
    FOREIGN KEY(player_id) REFERENCES players(player_id)
);
