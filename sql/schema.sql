-- schema.sql
-- SQLite schema for NBA forecasting project

PRAGMA foreign_keys = ON;

-- Teams table
CREATE TABLE IF NOT EXISTS teams (
    team_id        INTEGER PRIMARY KEY,
    team_name      TEXT NOT NULL,
    team_abbrev    TEXT NOT NULL,
    team_nickname  TEXT,
    team_city      TEXT
);

-- Players table
CREATE TABLE IF NOT EXISTS players (
    player_id   INTEGER PRIMARY KEY,
    full_name   TEXT NOT NULL,
    first_name  TEXT,
    last_name   TEXT,
    is_active   INTEGER NOT NULL,
    -- team_id is nullable because players move teams; we can snapshot it later if needed
    team_id     INTEGER,
    FOREIGN KEY (team_id) REFERENCES teams(team_id)
);

-- Games table (skeleton for later phases)
CREATE TABLE IF NOT EXISTS games (
    game_id        TEXT PRIMARY KEY,
    season         TEXT,
    game_date      TEXT,
    home_team_id   INTEGER,
    away_team_id   INTEGER,
    FOREIGN KEY (home_team_id) REFERENCES teams(team_id),
    FOREIGN KEY (away_team_id) REFERENCES teams(team_id)
);

-- Boxscores table (skeleton for later phases)
CREATE TABLE IF NOT EXISTS boxscores (
    id               INTEGER PRIMARY KEY AUTOINCREMENT,
    game_id          TEXT NOT NULL,
    player_id        INTEGER NOT NULL,
    team_id          INTEGER NOT NULL,
    opponent_team_id INTEGER NOT NULL,
    minutes          REAL,
    points           INTEGER,
    rebounds         INTEGER,
    assists          INTEGER,
    steals           INTEGER,
    blocks           INTEGER,
    turnovers        INTEGER,
    FOREIGN KEY (game_id)   REFERENCES games(game_id),
    FOREIGN KEY (player_id) REFERENCES players(player_id),
    FOREIGN KEY (team_id)   REFERENCES teams(team_id),
    FOREIGN KEY (opponent_team_id) REFERENCES teams(team_id)
);

-- Simple index examples (can expand later)
CREATE INDEX IF NOT EXISTS idx_boxscores_player ON boxscores(player_id);
CREATE INDEX IF NOT EXISTS idx_boxscores_game   ON boxscores(game_id);
