CREATE TABLE IF NOT EXISTS Games (
    id INTEGER PRIMARY KEY,
    datetime DATETIME,
    home_id INTEGER NOT NULL,
    away_id INTEGER NOT NULL,
    venue_id INTEGER NOT NULL,
    -- not sure 
    status_id INTEGER NOT NULL,
    tbd BOOLEAN NOT NULL,
    is_neutral_site BOOLEAN,
    is_conference BOOLEAN,
    has_shot_chart BOOLEAN,
    attendance INTEGER,
    FOREIGN KEY (home_id) REFERENCES Teams (id),
    FOREIGN KEY (away_id) REFERENCES Teams (id),
    FOREIGN KEY (venue_id) REFERENCES Venues (id),
    FOREIGN KEY (status_id) REFERENCES GameStatuses (id)
);

CREATE TABLE IF NOT EXISTS GameStatuses (
    id INTEGER PRIMARY KEY,
    state VARCHAR,
    detail VARCHAR
);

CREATE TABLE IF NOT EXISTS Venues (
    id INTEGER PRIMARY KEY,
    name VARCHAR NOT NULL,
    city VARCHAR,
    state VARCHAR
);

CREATE TABLE IF NOT EXISTS Teams (
    id INTEGER PRIMARY KEY,
    location VARCHAR NOT NULL,
    mascot VARCHAR NOT NULL,
    -- not sure
    abbrev VARCHAR NOT NULL,
    color VARCHAR(6),
    alt_color VARCHAR(6)
);

CREATE TABLE IF NOT EXISTS ConferenceAlignments (
    team_id INTEGER,
    conference_id INTEGER,
    season INTEGER,
    PRIMARY KEY (team_id, conference_id, season),
    FOREIGN KEY (team_id) REFERENCES Teams (id),
    FOREIGN KEY (conference_id) REFERENCES Conferences (id)
);

CREATE TABLE IF NOT EXISTS Conferences (
    id INTEGER PRIMARY KEY,
    name VARCHAR NOT NULL,
    abbrev VARCHAR NOT NULL
);

CREATE TABLE IF NOT EXISTS Plays (
    game_id INTEGER,
    sequence_id INTEGER,
    play_type_id INTEGER NOT NULL,
    points_attempted INTEGER NOT NULL CHECK (points_attempted <= 3),
    is_score BOOLEAN NOT NULL,
    team_id INTEGER,
    player_id INTEGER,
    assist_id INTEGER,
    period INTEGER,
    clock_minutes INTEGER CHECK (clock_minutes >= 0),
    clock_seconds INTEGER CHECK (clock_seconds >= 0),
    home_score INTEGER CHECK (home_score >= 0),
    away_score INTEGER CHECK (away_score >= 0),
    x_coord INTEGER,
    y_coord INTEGER,
    timestamp DATETIME,
    PRIMARY KEY (game_id, sequence_id),
    FOREIGN KEY (game_id) REFERENCES Games (id),
    FOREIGN KEY (play_type_id) REFERENCES PlayTypes (id),
    FOREIGN KEY (team_id) REFERENCES Teams (id),
    FOREIGN KEY (player_id) REFERENCES Players (id),
    FOREIGN KEY (assist_id) REFERENCES Players (id)
);

CREATE TABLE IF NOT EXISTS PlayTypes (
    id INTEGER PRIMARY KEY,
    description VARCHAR,
    is_shot BOOLEAN
);

CREATE TABLE IF NOT EXISTS Players (
    id INTEGER PRIMARY KEY,
    first_name VARCHAR NOT NULL,
    last_name VARCHAR NOT NULL,
    position VARCHAR(1),
    height_ft INTEGER,
    height_in INTEGER,
    weight INTEGER,
    birth_city VARCHAR,
    birth_state VARCHAR
);

CREATE TABLE IF NOT EXISTS PlayerSeasons (
    player_id INTEGER,
    team_id INTEGER NOT NULL,
    season INTEGER NOT NULL,
    jersey INTEGER,
    PRIMARY KEY (player_id, team_id, season),
    FOREIGN KEY (player_id) REFERENCES Players (id),
    FOREIGN KEY (team_id) REFERENCES Teams (id)
);

CREATE TABLE IF NOT EXISTS GameLogs (
    player_id INTEGER,
    game_id INTEGER,
    played BOOLEAN NOT NULL,
    started BOOLEAN NOT NULL,
    ejected BOOLEAN NOT NULL,
    -- TODO: should there be stats here
    PRIMARY KEY (player_id, game_id),
    FOREIGN KEY (player_id) REFERENCES Players (id),
    FOREIGN KEY (game_id) REFERENCES Teams (id)
);

-- FUTURE: AP Top 25 Ranking snapshots
-- FUTURE: Conference aliases?