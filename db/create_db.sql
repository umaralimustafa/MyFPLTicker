DROP DATABASE IF EXISTS :db_name;
CREATE DATABASE :db_name;
\c :db_name

GRANT ALL PRIVILEGES ON DATABASE :db_name TO :db_user;

-- teams
CREATE TABLE teams (
    code INTEGER PRIMARY KEY,
    draw INTEGER,
    form TEXT,
    id INTEGER,
    loss INTEGER,
    name TEXT,
    played INTEGER,
    points INTEGER,
    position INTEGER,
    short_name TEXT,
    strength INTEGER,
    team_division INTEGER,
    unavailable BOOLEAN,
    win INTEGER,
    strength_overall_home INTEGER,
    strength_overall_away INTEGER,
    strength_attack_home INTEGER,
    strength_attack_away INTEGER,
    strength_defence_home INTEGER,
    strength_defence_away INTEGER,
    pulse_id INTEGER
);