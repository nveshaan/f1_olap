-- CIRCUIT INFO --

CREATE TABLE circuits (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    name TEXT NOT NULL,
    rotation INTEGER NOT NULL
);

CREATE TABLE corners (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    circuit_id INTEGER NOT NULL,
    x FLOAT NOT NULL,
    y FLOAT NOT NULL,
    number INTEGER NOT NULL,
    letter TEXT,
    angle FLOAT NOT NULL,
    distance FLOAT NOT NULL,
    FOREIGN KEY (circuit_id) REFERENCES circuits(id)
);

CREATE TABLE marshal_lights (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    circuit_id INTEGER NOT NULL,
    x FLOAT NOT NULL,
    y FLOAT NOT NULL,
    number INTEGER NOT NULL,
    letter TEXT,
    angle FLOAT NOT NULL,
    distance FLOAT NOT NULL,
    FOREIGN KEY (circuit_id) REFERENCES circuits(id)
);

CREATE TABLE marshal_sectors (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    circuit_id INTEGER NOT NULL,
    x FLOAT NOT NULL,
    y FLOAT NOT NULL,
    number INTEGER NOT NULL,
    letter TEXT,
    angle FLOAT NOT NULL,
    distance FLOAT NOT NULL,
    FOREIGN KEY (circuit_id) REFERENCES circuits(id)
);

-- DRIVER & TEAM INFO --

CREATE TABLE drivers (
    id INTEGER PRIMARY KEY,
    name TEXT NOT NULL,
    broadcast_name TEXT,
    driver_number INTEGER NOT NULL,
    abbrevation TEXT NOT NULL,
    country TEXT,
    first_name TEXT,
    last_name TEXT
);

CREATE TABLE teams (
    id INTEGER PRIMARY KEY,
    name TEXT NOT NULL,
    color TEXT NOT NULL
);

-- SESSION & RESULTS --

CREATE TABLE results (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    driver_id INTEGER NOT NULL,
    team_id INTEGER NOT NULL,
    position INTEGER,
    classifies_position TEXT,
    grid_position TEXT,
    q1 DATETIME,
    q2 DATETIME,
    q3 DATETIME,
    time DATETIME,
    status TEXT,
    points INTEGER NOT NULL,
    laps INTEGER,
    FOREIGN KEY (driver_id) REFERENCES drivers(id),
    FOREIGN KEY (team_id) REFERENCES teams(id)
);

create TABLE sessions (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    event_name TEXT,
    session_name TEXT,
    date DATETIME,
    circuit_id INTEGER,
    result_id INTEGER,
    FOREIGN KEY (circuit_id) REFERENCES circuits(id),
    FOREIGN KEY (result_id) REFERENCES results(id)
);

CREATE TABLE weather (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    timestamp DATETIME,
    air_temp FLOAT,
    humidity FLOAT,
    pressure FLOAT,
    rainfall BOOLEAN,
    track_temp FLOAT,
    wind_direction INTEGER,
    wind_speed FLOAT,
    session_id INTEGER,
    FOREIGN KEY (session_id) REFERENCES sessions(id)
);

CREATE TABLE laps (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    timestamp DATETIME,
    session_id INTEGER,
    driver_id INTEGER,
    lap_number INTEGER,
    lap_time DATETIME,
    stint INTEGER,
    sector1_time DATETIME,
    sector2_time DATETIME,
    sector3_time DATETIME,
    speed1 FLOAT,
    speed2 FLOAT,
    speedFL FLOAT,
    compound TEXT,
    track_status TEXT,
    position INTEGER,
    pit_in_time DATETIME,
    pit_out_time DATETIME,
    FOREIGN KEY (session_id) REFERENCES sessions(id),
    FOREIGN KEY (driver_id) REFERENCES drivers(id)
);

CREATE TABLE telemetry (
    id INTEGER PRIMARY KEY AUTOINCREMENT
);