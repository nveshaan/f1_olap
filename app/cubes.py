def setup_cubes(con):
    """
    Creates the dimension and fact views for the 3 OLAP cubes over the base tables.
    """
    
    # 1. Race Performance Cube
    con.execute("""
        CREATE OR REPLACE TEMP VIEW dim_drivers AS 
        SELECT id as driver_key, name, abbrevation, country 
        FROM drivers;
    """)
    
    con.execute("""
        CREATE OR REPLACE TEMP VIEW dim_teams AS 
        SELECT id as team_key, name, color 
        FROM teams;
    """)
    
    con.execute("""
        CREATE OR REPLACE TEMP VIEW dim_sessions AS 
        SELECT id as session_key, session_name, event_name, date 
        FROM sessions;
    """)
    
    con.execute("""
        CREATE OR REPLACE TEMP VIEW dim_circuits AS 
        SELECT id as circuit_key, name, rotation 
        FROM circuits;
    """)
    
    con.execute("""
        CREATE OR REPLACE TEMP VIEW fact_race_results AS 
        SELECT 
            r.id as result_key, 
            r.driver_id as driver_key, 
            r.team_id as team_key, 
            r.session_id as session_key, 
            s.circuit_id as circuit_key, 
            TRY_CAST(r.points AS FLOAT) as points_scored, 
            TRY_CAST(r.position AS INTEGER) as classified_position,
            TRY_CAST(r.grid_position AS INTEGER) - TRY_CAST(r.position AS INTEGER) as position_gain, 
            TRY_CAST(r.laps AS INTEGER) as laps_completed
        FROM results r
        JOIN sessions s ON r.session_id = s.id;
    """)

    # 2. Lap & Tyre Strategy Cube
    con.execute("""
        CREATE OR REPLACE TEMP VIEW fact_laps AS
        SELECT 
            id as lap_key, 
            driver_id as driver_key, 
            session_id as session_key, 
            compound, 
            fresh_tyre,
            EXTRACT('epoch' FROM TRY_CAST(REPLACE(lap_time, 'days', 'day') AS INTERVAL)) as lap_time_sec, 
            EXTRACT('epoch' FROM TRY_CAST(REPLACE(sector1_time, 'days', 'day') AS INTERVAL)) as s1_time, 
            EXTRACT('epoch' FROM TRY_CAST(REPLACE(sector2_time, 'days', 'day') AS INTERVAL)) as s2_time, 
            EXTRACT('epoch' FROM TRY_CAST(REPLACE(sector3_time, 'days', 'day') AS INTERVAL)) as s3_time,
            TRY_CAST(tyre_life AS INTEGER) as tyre_life, 
            TRY_CAST(personal_best AS BOOLEAN) as is_personal_best,
            TRY_CAST(lap_number AS INTEGER) as lap_number,
            TRY_CAST(position AS INTEGER) as current_position,
            pit_in_time
        FROM laps;
    """)

    # 3. Telemetry & Physics Cube
    con.execute("""
        CREATE OR REPLACE TEMP VIEW dim_weather AS 
        SELECT id as weather_key, air_temp, track_temp, rainfall, session_id as session_key 
        FROM weather;
    """)
    
    con.execute("""
        CREATE OR REPLACE TEMP VIEW fact_telemetry AS
        SELECT 
            id as telemetry_key, 
            lap_id as lap_key, 
            TRY_CAST(speed AS FLOAT) as speed, 
            TRY_CAST(rpm AS FLOAT) as rpm, 
            TRY_CAST(throttle AS FLOAT) as throttle_pct, TRY_CAST(brake AS BOOLEAN) as brake_applied, TRY_CAST(ngear AS INTEGER) as gear, 
            TRY_CAST(distance AS FLOAT) as distance_in_lap, 
            drs as drs_status
        FROM telemetry;
    """)
