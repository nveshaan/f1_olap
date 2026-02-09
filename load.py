import fastf1
import pandas as pd
from datetime import timedelta
from tqdm import tqdm

from db import init_db, get_db

# Enable FastF1 cache for faster loading
fastf1.Cache.enable_cache('cache')


def load_circuit_info(session, db):
    """Load circuit information including corners and marshal points."""
    circuit_info = session.get_circuit_info()

    # check if circuit already exists
    cursor = db.cursor()
    cursor.execute("SELECT id FROM circuits WHERE name = ?", (session.event['Location'],))
    existing_circuit = cursor.fetchone()
    if existing_circuit is not None:
        print(f"  Circuit {session.event['Location']} already exists in database, skipping circuit info")
        return existing_circuit[0]

    # Insert circuit
    cursor = db.cursor()
    cursor.execute("""
        INSERT INTO circuits (name, rotation)
        VALUES (?, ?)
    """, (session.event['Location'], circuit_info.rotation))
    circuit_id = cursor.lastrowid

    # Insert corners
    if hasattr(circuit_info, 'corners') and circuit_info.corners is not None:
        for _, corner in circuit_info.corners.iterrows():
            cursor.execute("""
                INSERT INTO corners (circuit_id, x, y, number, letter, angle, distance)
                VALUES (?, ?, ?, ?, ?, ?, ?)
            """, (
                circuit_id,
                float(corner['X']),
                float(corner['Y']),
                int(corner['Number']),
                corner.get('Letter'),
                float(corner['Angle']),
                float(corner['Distance'])
            ))

    # Insert marshal lights
    if hasattr(circuit_info, 'marshal_lights') and circuit_info.marshal_lights is not None:
        for _, light in circuit_info.marshal_lights.iterrows():
            cursor.execute("""
                INSERT INTO marshal_lights (circuit_id, x, y, number, letter, angle, distance)
                VALUES (?, ?, ?, ?, ?, ?, ?)
            """, (
                circuit_id,
                float(light['X']),
                float(light['Y']),
                int(light['Number']),
                light.get('Letter'),
                float(light['Angle']),
                float(light['Distance'])
            ))

    # Insert marshal sectors
    if hasattr(circuit_info, 'marshal_sectors') and circuit_info.marshal_sectors is not None:
        for _, sector in circuit_info.marshal_sectors.iterrows():
            cursor.execute("""
                INSERT INTO marshal_sectors (circuit_id, x, y, number, letter, angle, distance)
                VALUES (?, ?, ?, ?, ?, ?, ?)
            """, (
                circuit_id,
                float(sector['X']),
                float(sector['Y']),
                int(sector['Number']),
                sector.get('Letter'),
                float(sector['Angle']),
                float(sector['Distance'])
            ))

    db.commit()
    return circuit_id


def load_drivers_and_teams(session, db):
    """Load driver and team information from session."""
    cursor = db.cursor()

    for _, driver_info in session.results.iterrows():
        # check if driver already exists
        cursor.execute("SELECT id FROM drivers WHERE driver_number = ?", (driver_info['DriverNumber'],))
        existing_driver = cursor.fetchone()
        if existing_driver is None:
            # Insert driver
            cursor.execute("""
                INSERT OR REPLACE INTO drivers
                (name, broadcast_name, driver_number, abbrevation, country, first_name, last_name)
                VALUES (?, ?, ?, ?, ?, ?, ?)
            """, (
                driver_info['FullName'],
                driver_info.get('BroadcastName'),
                int(driver_info['DriverNumber']),
                driver_info['Abbreviation'],
                driver_info.get('CountryCode'),
                driver_info.get('FirstName'),
                driver_info.get('LastName')
            ))

        # check if team already exists
        cursor.execute("SELECT id FROM teams WHERE name = ?", (driver_info['TeamName'],))
        existing_team = cursor.fetchone()
        if existing_team is None:
            # Insert team
            team_name = driver_info['TeamName']
            cursor.execute("""
                INSERT OR REPLACE INTO teams (name, color)
                VALUES (?, ?)
            """, (
                team_name,
                driver_info.get('TeamColor', '#FFFFFF')
            ))

    db.commit()
    return


def load_session(session, circuit_id, db):
    """Load session metadata."""
    cursor = db.cursor()
    cursor.execute("""
        INSERT INTO sessions (event_name, session_name, date, circuit_id)
        VALUES (?, ?, ?, ?)
    """, (
        session.event['EventName'],
        session.name,
        session.date.isoformat() if session.date else None,
        circuit_id,
    ))
    session_id = cursor.lastrowid
    db.commit()
    return session_id


def load_results(session, session_id, db):
    """Load session results."""
    cursor = db.cursor()

    for _, result in session.results.iterrows():
        cursor.execute("SELECT id FROM drivers WHERE driver_number = ?", (result['DriverNumber'],))
        driver_row = cursor.fetchone()
        if driver_row is None:
            print(f"  Warning: Driver {result['DriverNumber']} not found in database, skipping result")
            continue
        driver_id = driver_row[0]
        cursor.execute("SELECT id FROM teams WHERE name = ?", (result['TeamName'],))
        team_row = cursor.fetchone()
        if team_row is None:
            print(f"  Warning: Team {result['TeamName']} not found in database, skipping result")
            continue
        team_id = team_row[0]
        # session id

        def safe_int(val):
            if pd.notna(val) and val != '':
                return int(val)
            return None

        cursor.execute("""
            INSERT INTO results
            (driver_id, team_id, session_id, position, classified_position, grid_position,
             q1, q2, q3, time, status, points, laps)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """, (
            driver_id,
            team_id,
            session_id,
            safe_int(result.get('Position')),
            result.get('ClassifiedPosition'),
            safe_int(result.get('GridPosition')),
            str(result.get('Q1')) if pd.notna(result.get('Q1')) else None,
            str(result.get('Q2')) if pd.notna(result.get('Q2')) else None,
            str(result.get('Q3')) if pd.notna(result.get('Q3')) else None,
            str(result.get('Time')) if pd.notna(result.get('Time')) else None,
            result.get('Status'),
            int(result.get('Points', 0)) if pd.notna(result.get('Points')) else 0,
            safe_int(result.get('Laps'))
        ))

    db.commit()
    return


def load_weather_data(session, session_id, db):
    """Load weather data for the session."""
    try:
        # Check if weather_data attribute exists and is available
        if not hasattr(session, 'weather_data'):
            print(f"  No weather data available for this session")
            return
        
        try:
            weather = session.weather_data
        except (AttributeError, ValueError) as e:
            print(f"  No weather data available for this session: {e}")
            return
            
        if weather is not None and len(weather) > 0:
            cursor = db.cursor()
            for _, w in weather.iterrows():
                cursor.execute("""
                    INSERT INTO weather
                    (timestamp, air_temp, humidity, pressure, rainfall,
                     track_temp, wind_direction, wind_speed, session_id)
                    VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
                """, (
                    w['Time'].isoformat() if pd.notna(w.get('Time')) else None,
                    float(w['AirTemp']) if pd.notna(w.get('AirTemp')) else None,
                    float(w['Humidity']) if pd.notna(w.get('Humidity')) else None,
                    float(w['Pressure']) if pd.notna(w.get('Pressure')) else None,
                    bool(w['Rainfall']) if pd.notna(w.get('Rainfall')) else False,
                    float(w['TrackTemp']) if pd.notna(w.get('TrackTemp')) else None,
                    int(w['WindDirection']) if pd.notna(w.get('WindDirection')) else None,
                    float(w['WindSpeed']) if pd.notna(w.get('WindSpeed')) else None,
                    session_id
                ))
            db.commit()
            print(f"  Loaded {len(weather)} weather records")
        else:
            print(f"  No weather records available")
    except Exception as e:
        print(f"  Warning: Could not load weather data: {e}")


def load_laps(session, session_id, db):
    """Load lap data for all drivers."""
    try:
        # Check if laps attribute exists and is available
        if not hasattr(session, 'laps'):
            print("  No lap data available for this session")
            return
        
        try:
            laps = session.laps
        except (AttributeError, ValueError) as e:
            print(f"  No lap data available for this session: {e}")
            return
            
    except Exception as e:
        print(f"  Warning: Could not load laps: {e}")
        return
        
    if laps is None or len(laps) == 0:
        print("  No lap data available")
        return

    cursor = db.cursor()
    count = 0

    for _, lap in laps.iterrows():
        cursor.execute("SELECT id FROM drivers WHERE driver_number = ?", (lap['DriverNumber'],))
        driver_row = cursor.fetchone()
        if driver_row is None:
            print(f"  Warning: Driver {lap['DriverNumber']} not found in database, skipping lap")
            continue
        driver_id = driver_row[0]

        cursor.execute("""
            INSERT INTO laps
            (timestamp, session_id, driver_id, lap_number, lap_time, stint,
             sector1_time, sector2_time, sector3_time, sector1_session_time,
             sector2_session_time, sector3_session_time, speed1, speed2, speedFL, speedST,
             personal_best, compound, tyre_life, fresh_tyre, lap_start_time,
             lap_start_date, track_status, position, pit_in_time, pit_out_time)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """, (
            lap['Time'].isoformat() if pd.notna(lap.get('Time')) else None,
            session_id,
            driver_id,
            int(lap['LapNumber']) if pd.notna(lap.get('LapNumber')) else None,
            str(lap['LapTime']) if pd.notna(lap.get('LapTime')) else None,
            int(lap['Stint']) if pd.notna(lap.get('Stint')) else None,
            str(lap['Sector1Time']) if pd.notna(lap.get('Sector1Time')) else None,
            str(lap['Sector2Time']) if pd.notna(lap.get('Sector2Time')) else None,
            str(lap['Sector3Time']) if pd.notna(lap.get('Sector3Time')) else None,
            lap['Sector1SessionTime'].isoformat() if pd.notna(lap.get('Sector1SessionTime')) else None,
            lap['Sector2SessionTime'].isoformat() if pd.notna(lap.get('Sector2SessionTime')) else None,
            lap['Sector3SessionTime'].isoformat() if pd.notna(lap.get('Sector3SessionTime')) else None,
            float(lap['SpeedI1']) if pd.notna(lap.get('SpeedI1')) else None,
            float(lap['SpeedI2']) if pd.notna(lap.get('SpeedI2')) else None,
            float(lap['SpeedFL']) if pd.notna(lap.get('SpeedFL')) else None,
            float(lap['SpeedST']) if pd.notna(lap.get('SpeedST')) else None,
            bool(lap['IsPersonalBest']) if pd.notna(lap.get('IsPersonalBest')) else False,
            lap.get('Compound'),
            int(lap['TyreLife']) if pd.notna(lap.get('TyreLife')) else None,
            bool(lap['FreshTyre']) if pd.notna(lap.get('FreshTyre')) else False,
            lap['LapStartTime'].isoformat() if pd.notna(lap.get('LapStartTime')) else None,
            lap['LapStartDate'].isoformat() if pd.notna(lap.get('LapStartDate')) else None,
            lap.get('TrackStatus'),
            int(lap['Position']) if pd.notna(lap.get('Position')) else None,
            lap['PitInTime'].isoformat() if pd.notna(lap.get('PitInTime')) else None,
            lap['PitOutTime'].isoformat() if pd.notna(lap.get('PitOutTime')) else None
        ))
        count += 1

        if count % 100 == 0:
            db.commit()

    db.commit()
    print(f"  Loaded {count} laps")


def load_telemetry(session, driver_abbr, db, sample_rate=1):
    """Load telemetry data for a specific driver (sampled to reduce data size)."""
    try:
        laps = session.laps.pick_drivers(driver_abbr)
        if laps is None or len(laps) == 0:
            return 0

        cursor = db.cursor()
        count = 0

        for _, lap in laps.iterrows():
            telemetry = lap.get_telemetry()
            if telemetry is None or len(telemetry) == 0:
                continue

            # Sample the data to reduce size
            telemetry = telemetry.iloc[::sample_rate]

            # Get lap_id from database
            cursor.execute(
                "SELECT id FROM laps WHERE driver_id = (SELECT id FROM drivers WHERE abbrevation = ?) AND lap_number = ?",
                (driver_abbr, int(lap.get('LapNumber')))
            )
            lap_row = cursor.fetchone()
            lap_id = lap_row[0] if lap_row is not None else None

            for _, telem in telemetry.iterrows():
                # Helper function to safely convert values, handling empty strings
                def safe_int(val):
                    if pd.notna(val) and val != '':
                        return int(val)
                    return None
                
                def safe_float(val):
                    if pd.notna(val) and val != '':
                        return float(val)
                    return None
                
                def safe_bool(val):
                    if pd.notna(val) and val != '':
                        return bool(val)
                    return False
                
                cursor.execute("""
                    INSERT INTO telemetry
                    (driver_ahead, dist_to_driver_ahead, time, date, rpm, speed, ngear,
                    throttle, brake, drs, distance, rel_dist, status, X, Y, Z, lap_id)
                    VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """, (
                    safe_int(telem.get('DriverAhead')),
                    safe_float(telem.get('DistanceToDriverAhead')),
                    telem['Time'].isoformat() if pd.notna(telem.get('Time')) else None,
                    telem['Date'].isoformat() if pd.notna(telem.get('Date')) else None,
                    safe_float(telem.get('RPM')),
                    safe_float(telem.get('Speed')),
                    safe_int(telem.get('nGear')),
                    safe_float(telem.get('Throttle')),
                    safe_bool(telem.get('Brake')),
                    safe_int(telem.get('DRS')),
                    safe_float(telem.get('Distance')),
                    safe_float(telem.get('RelativeDistance')),
                    telem.get('Status'),
                    safe_float(telem.get('X')),
                    safe_float(telem.get('Y')),
                    safe_float(telem.get('Z')),
                    lap_id
                ))
                count += 1

        db.commit()
        return count
    except Exception as e:
        print(f"    Warning: Could not load telemetry for {driver_abbr}: {e}")
        return 0


def load_event(year, event, session_num, load_telemetry_data=False):
    """
    Load a complete F1 event into the database.

    Args:
        year: Season year (e.g., 2024)
        event: Event identifier (e.g., 'Bahrain', 'Monaco', or round number)
        session_num: Session number (1-5, where 1=FP1, 2=FP2, 3=FP3, 4=Qualifying, 5=Race)
        load_telemetry_data: Whether to load telemetry (can be very large)
    """
    print(f"\nLoading {year} {event} - {session_num}")

    try:
        # Load the session
        session = fastf1.get_session(year, event, session_num)
        session.load(telemetry=load_telemetry_data, messages=False, laps=True, weather=True)
    except Exception as e:
        print(f"Error loading session data: {e}")
        return

    db = get_db()

    # Load circuit info
    print("Loading circuit info...")
    try:
        circuit_id = load_circuit_info(session, db)
    except Exception as e:
        print(f"  Warning: Could not load circuit info: {e}")
        # Create a basic circuit entry
        cursor = db.cursor()
        cursor.execute("INSERT INTO circuits (name, rotation) VALUES (?, ?)",
                      (session.event.get('Location', 'Unknown'), 0))
        circuit_id = cursor.lastrowid
        db.commit()

    # Load drivers and teams
    print("Loading drivers and teams...")
    load_drivers_and_teams(session, db)

    # Load session
    print("Loading session metadata...")
    session_id = load_session(session, circuit_id, db)

    # Load results
    print("Loading results...")
    load_results(session, session_id, db)

    # Load weather
    print("Loading weather data...")
    load_weather_data(session, session_id, db)

    # Load laps
    print("Loading lap data...")
    load_laps(session, session_id, db)

    # Load telemetry (optional, can be very large)
    if load_telemetry_data:
        print("Loading telemetry data (sampled)...")
        total_telem = 0
        cursor = db.cursor()
        cursor.execute("SELECT abbrevation FROM drivers")
        driver_abbrs = [row[0] for row in cursor.fetchall()]
        for driver_abbr in driver_abbrs:
            count = load_telemetry(session, driver_abbr, db)
            if count > 0:
                print(f"  Loaded {count} telemetry points for {driver_abbr}")
                total_telem += count
        print(f"Total telemetry points: {total_telem}")

    db.close()
    print(f"\n Successfully loaded {year} {event} - {session_num}")


if __name__ == '__main__':
    # Initialize database
    init_db()
    years = range(2025, 2026)
    for year in years:
        # Load all events for the year
        schedule = fastf1.get_event_schedule(year)
        for _, event in schedule.iloc[1:].iterrows():
            try:
                for i in range(5, 6):
                    load_event(year, event['EventName'], session_num=i, load_telemetry_data=True)
            except Exception as e:
                print(f"Error loading {year} {event['EventName']}: {e}")
