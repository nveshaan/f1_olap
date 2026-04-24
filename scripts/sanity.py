import duckdb
import os
import argparse
from typing import Optional

def check_f1_data_integrity(local_db: Optional[str] = None):
    # Initialize duckdb connection. If a local DB file is provided, open it.
    if local_db:
        local_db = os.path.expanduser(local_db)
        if not os.path.exists(local_db):
            raise FileNotFoundError(f"Local DB not found: {local_db}")
        con = duckdb.connect(local_db)
        hf_path_template = None
    else:
        con = duckdb.connect()
        # The 'hf://' protocol often requires the 'httpfs' extension to be loaded
        con.execute("INSTALL httpfs; LOAD httpfs;")
        # Use the requested hf:// path format
        hf_path_template = "hf://datasets/nveshaan/f1_olap/f1_parquet/{table}/{table}.parquet"

    tables = [
        "sessions", "results", "teams", "drivers",
        "circuits", "weather", "laps", "telemetry"
    ]

    for table in tables:
        try:
            if hf_path_template:
                url = hf_path_template.format(table=table)
                con.execute(f"CREATE OR REPLACE VIEW {table} AS SELECT * FROM read_parquet('{url}')")
            else:
                # If using a local DB file, expect the tables to already exist in the DB.
                # If the table exists, do nothing; otherwise warn and skip.
                try:
                    exists = con.execute(
                        f"SELECT COUNT(*) FROM information_schema.tables WHERE table_name = '{table}'"
                    ).fetchone()[0]
                except Exception:
                    exists = 0

                if exists:
                    # Table (or view) exists in DB; use it directly.
                    print(f"Using existing table/view '{table}' in local DB.")
                else:
                    print(f"Table '{table}' not found in local DB: {local_db}; skipping {table} view creation.")
                    continue
        except Exception as e:
            if hf_path_template:
                print(f"Error creating view for {table}: {e}")
                print(f"URL attempted: {url}")
            else:
                print(f"Error creating view for {table} from local DB: {e}")
                print(f"Local DB path: {local_db}")
            return
    
    print("--- F1 Data Integrity Check ---")
    
    # Validation Queries
    # We'll use a single query per session to check integrity efficiently
    integrity_query = """
    SELECT 
        s.id, s.event_name, s.session_name,
        (SELECT COUNT(*) FROM circuits c WHERE c.id = s.circuit_id) as circuit_exists,
        (SELECT COUNT(*) FROM results r WHERE r.session_id = s.id) as result_count,
        (SELECT COUNT(DISTINCT r.team_id) FROM results r WHERE r.session_id = s.id) as team_count,
        (SELECT COUNT(DISTINCT r.driver_id) FROM results r WHERE r.session_id = s.id) as driver_count,
        (SELECT COUNT(*) FROM weather w WHERE w.session_id = s.id) as weather_count,
        (SELECT COUNT(*) FROM laps l WHERE l.session_id = s.id) as lap_count,
        (SELECT COUNT(DISTINCT r.driver_id) 
         FROM results r 
         WHERE r.session_id = s.id 
         AND r.driver_id NOT IN (
            SELECT DISTINCT l.driver_id 
            FROM laps l 
            JOIN telemetry t ON t.lap_id = l.id 
            WHERE l.session_id = s.id
         )
        ) as drivers_missing_telemetry
    FROM sessions s
    """
    
    sessions = con.execute(integrity_query).fetchall()
    
    all_passed = True
    for row in sessions:
        sid, event, sname, c_exists, r_count, t_count, d_count, w_count, l_count, missing_tel_count = row
        
        errors = []
        if not c_exists: errors.append("Missing Circuit")
        if r_count == 0: errors.append("No Results")
        if t_count == 0: errors.append("No Teams")
        if d_count == 0: errors.append("No Drivers")
        if w_count == 0: errors.append("No Weather")
        if l_count == 0: errors.append("No Laps")
        
        if r_count > 0:
            if missing_tel_count == d_count:
                errors.append("No Telemetry")
            elif missing_tel_count > 0:
                errors.append(f"Missing Telemetry ({missing_tel_count}/{d_count} drivers)")
        
        status = "OK" if not errors else f"FAIL ({', '.join(errors)})"
        print(f"Session {sid:3} | {event:25} | {sname:15} | {status}")
        if errors: all_passed = False

    if all_passed:
        print("\nAll sessions passed integrity checks!")
    else:
        print("\nIntegrity checks failed for some sessions.")

def parse_args():
    p = argparse.ArgumentParser(description="Run F1 data integrity checks against remote hf:// parquet or a local DuckDB file ({year}.db)")
    p.add_argument("--local-db", dest="local_db", help="Path to local DuckDB file (e.g. 2022.db). If provided, loads tables from this DB.")
    p.add_argument("--year", dest="year", type=int, help="Shortcut to use a local DB named {year}.db in the current directory")
    return p.parse_args()


if __name__ == "__main__":
    args = parse_args()
    local_db = args.local_db
    if not local_db and args.year:
        local_db = f"{args.year}.db"

    try:
        # If a local_db path is provided and it's a directory, scan all .db files inside.
        if local_db and os.path.isdir(local_db):
            db_files = sorted([f for f in os.listdir(local_db) if f.endswith('.db')])
            if not db_files:
                print(f"No .db files found in directory: {local_db}")
            for dbf in db_files:
                db_path = os.path.join(local_db, dbf)
                print(f"\n--- Checking local DB: {db_path} ---")
                try:
                    check_f1_data_integrity(local_db=db_path)
                except Exception as e:
                    print(f"Error processing {db_path}: {e}")
        else:
            check_f1_data_integrity(local_db=local_db)
    except Exception as e:
        print(f"Error: {e}")
