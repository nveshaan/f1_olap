import duckdb
import os

def get_connection(local_db=None):
    """
    Initialize duckdb connection. If a local DB file is provided, open it.
    Otherwise, load HF datasets.
    """
    if local_db:
        local_db = os.path.expanduser(local_db)
        if not os.path.exists(local_db):
            raise FileNotFoundError(f"Local DB not found: {local_db}")
        # Initialize an empty DuckDB, load SQLite scanner, and treat all SQLite columns as strings
        # to prevent strict conversion crashes on lap times
        con = duckdb.connect()
        con.execute("INSTALL sqlite; LOAD sqlite;")
        con.execute("SET sqlite_all_varchar=true")
        con.execute(f"ATTACH '{local_db}' AS local_db (TYPE SQLITE)")
        con.execute("USE local_db")
        hf_path_template = None
    else:
        con = duckdb.connect()
        con.execute("INSTALL httpfs; LOAD httpfs;")
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
            # If using a local DB file, expect the tables to already exist.
        except Exception as e:
            print(f"Error creating view for {table}: {e}")
            if hf_path_template:
                print(f"URL attempted: {url}")
                
    return con
