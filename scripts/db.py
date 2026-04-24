import sqlite3
import os

def get_db(path: str="f1.db"):
    conn = sqlite3.connect(path)
    conn.row_factory = sqlite3.Row
    return conn

def init_db(year, reset=False):
    path = str(year) + ".db"
    if os.path.exists(path=path) and not reset:
        print("Database already exists. Use reset=True to overwrite.")
        return

    with open('sql/schema.sql') as f:
        if os.path.exists(path=path):
            os.remove(path)
        db = get_db(path)
        db.executescript(f.read())
        db.commit()

if __name__ == '__main__':
    init_db(year=2017, reset=True)