import sqlite3
import os

def get_db():
    conn = sqlite3.connect("f1.db")
    conn.row_factory = sqlite3.Row
    return conn

def init_db(reset=False):
    if os.path.exists("f1.db") and not reset:
        print("Database already exists. Use reset=True to overwrite.")
        return

    with open('schema.sql') as f:
        if os.path.exists("f1.db"):
            os.remove("f1.db")
        db = get_db()
        db.executescript(f.read())
        db.commit()

if __name__ == '__main__':
    init_db()