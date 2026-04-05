#!/usr/bin/env python3
"""
Combine individual year .db files into a single f1.db.

Usage:
    python combine_databases.py [--dest f1.db] [--pattern "*.db"]

Notes:
- Skips the destination DB if present.
- Creates missing tables in destination using source CREATE statements (uses IF NOT EXISTS).
- Uses `INSERT OR IGNORE` to avoid duplicate primary-key rows.
"""

import argparse
import glob
import os
import sqlite3
from tqdm import tqdm
import tempfile
import shutil



def normalize_create_sql(sql: str) -> str:
    if sql is None:
        return None
    # Add IF NOT EXISTS to CREATE TABLE statements
    return sql.replace("CREATE TABLE ", "CREATE TABLE IF NOT EXISTS ", 1)


# Tables that are common across years and should be deduplicated by a natural key
COMMON_TABLES = {"circuits", "drivers", "teams"}


def find_existing_id(dest_conn: sqlite3.Connection, table: str, row_vals: dict, pk_col: str):
    """Return existing id in destination matching the natural key for `table`, or None."""
    cur = dest_conn.cursor()
    try:
        if table == "circuits":
            name = row_vals.get("name")
            if name is None:
                return None
            r = cur.execute("SELECT id FROM circuits WHERE name = ? LIMIT 1", (name,)).fetchone()
            return r[0] if r else None
        if table == "teams":
            name = row_vals.get("name")
            if name is None:
                return None
            r = cur.execute("SELECT id FROM teams WHERE name = ? LIMIT 1", (name,)).fetchone()
            return r[0] if r else None
        if table == "drivers":
            # prefer abbrevation match, then (name, driver_number)
            abbr = row_vals.get("abbrevation") or row_vals.get("abbreviation")
            if abbr:
                r = cur.execute("SELECT id FROM drivers WHERE abbrevation = ? LIMIT 1", (abbr,)).fetchone()
                if r:
                    return r[0]
            name = row_vals.get("name")
            num = row_vals.get("driver_number")
            if name and num is not None:
                r = cur.execute("SELECT id FROM drivers WHERE name = ? AND driver_number = ? LIMIT 1", (name, num)).fetchone()
                return r[0] if r else None
    except Exception:
        return None
    return None


def update_existing_row(dest_conn: sqlite3.Connection, table: str, dest_id: int, row_vals: dict, cols: list):
    """Update the destination row `dest_id` in `table` with non-null values from `row_vals` when dest is NULL.
    This mirrors `load.py` behaviour of filling missing fields for common tables."""
    cur = dest_conn.cursor()
    try:
        # fetch existing row values in the same column order
        q = f"SELECT {', '.join([f'\"{c}\"' for c in cols])} FROM {table} WHERE id = ?"
        dest_row = cur.execute(q, (dest_id,)).fetchone()
        if not dest_row:
            return
        updates = {}
        for i, c in enumerate(cols):
            if c == 'id':
                continue
            dst_val = dest_row[i]
            src_val = row_vals.get(c)
            if dst_val is None and src_val is not None:
                updates[c] = src_val
        if updates:
            set_clause = ", ".join([f'\"{k}\" = ?' for k in updates.keys()])
            cur.execute(f"UPDATE {table} SET {set_clause} WHERE id = ?", (*updates.values(), dest_id))
            dest_conn.commit()
    except Exception:
        # best-effort update; ignore failures
        return


def find_db_files(pattern: str, dest: str):
    dest_abs = os.path.abspath(dest)
    files = sorted(glob.glob(pattern))
    files = [f for f in files if os.path.abspath(f) != dest_abs]
    return files


def combine_databases(db_files, dest_path="f1.db"):
    if not db_files:
        print("No source .db files found to combine.")
        return

    dest_conn = sqlite3.connect(dest_path)
    dest_conn.execute("PRAGMA foreign_keys=OFF")
    dest_conn.execute("PRAGMA journal_mode=WAL")

    try:
        for idx, src in enumerate(tqdm(db_files, desc="Source DBs")):
            alias = f"src{idx}"
            tqdm.write(f"Attaching {src} as {alias}")
            dest_conn.execute(f"ATTACH DATABASE ? AS {alias}", (src,))

            # Read tables and create in destination if missing
            tables = dest_conn.execute(f"SELECT name, sql FROM {alias}.sqlite_master WHERE type='table'").fetchall()
            table_names = [name for name, _ in tables if not name.startswith("sqlite_")]
            for name, sql in tables:
                if name.startswith("sqlite_"):
                    continue
                create_sql = normalize_create_sql(sql)
                if create_sql:
                    try:
                        dest_conn.execute(create_sql)
                    except Exception as e:
                        tqdm.write(f"  Warning creating table {name}: {e}")

            # Build dependency graph (parent -> [children]) using foreign_key_list
            parents = {t: set() for t in table_names}
            children = {t: set() for t in table_names}
            fk_map = {t: [] for t in table_names}  # list of fk dicts for each table
            for t in table_names:
                try:
                    fk_rows = dest_conn.execute(f"PRAGMA {alias}.foreign_key_list('{t}')").fetchall()
                except Exception:
                    fk_rows = []
                for fk in fk_rows:
                    # fk format: (id, seq, table, from, to, on_update, on_delete, match)
                    parent_table = fk[2]
                    from_col = fk[3]
                    to_col = fk[4]
                    if parent_table in table_names:
                        parents[t].add(parent_table)
                        children[parent_table].add(t)
                        fk_map[t].append({"from": from_col, "to": to_col, "parent": parent_table})

            # Topological sort: parents before children
            in_degree = {t: len(parents[t]) for t in table_names}
            queue = [t for t, deg in in_degree.items() if deg == 0]
            topo = []
            while queue:
                n = queue.pop(0)
                topo.append(n)
                for c in children.get(n, []):
                    in_degree[c] -= 1
                    if in_degree[c] == 0:
                        queue.append(c)

            # mapping old_id -> new_id per table
            id_map = {t: {} for t in table_names}

            # Process tables in topo order
            for t in topo:
                # get columns and pk info
                try:
                    cols_info = dest_conn.execute(f"PRAGMA {alias}.table_info('{t}')").fetchall()
                except Exception:
                    cols_info = []
                cols = [row[1] for row in cols_info]
                pk_cols = [row[1] for row in cols_info if row[5] > 0]
                pk_col = pk_cols[0] if len(pk_cols) == 1 else None
                if not cols:
                    continue

                cols_quoted = ", ".join([f'"{c}"' for c in cols])
                select_rows = dest_conn.execute(f'SELECT {cols_quoted} FROM {alias}."{t}"').fetchall()
                if not select_rows:
                    continue

                dest_cursor = dest_conn.cursor()
                dest_cursor.execute("BEGIN")
                try:
                    for row in select_rows:
                        row_vals = dict(zip(cols, row))

                        # If this is a common table, try to find an existing row in dest and reuse its id
                        if t in COMMON_TABLES and pk_col and row_vals.get(pk_col) is not None:
                            existing = find_existing_id(dest_conn, t, row_vals, pk_col)
                            if existing is not None:
                                # update destination row with any missing fields from source (mirrors load.py)
                                update_existing_row(dest_conn, t, existing, row_vals, cols)
                                id_map[t][row_vals[pk_col]] = existing
                                continue

                        # Remap FK columns to new ids if present
                        for fk in fk_map.get(t, []):
                            from_col = fk["from"]
                            parent = fk["parent"]
                            val = row_vals.get(from_col)
                            if val is not None and val in id_map.get(parent, {}):
                                row_vals[from_col] = id_map[parent][val]

                        # Attempt insert preserving PK if possible, otherwise let dest assign a new id
                        insert_cols = cols.copy()
                        insert_vals = [row_vals[c] for c in insert_cols]
                        try:
                            placeholders = ", ".join(["?" for _ in insert_cols])
                            insert_sql = f'INSERT INTO "{t}" ({", ".join([f'"{c}"' for c in insert_cols])}) VALUES ({placeholders})'
                            dest_cursor.execute(insert_sql, insert_vals)
                            new_id = dest_cursor.lastrowid
                        except sqlite3.IntegrityError:
                            # For common tables, try to find existing row again
                            if t in COMMON_TABLES and pk_col and row_vals.get(pk_col) is not None:
                                existing = find_existing_id(dest_conn, t, row_vals, pk_col)
                                if existing is not None:
                                    id_map[t][row_vals[pk_col]] = existing
                                    continue
                            # remove PK column (if single integer PK) and insert again to get new id
                            if pk_col and pk_col in insert_cols:
                                idx_pk = insert_cols.index(pk_col)
                                insert_cols.pop(idx_pk)
                                insert_vals.pop(idx_pk)
                                placeholders = ", ".join(["?" for _ in insert_cols])
                                insert_sql = f'INSERT INTO "{t}" ({", ".join([f'"{c}"' for c in insert_cols])}) VALUES ({placeholders})'
                                dest_cursor.execute(insert_sql, insert_vals)
                                new_id = dest_cursor.lastrowid
                            else:
                                # cannot remap, skip
                                tqdm.write(f"  Skipping row in {t} due to integrity error and no single PK to remap")
                                continue

                        # record id mapping
                        if pk_col and row_vals.get(pk_col) is not None:
                            old_id = row_vals[pk_col]
                            id_map[t][old_id] = new_id

                    dest_cursor.execute("COMMIT")
                except Exception as e:
                    dest_cursor.execute("ROLLBACK")
                    tqdm.write(f"  Warning inserting rows for table {t} from {src}: {e}")

            dest_conn.execute(f"DETACH DATABASE {alias}")
        print(f"Combined {len(db_files)} databases into {dest_path}")
    finally:
        dest_conn.close()


def run_self_test():
    """Create two sample DBs with conflicting ids and verify combine remaps ids and FKs."""
    tmpdir = tempfile.mkdtemp(prefix="f1_combine_test_")
    try:
        src1 = os.path.join(tmpdir, "a.db")
        src2 = os.path.join(tmpdir, "b.db")
        dest = os.path.join(tmpdir, "combined.db")

        schema = """
        CREATE TABLE parent (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            name TEXT
        );
        CREATE TABLE child (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            parent_id INTEGER,
            val TEXT,
            FOREIGN KEY(parent_id) REFERENCES parent(id)
        );
        """

        def make_db(path, parent_name, child_val):
            conn = sqlite3.connect(path)
            cur = conn.cursor()
            cur.executescript(schema)
            cur.execute("INSERT INTO parent (id, name) VALUES (?, ?)", (1, parent_name))
            cur.execute("INSERT INTO child (id, parent_id, val) VALUES (?, ?, ?)", (1, 1, child_val))
            conn.commit()
            conn.close()

        make_db(src1, "PARENT_A", "CHILD_A")
        make_db(src2, "PARENT_B", "CHILD_B")

        # Run combine on the two created DBs
        combine_databases([src1, src2], dest_path=dest)

        # Verify
        conn = sqlite3.connect(dest)
        cur = conn.cursor()
        parents = cur.execute("SELECT id, name FROM parent ORDER BY id").fetchall()
        children = cur.execute("SELECT id, parent_id, val FROM child ORDER BY id").fetchall()
        conn.close()

        print("Parents:", parents)
        print("Children:", children)

        assert len(parents) == 2, "Expected 2 parents in combined DB"
        assert len(children) == 2, "Expected 2 children in combined DB"

        # Each child should reference one of the parent ids
        parent_ids = {p[0] for p in parents}
        assert all(ch[1] in parent_ids for ch in children), "Child parent_id not remapped correctly"

        print("Self-test passed: ids remapped and foreign keys updated.")
    finally:
        shutil.rmtree(tmpdir)


def main():
    p = argparse.ArgumentParser(description="Combine year DBs into a single f1.db")
    p.add_argument("--dest", "-d", default="f1.db", help="Destination combined DB path")
    p.add_argument("--pattern", "-p", default="*.db", help="Glob pattern to find source DBs")
    p.add_argument("--test", action="store_true", help="Run self-test and exit")
    args = p.parse_args()

    if args.test:
        run_self_test()
        return

    db_files = find_db_files(args.pattern, args.dest)
    combine_databases(db_files, dest_path=args.dest)


if __name__ == '__main__':
    main()

