import sqlite3

conn = sqlite3.connect('f1.db')
conn.row_factory = sqlite3.Row
cursor = conn.cursor()

cursor.execute("SELECT id, name from teams")
teams = cursor.fetchall()

cursor.execute("SELECT id from sessions")
races = cursor.fetchall()

best_ranks = {}

for tid, name in teams:
    rank = 50
    for sid in races:
        cursor.execute("""
            SELECT team_id, RANK() OVER (ORDER BY points DESC) AS rank
            FROM results
            WHERE session_id = ?
            GROUP BY team_id
        """, (sid['id'],))
        rank_list = cursor.fetchall()
        for row in rank_list:
            if row['team_id'] == tid:
                rank = min(rank, row['rank'])

    best_ranks[name] = rank

print("Best Ranks for Each Team:")
for team, rank in best_ranks.items():
    print(f"{team}: {rank}")
