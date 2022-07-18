import sqlite3

db = sqlite3.connect("output.db")

cursor = db.cursor()

cursor.execute("""CREATE TABLE report (
    timestamp integer,
    player_id integer,
    event_id integer,
    error_id text,
    json_server text,
    json_client text
)""")

db.commit()

db.close()