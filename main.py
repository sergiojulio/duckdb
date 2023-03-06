import duckdb

cursor = duckdb.connect()

print(cursor.execute("DESCRIBE SELECT * FROM 'userdata1.parquet';").fetchall())

