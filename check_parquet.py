import duckdb

path = r"C:/Users/Alexander/Downloads/reckoner_dms_1776462232314.parquet"

conn = duckdb.connect()
df = conn.execute(f"SELECT * FROM read_parquet('{path}') LIMIT 5").df()
print(f"Rows in file: {conn.execute(f'SELECT COUNT(*) FROM read_parquet(\'{path}\')').fetchone()[0]}")
print(f"Columns: {list(df.columns)}")
print()
print(df.to_string())
