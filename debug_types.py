import pandas as pd
import duckdb

# Load the CSV data like the focus validator does
df = pd.read_csv("tests/samples/focus_sample_10000.csv")
print(f"Pandas ChargeClass dtype: {df['ChargeClass'].dtype}")
print(f"Pandas ChargeClass unique values: {df['ChargeClass'].dropna().unique()}")
print(f"Total non-null ChargeClass values: {df['ChargeClass'].notna().sum()}")

# Register with DuckDB
conn = duckdb.connect(":memory:")
conn.register("focus_data", df)

# Check DuckDB schema
result = conn.execute("PRAGMA table_info('focus_data')").fetchall()
for row in result:
    if 'ChargeClass' in row[1]:  # column name is at index 1
        print(f"DuckDB ChargeClass type: {row}")

# Try the problematic query
try:
    result = conn.execute("SELECT COUNT(*) FROM focus_data WHERE ChargeClass != 'Correction'").fetchall()
    print(f"Query result: {result}")
except Exception as e:
    print(f"Error: {e}")

