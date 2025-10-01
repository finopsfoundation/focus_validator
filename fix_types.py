import pandas as pd
import duckdb

# Define expected string columns based on FOCUS spec
STRING_COLUMNS = [
    'AvailabilityZone', 'BillingAccountId', 'BillingAccountName', 'BillingCurrency',
    'ChargeCategory', 'ChargeClass', 'ChargeDescription', 'ChargeFrequency',
    'CommitmentDiscountCategory', 'CommitmentDiscountId', 'CommitmentDiscountName',
    'CommitmentDiscountStatus', 'CommitmentDiscountType', 'ConsumedUnit',
    'InvoiceIssuerName', 'PricingCategory', 'PricingUnit', 'ProviderName',
    'PublisherName', 'RegionId', 'RegionName', 'ResourceId', 'ResourceName',
    'ResourceType', 'ServiceCategory', 'ServiceName', 'SkuId', 'SkuPriceId',
    'SubAccountId', 'SubAccountName', 'Tags'
]

# Load CSV with explicit string types for known string columns
dtype_dict = {col: 'string' for col in STRING_COLUMNS}
df = pd.read_csv("tests/samples/focus_sample_10000.csv", dtype=dtype_dict)

print(f"Fixed Pandas ChargeClass dtype: {df['ChargeClass'].dtype}")

# Register with DuckDB
conn = duckdb.connect(":memory:")
conn.register("focus_data", df)

# Check DuckDB schema for ChargeClass
result = conn.execute("PRAGMA table_info('focus_data')").fetchall()
for row in result:
    if 'ChargeClass' in row[1]:
        print(f"Fixed DuckDB ChargeClass type: {row}")

# Try the query again
try:
    result = conn.execute("SELECT COUNT(*) FROM focus_data WHERE ChargeClass != 'Correction'").fetchall()
    print(f"Query result: {result}")
except Exception as e:
    print(f"Error: {e}")

