SOURCE_PATH = "/Workspace/Users/artisadikaj99@gmail.com/csv_files/"

IDENTIFIER_COLUMNS = [
    "salesperson_id", "salesperson_name",
    "sales_state_id", "sales_state_name",
    "store_id", "store_name", "year"
]
SALES_COLUMN_PATTERN = r"^[A-Z][a-z]{2}_sales$"

MONTH_MAP = {
    'Jan': 1, 'Feb': 2, 'Mar': 3, 'Apr': 4,
    'May': 5, 'Jun': 6, 'Jul': 7, 'Aug': 8,
    'Sep': 9, 'Oct': 10, 'Nov': 11, 'Dec': 12
}

import re
from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col, lit, expr, current_timestamp,
    when, coalesce
)
from pyspark.sql.types import IntegerType, DoubleType

spark = SparkSession.builder.getOrCreate()

print("Spark session initialized")
print(f"Spark version: {spark.version}")

def classify_columns(columns):
    """Separate identifier and sales columns based on pattern."""
    pattern = re.compile(SALES_COLUMN_PATTERN)
    sales_cols = [c for c in columns if pattern.match(c)]
    id_cols = [c for c in columns if c in IDENTIFIER_COLUMNS]
    return id_cols, sales_cols

def unpivot_dataframe(df, id_cols, sales_cols):
    """Transform wide format to normalized long format."""
    if not sales_cols:
        return df

    stack_parts = []
    for col_name in sales_cols:
        month = col_name.replace("_sales", "")
        stack_parts.append(f"'{month}', `{col_name}`")

    stack_expr = f"stack({len(sales_cols)}, {', '.join(stack_parts)}) as (month, sales_amount)"

    select_cols = id_cols + ["source_file", "load_timestamp"]
    result = df.select(
        *[col(c) for c in select_cols],
        expr(stack_expr)
    )

    month_case = " ".join([f"WHEN month = '{m}' THEN {n}" for m, n in MONTH_MAP.items()])
    result = result \
        .withColumn("month_number", expr(f"CASE {month_case} ELSE 0 END")) \
        .withColumn("quarter", ((col("month_number") - 1) / 3 + 1).cast("int"))

    return result

def validate_data(df):
    validated = df.withColumn(
        "is_valid",
        when(col("salesperson_id").isNull(), lit(False))
        .when(col("sales_amount").isNull(), lit(False))
        .when(col("sales_amount") < 0, lit(False))
        .otherwise(lit(True))
    )

    valid_df = validated.filter(col("is_valid") == True).drop("is_valid")
    invalid_df = validated.filter(col("is_valid") == False)

    return valid_df, invalid_df

try:
    files = spark.sql(f"SHOW FILES IN '{SOURCE_PATH}'").collect()
    print(f"Found {len(files)} files in {SOURCE_PATH}")
    for f in files:
        print(f"  - {f[0]}")
except:
    print(f"Checking path: {SOURCE_PATH}")

raw_df = spark.read \
    .option("header", "true") \
    .option("inferSchema", "true") \
    .csv(SOURCE_PATH + "*.csv")

raw_df = raw_df \
    .withColumn("source_file", col("_metadata.file_path")) \
    .withColumn("load_timestamp", current_timestamp())

print(f"Total records: {raw_df.count():,}")
print(f"Columns: {raw_df.columns}")
raw_df.show(5, truncate=False)

all_columns = [c for c in raw_df.columns if c not in ['source_file', 'load_timestamp']]

id_cols, sales_cols = classify_columns(all_columns)

print(f"Identifier columns ({len(id_cols)}): {id_cols}")
print(f"Sales columns ({len(sales_cols)}): {sales_cols}")

for c in sales_cols:
    raw_df = raw_df.withColumn(c, col(c).cast("double"))

normalized_df = unpivot_dataframe(raw_df, id_cols, sales_cols)

print(f"\nTransformed to {normalized_df.count():,} records")
normalized_df.show(10)

valid_df, invalid_df = validate_data(normalized_df)

valid_count = valid_df.count()
invalid_count = invalid_df.count()

print(f"Valid records: {valid_count:,}")
print(f"Invalid records: {invalid_count:,}")
print(f"Validation rate: {valid_count / (valid_count + invalid_count) * 100:.2f}%")

if invalid_count > 0:
    print("\nSample invalid records:")
    invalid_df.show(5)

valid_df.write \
    .mode("overwrite") \
    .format("delta") \
    .partitionBy("year", "month") \
    .saveAsTable(OUTPUT_TABLE)

print(f"Data saved to table: {OUTPUT_TABLE}")

result = spark.sql(f"SELECT COUNT(*) as count FROM {OUTPUT_TABLE}")
result.show()

summary = spark.sql(f"""
    SELECT
        COUNT(*) as total_records,
        COUNT(DISTINCT salesperson_id) as unique_salespeople,
        COUNT(DISTINCT store_id) as unique_stores,
        COUNT(DISTINCT sales_state_id) as unique_states,
        COUNT(DISTINCT month) as months_covered,
        SUM(sales_amount) as total_sales,
        AVG(sales_amount) as avg_sales,
        MIN(sales_amount) as min_sales,
        MAX(sales_amount) as max_sales
    FROM {OUTPUT_TABLE}
""")

print("Summary Statistics:")
summary.show(vertical=True)

print("Sales by Quarter:")
spark.sql(f"""
    SELECT year, quarter, SUM(sales_amount) as total_sales
    FROM {OUTPUT_TABLE}
    GROUP BY year, quarter
    ORDER BY year, quarter
""").show()

print("\nTop 10 Salespeople:")
spark.sql(f"""
    SELECT salesperson_id, salesperson_name, SUM(sales_amount) as total_sales
    FROM {OUTPUT_TABLE}
    GROUP BY salesperson_id, salesperson_name
    ORDER BY total_sales DESC
    LIMIT 10
""").show()

print("\nSales by State:")
spark.sql(f"""
    SELECT sales_state_name, SUM(sales_amount) as total_sales
    FROM {OUTPUT_TABLE}
    GROUP BY sales_state_name
    ORDER BY total_sales DESC
    LIMIT 10
""").show()