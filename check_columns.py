from pyspark.sql import SparkSession

spark = SparkSession.builder.getOrCreate()
df = spark.read.option("header", "true").option("sep", "|").csv("data/ESCOLAS.CSV")
print("\n=== COLUMNS IN YOUR CSV ===")
for col in df.columns:
    print(col)
print(f"\nTotal columns: {len(df.columns)}")
spark.stop()