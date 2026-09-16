from pyspark.sql import SparkSession

spark = SparkSession.builder \
    .appName("Ecommerce Bronze Ingestion") \
    .getOrCreate()

base_path = "s3://ecommerce-customer-intelligence"

datasets = [
    "customers",
    "products",
    "orders",
    "order_items",
    "payments",
    "returns",
    "customer_activity",
    "reviews",
    "fraud_signals"
]

for dataset in datasets:
    raw_path = f"{base_path}/raw/{dataset}/{dataset}.csv"
    bronze_path = f"{base_path}/bronze/{dataset}"

    df = spark.read \
        .option("header", "true") \
        .option("inferSchema", "true") \
        .csv(raw_path)

    df.write \
        .format("delta") \
        .mode("overwrite") \
        .save(bronze_path)

    print(f"Bronze ingestion completed: {dataset}")

print("All Bronze datasets ingested successfully.")