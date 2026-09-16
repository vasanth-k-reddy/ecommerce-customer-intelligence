from pyspark.sql import SparkSession
from pyspark.sql.functions import col, trim, to_timestamp, to_date

spark = SparkSession.builder \
    .appName("Ecommerce Silver Transformation") \
    .getOrCreate()

base_path = "s3://ecommerce-customer-intelligence"

# Customers
customers = spark.read.format("delta").load(
    f"{base_path}/bronze/customers"
)

customers = customers \
    .withColumn("customer_id", trim(col("customer_id"))) \
    .withColumn("customer_name", trim(col("customer_name"))) \
    .withColumn("signup_date", to_date(col("signup_date")))

customers.write.format("delta").mode("overwrite").save(
    f"{base_path}/silver/customers"
)

# Products
products = spark.read.format("delta").load(
    f"{base_path}/bronze/products"
)

products = products \
    .withColumn("product_id", trim(col("product_id"))) \
    .withColumn("product_name", trim(col("product_name"))) \
    .withColumn("unit_price", col("unit_price").cast("double")) \
    .withColumn("product_rating", col("product_rating").cast("double"))

products.write.format("delta").mode("overwrite").save(
    f"{base_path}/silver/products"
)

# Orders
orders = spark.read.format("delta").load(
    f"{base_path}/bronze/orders"
)

orders = orders \
    .withColumn("order_id", trim(col("order_id"))) \
    .withColumn("customer_id", trim(col("customer_id"))) \
    .withColumn("order_timestamp", to_timestamp(col("order_timestamp"))) \
    .withColumn("order_amount", col("order_amount").cast("double"))

orders.write.format("delta").mode("overwrite").save(
    f"{base_path}/silver/orders"
)

print("Customers, products, and orders transformed successfully.")

# Order Items
order_items = spark.read.format("delta").load(
    f"{base_path}/bronze/order_items"
)

order_items = order_items \
    .withColumn("order_item_id", trim(col("order_item_id"))) \
    .withColumn("order_id", trim(col("order_id"))) \
    .withColumn("product_id", trim(col("product_id"))) \
    .withColumn("quantity", col("quantity").cast("int")) \
    .withColumn("unit_price", col("unit_price").cast("double")) \
    .withColumn("discount_pct", col("discount_pct").cast("double")) \
    .withColumn("line_amount", col("line_amount").cast("double"))

order_items.write.format("delta").mode("overwrite").save(
    f"{base_path}/silver/order_items"
)


# Payments
payments = spark.read.format("delta").load(
    f"{base_path}/bronze/payments"
)

payments = payments \
    .withColumn("payment_id", trim(col("payment_id"))) \
    .withColumn("order_id", trim(col("order_id"))) \
    .withColumn("customer_id", trim(col("customer_id"))) \
    .withColumn("payment_amount", col("payment_amount").cast("double")) \
    .withColumn("payment_attempts", col("payment_attempts").cast("int")) \
    .withColumn("payment_timestamp", to_timestamp(col("payment_timestamp")))

payments.write.format("delta").mode("overwrite").save(
    f"{base_path}/silver/payments"
)


# Returns
returns = spark.read.format("delta").load(
    f"{base_path}/bronze/returns"
)

returns = returns \
    .withColumn("return_id", trim(col("return_id"))) \
    .withColumn("order_id", trim(col("order_id"))) \
    .withColumn("customer_id", trim(col("customer_id"))) \
    .withColumn("return_amount", col("return_amount").cast("double")) \
    .withColumn("return_date", to_date(col("return_date")))

returns.write.format("delta").mode("overwrite").save(
    f"{base_path}/silver/returns"
)


# Customer Activity
customer_activity = spark.read.format("delta").load(
    f"{base_path}/bronze/customer_activity"
)

customer_activity = customer_activity \
    .withColumn("activity_id", trim(col("activity_id"))) \
    .withColumn("customer_id", trim(col("customer_id"))) \
    .withColumn("activity_timestamp", to_timestamp(col("activity_timestamp")))

customer_activity.write.format("delta").mode("overwrite").save(
    f"{base_path}/silver/customer_activity"
)


# Reviews
reviews = spark.read.format("delta").load(
    f"{base_path}/bronze/reviews"
)

reviews = reviews \
    .withColumn("review_id", trim(col("review_id"))) \
    .withColumn("order_id", trim(col("order_id"))) \
    .withColumn("customer_id", trim(col("customer_id"))) \
    .withColumn("product_id", trim(col("product_id"))) \
    .withColumn("rating", col("rating").cast("int")) \
    .withColumn("review_date", to_date(col("review_date")))

reviews.write.format("delta").mode("overwrite").save(
    f"{base_path}/silver/reviews"
)


# Fraud Signals
fraud_signals = spark.read.format("delta").load(
    f"{base_path}/bronze/fraud_signals"
)

fraud_signals = fraud_signals \
    .withColumn("order_id", trim(col("order_id"))) \
    .withColumn("customer_id", trim(col("customer_id"))) \
    .withColumn("is_fraud", col("is_fraud").cast("int")) \
    .withColumn("payment_failed", col("payment_failed").cast("int")) \
    .withColumn("country_mismatch", col("country_mismatch").cast("int")) \
    .withColumn("payment_attempts", col("payment_attempts").cast("int"))

fraud_signals.write.format("delta").mode("overwrite").save(
    f"{base_path}/silver/fraud_signals"
)

print("Silver transformation completed for all 9 datasets.")