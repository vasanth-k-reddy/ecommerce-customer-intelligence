from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col, count, sum, avg, min, max,
    coalesce, lit, round, datediff, when
)

spark = SparkSession.builder \
    .appName("Ecommerce Gold Analytics") \
    .getOrCreate()

base_path = "s3://ecommerce-customer-intelligence"


# ============================================================
# 1. Customer Intelligence Gold Table
# ============================================================

silver_customers = spark.read.format("delta").load(
    f"{base_path}/silver/customers"
)

silver_orders = spark.read.format("delta").load(
    f"{base_path}/silver/orders"
)

customer_order_metrics = silver_orders.groupBy("customer_id").agg(
    count("order_id").alias("total_orders"),
    sum("order_amount").alias("total_spending"),
    avg("order_amount").alias("avg_order_value"),
    min("order_timestamp").alias("first_order_date"),
    max("order_timestamp").alias("last_order_date")
)

gold_customer_intelligence = silver_customers.join(
    customer_order_metrics,
    silver_customers.customer_id == customer_order_metrics.customer_id,
    "left"
).select(
    silver_customers.customer_id,
    silver_customers.customer_name,
    silver_customers.email,
    silver_customers.country,
    silver_customers.city,
    silver_customers.customer_segment,
    customer_order_metrics.total_orders,
    customer_order_metrics.total_spending,
    customer_order_metrics.avg_order_value,
    customer_order_metrics.first_order_date,
    customer_order_metrics.last_order_date
)

gold_customer_intelligence = gold_customer_intelligence \
    .withColumn("total_orders", coalesce(col("total_orders"), lit(0))) \
    .withColumn(
        "total_spending",
        round(coalesce(col("total_spending"), lit(0.0)), 2)
    ) \
    .withColumn(
        "avg_order_value",
        round(coalesce(col("avg_order_value"), lit(0.0)), 2)
    ) \
    .withColumn(
        "customer_lifetime_days",
        datediff(col("last_order_date"), col("first_order_date"))
    )

gold_customer_intelligence.write \
    .format("delta") \
    .mode("overwrite") \
    .save(f"{base_path}/gold/customer_intelligence")


# ============================================================
# 2. Product Performance Gold Table
# ============================================================

silver_order_items = spark.read.format("delta").load(
    f"{base_path}/silver/order_items"
)

silver_products = spark.read.format("delta").load(
    f"{base_path}/silver/products"
)

product_metrics = silver_order_items.groupBy("product_id").agg(
    sum("quantity").alias("units_sold"),
    round(sum("line_amount"), 2).alias("total_revenue"),
    round(avg("unit_price"), 2).alias("avg_selling_price"),
    round(avg("discount_pct") * 100, 2).alias("avg_discount_pct")
)

gold_product_performance = silver_products.join(
    product_metrics,
    silver_products.product_id == product_metrics.product_id,
    "left"
).select(
    silver_products.product_id,
    silver_products.product_name,
    silver_products.category,
    silver_products.brand,
    silver_products.unit_price,
    silver_products.product_rating,
    product_metrics.units_sold,
    product_metrics.total_revenue,
    product_metrics.avg_selling_price,
    product_metrics.avg_discount_pct
)

gold_product_performance.write \
    .format("delta") \
    .mode("overwrite") \
    .save(f"{base_path}/gold/product_performance")


# ============================================================
# 3. Fraud Analytics Gold Table
# ============================================================

silver_payments = spark.read.format("delta").load(
    f"{base_path}/silver/payments"
)

silver_fraud_signals = spark.read.format("delta").load(
    f"{base_path}/silver/fraud_signals"
)

fraud_base = silver_orders.alias("o") \
    .join(
        silver_payments.alias("p"),
        col("o.order_id") == col("p.order_id"),
        "left"
    ) \
    .join(
        silver_fraud_signals.alias("f"),
        col("o.order_id") == col("f.order_id"),
        "left"
    )

gold_fraud_analytics = fraud_base.select(
    col("o.order_id").alias("order_id"),
    col("o.customer_id").alias("customer_id"),
    col("o.order_timestamp").alias("order_timestamp"),
    col("o.order_amount").alias("order_amount"),
    col("o.order_status").alias("order_status"),
    col("p.payment_method").alias("payment_method"),
    col("p.payment_status").alias("payment_status"),
    col("p.payment_attempts").alias("payment_attempts"),
    col("p.device_id").alias("device_id"),
    col("p.ip_address").alias("ip_address"),
    col("f.is_fraud").alias("is_fraud"),
    col("f.fraud_reason").alias("fraud_reason"),
    col("f.country_mismatch").alias("country_mismatch")
)

gold_fraud_analytics = gold_fraud_analytics.withColumn(
    "risk_level",
    when(
        col("is_fraud") == 1,
        "High Risk"
    ).when(
        (col("payment_attempts") >= 3) |
        (col("country_mismatch") == 1) |
        (col("payment_status") == "Failed"),
        "Medium Risk"
    ).otherwise("Low Risk")
)

gold_fraud_analytics.write \
    .format("delta") \
    .mode("overwrite") \
    .save(f"{base_path}/gold/fraud_analytics")


print("Gold analytics tables created successfully.")