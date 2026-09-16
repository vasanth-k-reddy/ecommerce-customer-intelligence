-- Use database and schema
USE DATABASE ECOMMERCE_DB;
USE SCHEMA ANALYTICS;

-- Customer Intelligence View
CREATE OR REPLACE VIEW VW_CUSTOMER_INTELLIGENCE AS
SELECT
    value:customer_id::VARCHAR AS customer_id,
    value:customer_name::VARCHAR AS customer_name,
    value:email::VARCHAR AS email,
    value:country::VARCHAR AS country,
    value:city::VARCHAR AS city,
    value:customer_segment::VARCHAR AS customer_segment,
    value:total_orders::NUMBER AS total_orders,
    value:total_spending::NUMBER(18,2) AS total_spending,
    value:avg_order_value::NUMBER(18,2) AS avg_order_value,
    value:first_order_date::TIMESTAMP AS first_order_date,
    value:last_order_date::TIMESTAMP AS last_order_date,
    value:customer_lifetime_days::NUMBER AS customer_lifetime_days
FROM CUSTOMER_INTELLIGENCE_EXT;


-- Product Performance View
CREATE OR REPLACE VIEW VW_PRODUCT_PERFORMANCE AS
SELECT
    value:product_id::VARCHAR AS product_id,
    value:product_name::VARCHAR AS product_name,
    value:category::VARCHAR AS category,
    value:brand::VARCHAR AS brand,
    value:unit_price::NUMBER(18,2) AS unit_price,
    value:product_rating::NUMBER(3,1) AS product_rating,
    value:units_sold::NUMBER AS units_sold,
    value:total_revenue::NUMBER(18,2) AS total_revenue,
    value:avg_selling_price::NUMBER(18,2) AS avg_selling_price,
    value:avg_discount_pct::NUMBER(5,2) AS avg_discount_pct
FROM PRODUCT_PERFORMANCE_EXT;


-- Fraud Analytics View
CREATE OR REPLACE VIEW VW_FRAUD_DASHBOARD AS
SELECT
    value:order_id::VARCHAR AS order_id,
    value:customer_id::VARCHAR AS customer_id,
    value:order_timestamp::TIMESTAMP AS order_timestamp,
    value:order_amount::NUMBER(18,2) AS order_amount,
    value:order_status::VARCHAR AS order_status,
    value:payment_method::VARCHAR AS payment_method,
    value:payment_status::VARCHAR AS payment_status,
    value:payment_attempts::NUMBER AS payment_attempts,
    value:device_id::VARCHAR AS device_id,
    value:ip_address::VARCHAR AS ip_address,
    value:is_fraud::NUMBER AS is_fraud,
    value:fraud_reason::VARCHAR AS fraud_reason,
    value:country_mismatch::NUMBER AS country_mismatch,
    value:risk_level::VARCHAR AS risk_level
FROM FRAUD_ANALYTICS_EXT;