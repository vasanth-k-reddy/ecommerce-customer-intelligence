# E-Commerce Customer Intelligence & Fraud Analytics Data Platform

## Project Overview

This project implements an end-to-end cloud data engineering platform for analyzing e-commerce customer behavior, product performance, and fraudulent transaction patterns.

The platform integrates data from multiple business entities such as customers, products, orders, order items, payments, reviews, customer activity, returns, and fraud signals.

The processed data is transformed into business-ready datasets and visualized through Power BI dashboards.

## Business Problem

E-commerce companies generate large volumes of transactional and customer data. Business teams need reliable and centralized analytics to:

- Identify high-value customers
- Understand customer purchasing behavior
- Analyze product revenue and sales performance
- Detect suspicious transactions
- Monitor payment failures and fraud indicators
- Support data-driven business decisions


## Architecture

Python → AWS S3 → Databricks/PySpark → Delta Lake → Snowflake → Power BI

### Data Flow

1. Synthetic e-commerce data is generated using Python.
2. Raw CSV files are uploaded to Amazon S3.
3. Databricks reads the raw data and creates the Bronze layer.
4. Data cleaning, validation, type conversion, and consistency checks are performed in the Silver layer.
5. Business aggregations and analytical datasets are created in the Gold layer.
6. Snowflake external tables and typed views expose the Gold data for analytics.
7. Power BI connects to Snowflake views and provides interactive dashboards.

## Technologies Used

| Technology | Purpose |
|---|---|
| Python | Synthetic data generation |
| Amazon S3 | Cloud object storage |
| Databricks | Data processing and transformation |
| PySpark | Distributed data processing |
| Delta Lake | Reliable data storage and layered architecture |
| Snowflake | Analytical data warehouse |
| SQL | Data querying and analytical views |
| Power BI | Interactive dashboards and visualization |
| GitHub | Project version control and documentation |


## Dataset Details

The project uses a synthetically generated relational e-commerce dataset designed to simulate real-world business data.

| Dataset | Approximate Records |
|---|---:|
| Customers | 8,000 |
| Products | 2,000 |
| Orders | 60,000 |
| Order Items | 104,920 |
| Payments | 60,000 |
| Returns | 6,000 |
| Customer Activity | 150,000 |
| Reviews | 30,000 |
| Fraud Signals | 60,000 |

The datasets are connected using primary-key and foreign-key relationships to maintain referential integrity.

## Data Lake Layered Architecture

### Bronze Layer

The Bronze layer stores the raw data from Amazon S3 in Delta format with minimal transformation.

Activities:
- Read raw CSV files
- Preserve source data
- Store data in Delta Lake
- Perform basic record-count validation

### Silver Layer

The Silver layer contains cleaned and standardized data.

Activities:
- Trim text and ID columns
- Convert dates and timestamps
- Cast numeric columns
- Validate business rules
- Check duplicate records
- Validate referential integrity
- Handle invalid and inconsistent data

### Gold Layer

The Gold layer contains business-ready analytical datasets.

Gold tables created:

- Customer Intelligence
- Product Performance
- Fraud Analytics

## Snowflake Integration

Snowflake is used as the analytical data warehouse layer.

### Implementation

- Created the `ECOMMERCE_DB` database
- Created the `ANALYTICS` schema
- Configured an AWS S3 storage integration
- Created an external stage pointing to the Gold layer
- Created external tables using Delta Lake data
- Created typed analytical views for Power BI

### Snowflake Views

- `VW_CUSTOMER_INTELLIGENCE`
- `VW_PRODUCT_PERFORMANCE`
- `VW_FRAUD_DASHBOARD`

These views convert semi-structured external table data into business-friendly typed columns for reporting and analysis.

## Power BI Dashboards

Power BI is connected to Snowflake analytical views to create interactive dashboards.

### 1. Customer Intelligence Dashboard

Key metrics and visuals:

- Total Customers
- Total Orders
- Total Customer Spending
- Average Order Value
- Customer Segment Distribution
- Spending by Country
- Top 10 Customers by Spending
- Average Order Value by Customer Segment

### 2. Fraud Analytics Dashboard

Key metrics and visuals:

- Total Orders
- Fraud Orders
- Fraud Rate
- High-Risk Orders
- Risk Level Distribution
- Fraud Orders by Payment Method
- Fraud Orders by Fraud Reason
- Suspicious Order Details

### 3. Product Performance Dashboard

Key metrics and visuals:

- Total Products
- Total Product Revenue
- Total Units Sold
- Average Product Rating
- Revenue by Product Category
- Top 10 Products by Revenue
- Average Selling Price by Category
- Units Sold by Product Category
- Average Product Rating by Category

## Business Insights

The platform enables the following business insights:

### Customer Intelligence

- Identify high-value and repeat customers
- Compare customer spending across countries
- Analyze customer segment behavior
- Understand average order value
- Identify customers with high purchase frequency

### Product Performance

- Identify high-revenue products
- Compare category-level revenue
- Analyze units sold by category
- Compare product ratings
- Understand average selling prices

### Fraud Analytics

- Identify confirmed synthetic fraud-labelled orders
- Monitor high-risk transactions
- Analyze fraud by payment method
- Identify suspicious payment attempts
- Analyze country mismatch indicators
- Review suspicious order details for investigation

## Data Quality and Validation

Data quality checks were performed at the Bronze, Silver, and Gold layers.

Validation activities included:

- Duplicate primary-key detection
- Null-value checks
- Invalid date and timestamp checks
- Numeric range validation
- Allowed-value validation for status columns
- Foreign-key and referential-integrity checks
- Payment amount versus order amount validation
- Customer and order relationship validation
- Fraud flag validation
- Record-count reconciliation between layers

All major referential-integrity checks returned zero orphan records.

## Challenges and Solutions

### Challenge 1: Maintaining Relationships Across Multiple Datasets

Multiple datasets contained interconnected customer, order, product, and payment records.

Solution:
- Used primary-key and foreign-key relationships
- Performed referential-integrity checks
- Validated customer and order consistency

### Challenge 2: Data Type and Data Quality Issues

Raw CSV files contained string-based dates, numeric values, and inconsistent data types.

Solution:
- Applied PySpark type casting
- Standardized timestamps and numeric columns
- Added validation rules in the Silver layer

### Challenge 3: Connecting Snowflake to AWS S3

Snowflake required secure access to the S3 Gold layer.

Solution:
- Created an AWS IAM role
- Configured a Snowflake storage integration
- Created an external stage
- Validated access using Snowflake integration validation

### Challenge 4: Semi-Structured Data in Snowflake

Delta external tables exposed records as VARIANT data.

Solution:
- Created typed Snowflake views
- Explicitly cast JSON fields into analytical data types
- Used these views as Power BI data sources

## Future Enhancements

Possible future improvements include:

- Implement incremental data loading
- Add Apache Airflow for workflow orchestration
- Introduce dbt for Snowflake transformations
- Add real-time fraud detection using Kafka and Spark Structured Streaming
- Build machine-learning-based fraud prediction
- Add automated data-quality monitoring
- Implement Snowflake role-based access control
- Add dashboard refresh automation
- Introduce CI/CD for deployment

## Project Structure

```text
ecommerce-customer-intelligence/
│
├── data/
│   ├── customers.csv
│   ├── products.csv
│   ├── orders.csv
│   ├── order_items.csv
│   ├── payments.csv
│   ├── returns.csv
│   ├── customer_activity.csv
│   ├── reviews.csv
│   └── fraud_signals.csv
│
├── databricks/
│   ├── bronze_ingestion
│   ├── silver_transformation
│   └── gold_analytics
│
├── snowflake/
│   ├── database_schema.sql
│   ├── external_tables.sql
│   └── analytical_views.sql
│
├── powerbi/
│   └── ecommerce_dashboard.pbix
│
└── README.md

