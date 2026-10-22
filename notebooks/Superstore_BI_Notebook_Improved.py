# Databricks notebook source
# /// script
# [tool.databricks.environment]
# environment_version = "6"
# ///
# MAGIC %md
# MAGIC # Sales Performance Analytics - validated workflow
# MAGIC
# MAGIC **Business goal:** describe sales performance across time, customer segment,
# MAGIC geography, product category, and shipping mode for the US retail dataset.
# MAGIC
# MAGIC This notebook separates line items from orders, validates the source before
# MAGIC analysis, and avoids profitability, ROI, and conversion claims that the
# MAGIC available fields cannot support.

# COMMAND ----------

# MAGIC %md
# MAGIC ## 1. Load the registered source table
# MAGIC
# MAGIC Expected source: a Databricks table named `train` containing 9,800 sales
# MAGIC line items. Loading a table does not change it.

# COMMAND ----------

from pyspark.sql import functions as F
from pyspark.sql.types import DoubleType

sales_raw = spark.table("train")

print(f"Source rows: {sales_raw.count():,}")
display(sales_raw.limit(10))

# COMMAND ----------

# MAGIC %md
# MAGIC ## 2. Validate schema and source assumptions

# COMMAND ----------

required_columns = {
    "Row ID", "Order ID", "Order Date", "Ship Date", "Ship Mode",
    "Customer ID", "Customer Name", "Segment", "Country", "City",
    "State", "Postal Code", "Region", "Product ID", "Category",
    "Sub-Category", "Product Name", "Sales"
}

missing_required_columns = sorted(required_columns - set(sales_raw.columns))
assert not missing_required_columns, (
    f"Missing required columns: {missing_required_columns}"
)

sales_raw.printSchema()

# COMMAND ----------

# MAGIC %md
# MAGIC ## 3. Prepare analysis fields
# MAGIC
# MAGIC Identifier fields that are not needed for the present analysis are removed
# MAGIC from the presentation dataset. No rows are removed. Stable customer and
# MAGIC product identifiers should be retained in a future dimensional model.

# COMMAND ----------

sales_clean = (
    sales_raw
    .drop("Row ID", "Product ID", "Postal Code", "Customer ID")
    .withColumn("Order Date", F.to_date(F.col("Order Date")))
    .withColumn("Ship Date", F.to_date(F.col("Ship Date")))
    .withColumn("Sales", F.col("Sales").cast(DoubleType()))
    .withColumn("Order Year", F.year(F.col("Order Date")))
    .withColumn("Order Month", F.date_format(F.col("Order Date"), "yyyy-MM"))
)

source_rows = sales_raw.count()
clean_rows = sales_clean.count()
assert source_rows == clean_rows, (
    f"Unexpected row-count change: source={source_rows}, clean={clean_rows}"
)

print(f"Prepared rows: {clean_rows:,}")
display(sales_clean.limit(10))

# COMMAND ----------

# MAGIC %md
# MAGIC ## 4. Data-quality checks
# MAGIC
# MAGIC An order may legitimately contain several product lines. Therefore repeated
# MAGIC Order IDs are not duplicates. Exact-row duplicates are reported for review
# MAGIC and are not removed automatically.

# COMMAND ----------

null_counts = sales_clean.select([
    F.sum(F.col(column).isNull().cast("int")).alias(column)
    for column in sales_clean.columns
])

exact_duplicate_rows = clean_rows - sales_clean.dropDuplicates().count()
non_positive_sales = sales_clean.filter(F.col("Sales") <= 0).count()
invalid_ship_dates = sales_clean.filter(
    F.col("Ship Date") < F.col("Order Date")
).count()

quality_summary = sales_clean.agg(
    F.countDistinct("Order ID").alias("distinct_orders"),
    F.countDistinct("Customer Name").alias("distinct_customers"),
    F.min("Order Date").alias("first_order_date"),
    F.max("Order Date").alias("last_order_date"),
    F.round(F.sum("Sales"), 2).alias("total_sales")
).first().asDict()

validation_results = {
    "source_rows": source_rows,
    "prepared_rows": clean_rows,
    "exact_duplicate_rows_for_review": exact_duplicate_rows,
    "non_positive_sales_rows": non_positive_sales,
    "ship_date_before_order_date_rows": invalid_ship_dates,
    **quality_summary,
}

print(validation_results)
display(null_counts)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 5. Create the temporary analytical view

# COMMAND ----------

sales_clean.createOrReplaceTempView("sales_data")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 6. Headline KPIs
# MAGIC
# MAGIC `line_items` counts product rows. `orders` counts distinct Order IDs.

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT
# MAGIC   COUNT(*) AS line_items,
# MAGIC   COUNT(DISTINCT `Order ID`) AS orders,
# MAGIC   ROUND(SUM(Sales), 2) AS total_sales,
# MAGIC   ROUND(SUM(Sales) / COUNT(DISTINCT `Order ID`), 2) AS sales_per_order
# MAGIC FROM sales_data

# COMMAND ----------

# MAGIC %md
# MAGIC ## 7. Sales by region

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT Region, ROUND(SUM(Sales), 2) AS total_sales
# MAGIC FROM sales_data
# MAGIC GROUP BY Region
# MAGIC ORDER BY total_sales DESC

# COMMAND ----------

# MAGIC %md
# MAGIC ## 8. Sales by category and sub-category

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT Category, ROUND(SUM(Sales), 2) AS total_sales
# MAGIC FROM sales_data
# MAGIC GROUP BY Category
# MAGIC ORDER BY total_sales DESC

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT `Sub-Category`, ROUND(SUM(Sales), 2) AS total_sales
# MAGIC FROM sales_data
# MAGIC GROUP BY `Sub-Category`
# MAGIC ORDER BY total_sales DESC

# COMMAND ----------

# MAGIC %md
# MAGIC ## 9. Chronological monthly sales
# MAGIC
# MAGIC Year-month is retained so this is a continuous trend, not an aggregation of
# MAGIC all Januaries, Februaries, and so on.

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT `Order Month` AS month, ROUND(SUM(Sales), 2) AS total_sales
# MAGIC FROM sales_data
# MAGIC GROUP BY `Order Month`
# MAGIC ORDER BY month

# COMMAND ----------

# MAGIC %md
# MAGIC ## 10. Customers and states by recorded sales

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT `Customer Name`, ROUND(SUM(Sales), 2) AS total_sales
# MAGIC FROM sales_data
# MAGIC GROUP BY `Customer Name`
# MAGIC ORDER BY total_sales DESC
# MAGIC LIMIT 10

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT State, ROUND(SUM(Sales), 2) AS total_sales
# MAGIC FROM sales_data
# MAGIC GROUP BY State
# MAGIC ORDER BY total_sales DESC

# COMMAND ----------

# MAGIC %md
# MAGIC ## 11. Shipping mode by distinct orders
# MAGIC
# MAGIC The original notebook used `COUNT(*)`, which counted line items while
# MAGIC labelling them as orders. This query uses distinct Order IDs.

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT
# MAGIC   `Ship Mode`,
# MAGIC   COUNT(DISTINCT `Order ID`) AS number_of_orders
# MAGIC FROM sales_data
# MAGIC GROUP BY `Ship Mode`
# MAGIC ORDER BY number_of_orders DESC

# COMMAND ----------

# MAGIC %md
# MAGIC ## 12. Correct average order value by segment
# MAGIC
# MAGIC First aggregate product lines into orders, then average the order totals.

# COMMAND ----------

# MAGIC %sql
# MAGIC WITH order_totals AS (
# MAGIC   SELECT
# MAGIC     Segment,
# MAGIC     `Order ID`,
# MAGIC     SUM(Sales) AS order_sales
# MAGIC   FROM sales_data
# MAGIC   GROUP BY Segment, `Order ID`
# MAGIC )
# MAGIC SELECT
# MAGIC   Segment,
# MAGIC   COUNT(*) AS number_of_orders,
# MAGIC   ROUND(AVG(order_sales), 2) AS average_order_value
# MAGIC FROM order_totals
# MAGIC GROUP BY Segment
# MAGIC ORDER BY average_order_value DESC

# COMMAND ----------

# MAGIC %md
# MAGIC ## 13. Monthly sales by segment

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT
# MAGIC   `Order Month` AS month,
# MAGIC   Segment,
# MAGIC   ROUND(SUM(Sales), 2) AS total_sales
# MAGIC FROM sales_data
# MAGIC GROUP BY `Order Month`, Segment
# MAGIC ORDER BY month, Segment

# COMMAND ----------

# MAGIC %md
# MAGIC ## 14. Complete export for Power BI
# MAGIC
# MAGIC Do **not** download the visible Databricks result table: it may be truncated.
# MAGIC Write the dataframe programmatically and confirm the exported row count.
# MAGIC Change the path if an existing folder must be preserved.

# COMMAND ----------

catalog_name = spark.sql(
    "SELECT current_catalog()"
).first()[0]

schema_name = spark.sql(
    "SELECT current_schema()"
).first()[0]

table_name = "superstore_sales_clean_v2"

powerbi_sales = sales_clean.select(
    F.col("Order ID").alias("order_id"),
    F.col("Order Date").alias("order_date"),
    F.col("Ship Date").alias("ship_date"),
    F.col("Ship Mode").alias("ship_mode"),
    F.col("Customer Name").alias("customer_name"),
    F.col("Segment").alias("segment"),
    F.col("Country").alias("country"),
    F.col("City").alias("city"),
    F.col("State").alias("state"),
    F.col("Region").alias("region"),
    F.col("Category").alias("category"),
    F.col("Sub-Category").alias("sub_category"),
    F.col("Product Name").alias("product_name"),
    F.col("Sales").alias("sales"),
    F.col("Order Year").alias("order_year"),
    F.col("Order Month").alias("order_month"),
)

full_table_name = (
    f"{catalog_name}.{schema_name}.{table_name}"
)

if spark.catalog.tableExists(full_table_name):
    raise ValueError(
        f"Safety stop: {full_table_name} already exists. "
        "Choose a new versioned name instead of overwriting it."
    )

(
    powerbi_sales
    .write
    .format("delta")
    .saveAsTable(full_table_name)
)

published_table = spark.table(full_table_name)

published_summary = published_table.agg(
    F.count("*").alias("rows"),
    F.countDistinct("order_id").alias("orders"),
    F.round(F.sum("sales"), 2).alias("total_sales")
).first().asDict()

assert published_summary["rows"] == clean_rows, (
    f"Published row-count validation failed: "
    f"{published_summary}"
)

assert (
    published_summary["orders"]
    == quality_summary["distinct_orders"]
), (
    f"Published order-count validation failed: "
    f"{published_summary}"
)

assert (
    published_summary["total_sales"]
    == quality_summary["total_sales"]
), (
    f"Published sales validation failed: "
    f"{published_summary}"
)

print(f"Published table: {full_table_name}")
print(published_summary)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 15. Interpretation boundaries
# MAGIC
# MAGIC Supported conclusions describe recorded sales, order counts, customer and
# MAGIC product concentration, shipping usage, and time patterns.
# MAGIC
# MAGIC This dataset does **not** contain profit, cost, margin, campaign spend,
# MAGIC impressions, conversion, inventory, or returns. It therefore cannot establish
# MAGIC profitability, marketing ROI, conversion performance, or causal impact.