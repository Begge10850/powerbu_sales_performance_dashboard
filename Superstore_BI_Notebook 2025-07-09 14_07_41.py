# Databricks notebook source
# Load the uploaded train.csv file
df = spark.table("train")

# Show the first few rows1
display(df)

# COMMAND ----------

# Display schema
df.printSchema()

# COMMAND ----------

#Dropping unecessary columns
df_cleaned = df.drop("Row ID", "Product ID", "Postal Code", "Customer ID")

display(df_cleaned)

# COMMAND ----------

#Displaying new schema
df_cleaned.printSchema()

# COMMAND ----------

from pyspark.sql.functions import count, when, isnull

#Checking for null values
df_cleaned.select([
    count(when(isnull(c), c)).alias(c) for c in df_cleaned.columns
]).display()

# COMMAND ----------

#Creating a cleaned dataframe as temporary SQL table
df_cleaned.createOrReplaceTempView("sales_data")

# COMMAND ----------

# MAGIC %sql
SELECT * FROM sales_data
# MAGIC LIMIT 10

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC /*
Question 1: What is the total sales for each region?
Answer: West has the highest
# MAGIC */

SELECT Region, Round(SUM(Sales), 2) AS Total_Sales
FROM sales_data
GROUP BY Region
ORDER BY Total_Sales DESC

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC /*
Question 2: What is the total sales for each category?
Answer: Technology has the highest
# MAGIC */

SELECT Category, Round(SUM(SALES), 2) AS Total_Sales
FROM sales_data
GROUP BY Category
ORDER BY Total_Sales DESC

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC /*
Question 3: What is the monthly sales trend?
# MAGIC */
                                   
SELECT 
 date_format(to_date(`Order Date`, 'yyyy-MM-dd'), 'yyyy-MM') AS Month,
 ROUND(SUM(Sales), 2) AS Total_Sales
FROM sales_data
GROUP BY Month
ORDER BY Month

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC /*
Question 4: What is the total sales for each product?
# MAGIC */

SELECT 
 `Sub-Category`, 
 ROUND(SUM(Sales), 2) AS Total_Sales
FROM sales_data
GROUP BY `Sub-Category`
ORDER BY Total_Sales DESC

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC /*
Question 5: Who are the top 10 customers by total revenue?
Answer: Sean Miller is the top customer
# MAGIC */

SELECT 
 `Customer Name`, 
 ROUND(SUM(Sales), 2) AS Total_Sales
FROM sales_data
GROUP BY `Customer Name`
ORDER BY Total_Sales DESC
LIMIT 10

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC /*
Question 6: What is the total sales by State?
Answer: California has the highest purchases
# MAGIC */

SELECT 
 State, 
 ROUND(SUM(Sales), 2) AS Total_Sales
FROM sales_data
GROUP BY State
ORDER BY Total_Sales DESC

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC /*
Question 7: Which shipping mode is used most frequently?
Answer: Standard Class is the most used shipping mode
# MAGIC */
                             
SELECT 
 `Ship Mode`, 
 COUNT(*) AS Number_of_Orders
FROM sales_data
GROUP BY `Ship Mode`
ORDER BY Number_of_Orders DESC

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC /*
Question 8: What are the total sales by Sub-Category?
Answer: Fasteners has the lowest sales
# MAGIC */

SELECT 
 `Sub-Category`, 
 ROUND(SUM(Sales), 2) AS Total_Sales
FROM sales_data
GROUP BY `Sub-Category`
ORDER BY Total_Sales ASC
LIMIT 5

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC /*
Question 9: What is the average order value by customer segment?
Answer: Consumer has the highest average order value
# MAGIC */

SELECT 
 Segment,
 ROUND(SUM(Sales) / COUNT(`Order ID`), 2) AS Avg_Order_Value
FROM sales_data
GROUP BY Segment
ORDER BY Avg_Order_Value DESC

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC /*
Question 10: What is the total sales by month and segment?
# MAGIC */

SELECT 
 date_format(to_date(`Order Date`, 'yyyy-MM-dd'), 'yyyy-MM') AS Month,
 Segment,
ROUND(SUM(Sales), 2) AS Total_Sales
FROM sales_data
GROUP BY Month, Segment
ORDER BY Month, Segment

# COMMAND ----------

display(df_cleaned)

# COMMAND ----------

