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
# MAGIC SELECT * FROM sales_data
# MAGIC LIMIT 10

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC /*
# MAGIC Question 1: What is the total sales for each region?
# MAGIC Answer: West has the highest
# MAGIC */
# MAGIC
# MAGIC SELECT Region, Round(SUM(Sales), 2) AS Total_Sales
# MAGIC FROM sales_data
# MAGIC GROUP BY Region
# MAGIC ORDER BY Total_Sales DESC

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC /*
# MAGIC Question 2: What is the total sales for each category?
# MAGIC Answer: Technology has the highest
# MAGIC */
# MAGIC
# MAGIC SELECT Category, Round(SUM(SALES), 2) AS Total_Sales
# MAGIC FROM sales_data
# MAGIC GROUP BY Category
# MAGIC ORDER BY Total_Sales DESC

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC /*
# MAGIC Question 3: What is the monthly sales trend?
# MAGIC */
# MAGIC
# MAGIC SELECT 
# MAGIC   date_format(to_date(`Order Date`, 'yyyy-MM-dd'), 'yyyy-MM') AS Month,
# MAGIC   ROUND(SUM(Sales), 2) AS Total_Sales
# MAGIC FROM sales_data
# MAGIC GROUP BY Month
# MAGIC ORDER BY Month

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC /*
# MAGIC Question 4: What is the total sales for each product?
# MAGIC */
# MAGIC
# MAGIC SELECT 
# MAGIC   `Sub-Category`, 
# MAGIC   ROUND(SUM(Sales), 2) AS Total_Sales
# MAGIC FROM sales_data
# MAGIC GROUP BY `Sub-Category`
# MAGIC ORDER BY Total_Sales DESC

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC /*
# MAGIC Question 5: Who are the top 10 customers by total revenue?
# MAGIC Answer: Sean Miller is the top customer
# MAGIC */
# MAGIC
# MAGIC SELECT 
# MAGIC   `Customer Name`, 
# MAGIC   ROUND(SUM(Sales), 2) AS Total_Sales
# MAGIC FROM sales_data
# MAGIC GROUP BY `Customer Name`
# MAGIC ORDER BY Total_Sales DESC
# MAGIC LIMIT 10

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC /*
# MAGIC Question 6: What is the total sales by State?
# MAGIC Answer: California has the highest purchases
# MAGIC */
# MAGIC
# MAGIC SELECT 
# MAGIC   State, 
# MAGIC   ROUND(SUM(Sales), 2) AS Total_Sales
# MAGIC FROM sales_data
# MAGIC GROUP BY State
# MAGIC ORDER BY Total_Sales DESC

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC /*
# MAGIC Question 7: Which shipping mode is used most frequently?
# MAGIC Answer: Standard Class is the most used shipping mode
# MAGIC */
# MAGIC
# MAGIC SELECT 
# MAGIC   `Ship Mode`, 
# MAGIC   COUNT(*) AS Number_of_Orders
# MAGIC FROM sales_data
# MAGIC GROUP BY `Ship Mode`
# MAGIC ORDER BY Number_of_Orders DESC

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC /*
# MAGIC Question 8: What are the total sales by Sub-Category?
# MAGIC Answer: Fasteners has the lowest sales
# MAGIC */
# MAGIC
# MAGIC SELECT 
# MAGIC   `Sub-Category`, 
# MAGIC   ROUND(SUM(Sales), 2) AS Total_Sales
# MAGIC FROM sales_data
# MAGIC GROUP BY `Sub-Category`
# MAGIC ORDER BY Total_Sales ASC
# MAGIC LIMIT 5

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC /*
# MAGIC Question 9: What is the average order value by customer segment?
# MAGIC Answer: Consumer has the highest average order value
# MAGIC */
# MAGIC
# MAGIC SELECT 
# MAGIC   Segment,
# MAGIC   ROUND(SUM(Sales) / COUNT(`Order ID`), 2) AS Avg_Order_Value
# MAGIC FROM sales_data
# MAGIC GROUP BY Segment
# MAGIC ORDER BY Avg_Order_Value DESC

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC /*
# MAGIC Question 10: What is the total sales by month and segment?
# MAGIC */
# MAGIC
# MAGIC SELECT 
# MAGIC   date_format(to_date(`Order Date`, 'yyyy-MM-dd'), 'yyyy-MM') AS Month,
# MAGIC   Segment,
# MAGIC   ROUND(SUM(Sales), 2) AS Total_Sales
# MAGIC FROM sales_data
# MAGIC GROUP BY Month, Segment
# MAGIC ORDER BY Month, Segment

# COMMAND ----------

display(df_cleaned)

# COMMAND ----------

