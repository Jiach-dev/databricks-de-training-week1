# Databricks notebook source
# MAGIC %md
# MAGIC # Week 1 Sales Data Exploration
# MAGIC
# MAGIC This notebook provides an initial exploration of the Week 1 sales dataset. The goal is to understand the structure of the data, identify any missing or inconsistent values, generate basic summary statistics, and derive preliminary insights that can guide further analysis and business decisions.
# MAGIC

# COMMAND ----------

print("Hello, Databricks!")


# COMMAND ----------

my_name = "Jean-Hénock VIAYINON"
print(my_name)


# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT "Hello from SQL!" AS message;
# MAGIC

# COMMAND ----------

sales_df = spark.table("bricks_space.default.mock_sales_data")
sales_df.show()


# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM bricks_space.default.mock_sales_data
# MAGIC

# COMMAND ----------

# MAGIC %sql
# MAGIC DESCRIBE EXTENDED bricks_space.default.mock_sales_data;
# MAGIC

# COMMAND ----------

# MAGIC %md
# MAGIC Action 3.3: Basic DataFrame Exploration

# COMMAND ----------

sales_df.printSchema()

# COMMAND ----------

sales_df.count()

# COMMAND ----------

sales_df.head(5)

# COMMAND ----------

sales_df.limit(5).show()

# COMMAND ----------

sales_df.describe().show()

# COMMAND ----------

sales_df.select("TransactionID", "Product", "SaleAmount").show(5)

# COMMAND ----------

# MAGIC %md
# MAGIC Task 4: Basic Data Transformation & Cleansing with Spark SQL & PySpark (Est. 10 hours)
# MAGIC Objective: Perform basic data transformations and cleansing operations on the sales DataFrame.
# MAGIC
# MAGIC

# COMMAND ----------

# MAGIC %md
# MAGIC Action 4.1: Using Spark SQL for Exploration
# MAGIC
# MAGIC To use Spark SQL directly on your DataFrame, you first need to create a temporary view.
# MAGIC ##### Create a temporary view from the DataFrame

# COMMAND ----------

sales_df.createOrReplaceTempView("sales_data_view")

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC SELECT * FROM sales_data_view LIMIT 10;

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC SELECT Product, SUM(SaleAmount) as TotalSales
# MAGIC
# MAGIC FROM sales_data_view
# MAGIC
# MAGIC GROUP BY Product
# MAGIC
# MAGIC ORDER BY TotalSales DESC;

# COMMAND ----------

# MAGIC %md
# MAGIC Action 4.2: Data Type Conversion (PySpark)

# COMMAND ----------

from pyspark.sql.functions import col, to_timestamp

# Assuming TransactionDate is in 'yyyy-MM-dd HH:mm:ss' format

# If it's just date 'yyyy-MM-dd', use to_date and adjust format if needed.

sales_df_transformed = sales_df.withColumn("TransactionTimestamp", to_timestamp(col("TransactionDate"), "yyyy-MM-dd HH:mm:ss"))

sales_df_transformed.printSchema()

sales_df_transformed.select("TransactionID", "TransactionDate", "TransactionTimestamp").show(5, truncate=False)

# COMMAND ----------

from pyspark.sql.types import DoubleType, IntegerType

# Example: if SaleAmount was a string

sales_df_transformed = sales_df_transformed.withColumn("SaleAmount", col("SaleAmount").cast(DoubleType()))

sales_df_transformed = sales_df_transformed.withColumn("Quantity", col("Quantity").cast(IntegerType()))

sales_df_transformed.printSchema()

# COMMAND ----------

sales_df_transformed = sales_df_transformed.fillna({"PaymentMethod": "Unknown"})

# Verify by showing rows where PaymentMethod might have been null

# (You might need to adjust the filter if you know specific TransactionIDs had nulls)

sales_df_transformed.filter(col("PaymentMethod") == "Unknown").show()

# COMMAND ----------

sales_df_transformed.dropna().count()

# COMMAND ----------

# MAGIC %md
# MAGIC Action 4.4: Creating New Columns (PySpark)
# MAGIC
# MAGIC

# COMMAND ----------

from pyspark.sql.functions import round

# Ensure Quantity is not zero to avoid division by zero if SaleAmount is total price

# For this exercise, we'll assume Quantity is always > 0 for simplicity

# If SaleAmount is already unit price, this step is not needed or can be adapted.

# Let's assume SaleAmount is total for the quantity, so UnitPrice = SaleAmount / Quantity

sales_df_transformed = sales_df_transformed.withColumn("UnitPrice", round(col("SaleAmount") / col("Quantity"), 2))

sales_df_transformed.select("Product", "Quantity", "SaleAmount", "UnitPrice").show(5)

# COMMAND ----------

# MAGIC %md
# MAGIC Action 4.5: Filtering and Ordering (PySpark)

# COMMAND ----------

credit_card_sales_df = sales_df_transformed.filter(col("PaymentMethod") == "Credit Card").orderBy(col("SaleAmount").desc())

credit_card_sales_df.show(10)

# COMMAND ----------

final_sales_df = sales_df_transformed

# COMMAND ----------

# MAGIC %md
# MAGIC Task 5: Storing Processed Data & Version Control Basics (Est. 6 hours)
# MAGIC Objective: Save your transformed DataFrame to DBFS and commit your notebook to a Git repository.

# COMMAND ----------

# Define the output path in DBFS

output_path_parquet = "/FileStore/tables/innovateretail/processed_sales_parquet"

# Write the DataFrame to Parquet format

# "overwrite" mode will replace data if it already exists

final_sales_df.write.mode("overwrite").parquet(output_path_parquet)

print(f"DataFrame saved to Parquet at: {output_path_parquet}")

# COMMAND ----------

# MAGIC %md
# MAGIC Action 5.2: Set up Git and GitHub Repository
# MAGIC --> databricks-de-training-week1
# MAGIC

# COMMAND ----------

