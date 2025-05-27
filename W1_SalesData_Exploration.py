# Databricks notebook source
# MAGIC %md
# MAGIC # Week 1 Sales Data Exploration
# MAGIC
# MAGIC This notebook explores the Week 1 sales dataset to understand its structure, identify issues, and perform initial transformations. We will:
# MAGIC
# MAGIC - Load and inspect data
# MAGIC - Clean missing values
# MAGIC - Convert data types
# MAGIC - Create new features
# MAGIC - Save processed data for future use
# MAGIC
# MAGIC

# COMMAND ----------

# Print a simple message to verify that the notebook is running successfully
print("Hello, Databricks!")


# COMMAND ----------

# Assigning my name to a variable and print it out as a basic Python variable example
my_name = "Jean-Hénock VIAYINON"
print(my_name)


# COMMAND ----------

# MAGIC %sql
# MAGIC -- This SQL statement selects a static string and labels it as 'message'.
# MAGIC -- It's a simple example to test SQL integration in the Databricks notebook.
# MAGIC
# MAGIC SELECT "Hello from SQL!" AS message;
# MAGIC

# COMMAND ----------

# MAGIC %md
# MAGIC ### Loading the Sales Data Table
# MAGIC
# MAGIC We load the `mock_sales_data` table from the `bricks_space.default` catalog using Spark. This allows us to explore and manipulate the data using PySpark. The `.show()` method displays the first few rows for an initial look at the dataset.

# COMMAND ----------

# Load the mock sales data table into a Spark DataFrame
sales_df = spark.table("bricks_space.default.mock_sales_data")

# Display the first few rows of the DataFrame
sales_df.show()


# COMMAND ----------

# MAGIC %md
# MAGIC ### Exploring the Full Sales Dataset
# MAGIC
# MAGIC This SQL query retrieves all records from the `mock_sales_data` table to allow a full inspection of the dataset directly within the notebook environment.
# MAGIC

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Retrieve all rows from the mock_sales_data table
# MAGIC %sql
# MAGIC SELECT * FROM bricks_space.default.mock_sales_data
# MAGIC

# COMMAND ----------

# MAGIC %md
# MAGIC ### Inspecting Table Metadata
# MAGIC
# MAGIC This command uses `DESCRIBE EXTENDED` to view the structure and metadata of the `mock_sales_data` table, including column data types, table location, and additional properties. This helps ensure we understand the schema before performing transformations.
# MAGIC

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Display detailed schema and metadata for the mock_sales_data table
# MAGIC %sql
# MAGIC DESCRIBE EXTENDED bricks_space.default.mock_sales_data;
# MAGIC

# COMMAND ----------

# MAGIC %md
# MAGIC Action 3.3: Basic DataFrame Exploration

# COMMAND ----------

# MAGIC %md
# MAGIC ### Viewing the DataFrame Schema
# MAGIC
# MAGIC We use `printSchema()` to display the structure of the `sales_df` DataFrame. This shows each column name along with its data type and nullability. Understanding the schema is essential before performing any_
# MAGIC

# COMMAND ----------

# Display the schema of the sales DataFrame to understand column names, data types, and nullability
sales_df.printSchema()

# COMMAND ----------

# MAGIC %md
# MAGIC ### Counting Records
# MAGIC
# MAGIC We use `.count()` to determine the total number of records (rows) in the `sales_df` DataFrame. This helps assess the size of the dataset we're working with.
# MAGIC

# COMMAND ----------

# Count the total number of rows in the DataFrame
sales_df.count()

# COMMAND ----------

# MAGIC %md
# MAGIC ### Viewing the First 5 Records (Python List Format)
# MAGIC
# MAGIC Using `.head(5)` returns the first 5 rows of the DataFrame as a list of Row objects. This is useful for quickly inspecting the raw data in Python without the formatting of `.show()`.
# MAGIC

# COMMAND ----------

# Display the first 5 rows of the DataFrame as a list of Row objects
sales_df.head(5)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Previewing the First 5 Records with `limit()` and `show()`
# MAGIC
# MAGIC We use `.limit(5)` to restrict the DataFrame to the first 5 rows, and `.show()` to display them in a neatly formatted table. This is useful for getting a quick visual sense of the
# MAGIC

# COMMAND ----------

# Display the first 5 rows of the DataFrame in a tabular format
sales_df.limit(5).show()

# COMMAND ----------

# MAGIC %md
# MAGIC ### Generating Summary Statistics with `describe()`
# MAGIC
# MAGIC The `.describe()` method provides basic statistical summaries for all numeric columns in the DataFrame—such as count, mean, standard deviation, min, and max. Using `.show()` displays the result in a readable tabular format
# MAGIC

# COMMAND ----------

# Generate and display summary statistics for numeric columns
sales_df.describe().show()

# COMMAND ----------

# MAGIC %md
# MAGIC ### Preview Selected Columns
# MAGIC
# MAGIC To get a focused view of specific columns, we use the `.select()` method. Here, we retrieve only the `TransactionID`, `Product`, and `SaleAmount` columns. This is helpful for a quick glance at important transactional details without the clutter of all other fields
# MAGIC

# COMMAND ----------

# Display the first 5 rows with only TransactionID, Product, and SaleAmount columns
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

# MAGIC %md
# MAGIC ### Create Temporary SQL View
# MAGIC
# MAGIC To use Spark SQL queries on the DataFrame, we first register it as a temporary view. This enables us to run SQL syntax directly on our DataFrame using the `%sql` magic command or `spark.sql()` in PySpark.
# MAGIC
# MAGIC The view will only exist during the current session.
# MAGIC

# COMMAND ----------

# Register the DataFrame as a temporary SQL view for querying with Spark SQL
sales_df.createOrReplaceTempView("sales_data_view")

# COMMAND ----------

# MAGIC %md
# MAGIC ### Querying the Temporary View
# MAGIC
# MAGIC This SQL query retrieves the first 10 rows from the temporary view `sales_data_view` we created from our sales DataFrame. 
# MAGIC
# MAGIC This helps us quickly inspect a sample of the dataset using familiar SQL syntax.
# MAGIC

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Select the first 10 rows from the sales_data_view temporary view
# MAGIC SELECT * FROM sales_data_view LIMIT 10;

# COMMAND ----------

# MAGIC %md
# MAGIC ### Total Sales by Product
# MAGIC
# MAGIC This SQL query calculates the total sales amount for each product by summing the `SaleAmount` grouped by `Product`. 
# MAGIC
# MAGIC The results are ordered in descending order of `TotalSales` so we can easily identify the top-selling products.
# MAGIC

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Calculate total sales amount per product, ordered by highest sales first
# MAGIC SELECT Product, SUM(SaleAmount) as TotalSales
# MAGIC FROM sales_data_view
# MAGIC GROUP BY Product
# MAGIC ORDER BY TotalSales DESC;
# MAGIC

# COMMAND ----------

# MAGIC %md
# MAGIC Action 4.2: Data Type Conversion (PySpark)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Convert TransactionDate to Timestamp
# MAGIC
# MAGIC This code converts the `TransactionDate` column from a string to a proper timestamp format using the `to_timestamp` function.
# MAGIC
# MAGIC - Assumes `TransactionDate` is in the format `'yyyy-MM-dd HH:mm:ss'`.
# MAGIC - Creates a new column `TransactionTimestamp` with the converted timestamp.
# MAGIC - Prints the updated schema to verify the new column type.
# MAGIC - Displays the first 5 rows to compare original and converted date values.
# MAGIC

# COMMAND ----------

from pyspark.sql.functions import col, to_timestamp

# Convert the 'TransactionDate' string to a timestamp using the specified format
sales_df_transformed = sales_df.withColumn(
    "TransactionTimestamp", 
    to_timestamp(col("TransactionDate"), "yyyy-MM-dd HH:mm:ss")
)

# Show the schema to confirm that 'TransactionTimestamp' has been added with TimestampType
sales_df_transformed.printSchema()

# Display first 5 rows showing original and converted date columns side-by-side
sales_df_transformed.select("TransactionID", "TransactionDate", "TransactionTimestamp").show(5, truncate=False)


# COMMAND ----------

# MAGIC %md
# MAGIC ### Cast Columns to Appropriate Data Types
# MAGIC
# MAGIC - Convert the `SaleAmount` column to `DoubleType` to ensure it is treated as a numeric (decimal) value.
# MAGIC - Convert the `Quantity` column to `IntegerType` to ensure it is treated as an integer.
# MAGIC - Printing the schema confirms that the data types have been updated correctly.
# MAGIC

# COMMAND ----------

from pyspark.sql.types import DoubleType, IntegerType

# Cast 'SaleAmount' from string (if applicable) to Double (decimal number)
sales_df_transformed = sales_df_transformed.withColumn("SaleAmount", col("SaleAmount").cast(DoubleType()))

# Cast 'Quantity' from string (if applicable) to Integer
sales_df_transformed = sales_df_transformed.withColumn("Quantity", col("Quantity").cast(IntegerType()))

# Print schema to verify the updated data types for 'SaleAmount' and 'Quantity'
sales_df_transformed.printSchema()


# COMMAND ----------

# MAGIC %md
# MAGIC ### Handling Missing Values in the `PaymentMethod` Column
# MAGIC
# MAGIC - Replace any `null` or missing values in the `PaymentMethod` column with the string `"Unknown"`.
# MAGIC - This ensures that subsequent analysis or grouping on `PaymentMethod` won't be affected by missing data.
# MAGIC - Verify the replacement by filtering and displaying rows where `PaymentMethod` is `"Unknown"`.
# MAGIC

# COMMAND ----------

sales_df_transformed = sales_df_transformed.fillna({"PaymentMethod": "Unknown"})

# Verify by showing rows where PaymentMethod might have been null

# (We might need to adjust the filter if you know specific TransactionIDs had nulls)

sales_df_transformed.filter(col("PaymentMethod") == "Unknown").show()

# COMMAND ----------

# Count the number of rows in the DataFrame after dropping any rows that contain null values in any column
# This helps us understand how many complete (non-missing) records we have after cleaning
sales_df_transformed.dropna().count()

# COMMAND ----------

# MAGIC %md
# MAGIC Action 4.4: Creating New Columns (PySpark)
# MAGIC
# MAGIC

# COMMAND ----------

from pyspark.sql.functions import round

# Calculate the UnitPrice by dividing the total SaleAmount by the Quantity sold
# Assuming Quantity is always greater than zero to avoid division by zero errors
# Round the UnitPrice to 2 decimal places for better readability
# Add the new UnitPrice column to the DataFrame

sales_df_transformed = sales_df_transformed.withColumn("UnitPrice", round(col("SaleAmount") / col("Quantity"), 2))

# Display a sample of the products with their quantities, total sale amounts, and calculated unit prices
sales_df_transformed.select("Product", "Quantity", "SaleAmount", "UnitPrice").show(5)


# COMMAND ----------

# MAGIC %md
# MAGIC Action 4.5: Filtering and Ordering (PySpark)

# COMMAND ----------

# Filter the sales data to include only transactions where the PaymentMethod is 'Credit Card'
# Then sort these transactions in descending order by the SaleAmount to see the highest sales first
# Display the top 10 credit card sales transactions

credit_card_sales_df = sales_df_transformed.filter(col("PaymentMethod") == "Credit Card").orderBy(col("SaleAmount").desc())

credit_card_sales_df.show(10)


# COMMAND ----------

# Assign the transformed sales DataFrame to final_sales_df for further processing or saving
final_sales_df = sales_df_transformed

# COMMAND ----------

# MAGIC %md
# MAGIC Task 5: Storing Processed Data & Version Control Basics (Est. 6 hours)
# MAGIC Objective: Save your transformed DataFrame to DBFS and commit your notebook to a Git repository.

# COMMAND ----------

# Define the path in DBFS where the processed sales data will be saved in Parquet format
output_path_parquet = "/FileStore/tables/innovateretail/processed_sales_parquet"

# Save the final_sales_df DataFrame to the specified path in Parquet format
# Using "overwrite" mode to replace any existing files at that location
final_sales_df.write.mode("overwrite").parquet(output_path_parquet)

# Print confirmation message with the save location
print(f"DataFrame saved to Parquet at: {output_path_parquet}")


# COMMAND ----------

# MAGIC %md
# MAGIC Action 5.2: Set up Git and GitHub Repository
# MAGIC --> databricks-de-training-week1
# MAGIC

# COMMAND ----------

