# Databricks notebook source
df=(spark.read.format("csv")
.option("header","true")
.option("inferSchema","true")
.option("path","/FileStore/tables/emtest.csv").load())
df.write.format("parquet").save("/dbfs/path/to/data1")

# COMMAND ----------

# Load data from DBFS to Snowflake
from pyspark.sql import SparkSession
from pyspark.sql.functions import col

# Initialize Spark session
spark = SparkSession.builder.appName("SnowflakeLoad").getOrCreate()

# Read data from DBFS
df = spark.read.format("parquet").load("/dbfs/path/to/data1")

# Write data to Snowflake
df.write \
  .format("snowflake") \
  .option("sfURL", "https://skgqomq-tx87376.snowflakecomputing.com") \
  .option("sfUser", "Rahul") \
  .option("sfPassword", "Rahul@123") \
  .option("sfDatabase", "sales1001") \
  .option("sfSchema", "PUBLIC") \
  .option("sfWarehouse", "NEW101") \
  .option("dbtable", "emp2") \
  .save()


# COMMAND ----------


