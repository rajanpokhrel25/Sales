# Databricks notebook source
from pyspark.sql import SparkSession
from pyspark.sql.functions import (col, upper, cast, hash as hashkey)

# COMMAND ----------

spark= SparkSession.builder.getOrCreate()

# COMMAND ----------

df_sales= spark.read.option("header", True).option("inferSchema", True).csv("/FileStore/tables/fact_sales.csv")

# COMMAND ----------

df_CustomerSegment = df_sales.select([
    col("CustomerSegment")
]).distinct()

# COMMAND ----------

df_CustomerSegmentId = df_CustomerSegment.withColumn("CustomerSegmentId", hashkey(upper(col("CustomerSegment"))).cast("bigint"))

# COMMAND ----------

df_CustomerSegmentNA = spark.createDataFrame([
    ("N/A", -1)
], ["CustomerSegment", "CustomerSegmentId"])

# COMMAND ----------

df_dimCustomerSegment= df_CustomerSegmentId.union(df_CustomerSegmentNA)

# COMMAND ----------

display(df_dimCustomerSegment)

# COMMAND ----------

df_dimCustomerSegment.write.format("delta").mode("overwrite").save("/FileStore/tables/DIM_DimCustomerSegment")