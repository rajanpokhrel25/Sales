# Databricks notebook source
from pyspark.sql import SparkSession
from pyspark.sql.functions import (col, upper, hash as hashkey, cast)

# COMMAND ----------

spark= SparkSession.builder.getOrCreate()

# COMMAND ----------

df_sales= spark.read.option("header", True).option("inferSchema", True).csv("/FileStore/tables/fact_sales.csv")

# COMMAND ----------

df_SalesChannel = df_sales.select([
    col("SalesChannel")
]).distinct()

# COMMAND ----------

df_SalesChannelId = df_SalesChannel.withColumn("SalesChannelId", hashkey(upper(col("SalesChannel"))).cast("bigint"))

# COMMAND ----------

df_SalesChannelNA= spark.createDataFrame([
    ("N/A", -1)
],[ "SalesChannel", "SalesChannelId"])

# COMMAND ----------

df_dimSalesChannel= df_SalesChannelId.union(df_SalesChannelNA)

# COMMAND ----------

df_dimSalesChannel.write.format("delta").mode("overwrite").save("/FileStore/tables/DIM_SalesChannel")