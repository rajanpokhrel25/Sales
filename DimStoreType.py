# Databricks notebook source
from pyspark.sql import SparkSession
from pyspark.sql.functions import (col,upper, cast, hash as hashkey)

# COMMAND ----------

spark= SparkSession.builder.getOrCreate()

# COMMAND ----------

df_sales= spark.read.option("header", True).option("inferSchema", True).csv("/FileStore/tables/fact_sales.csv")

# COMMAND ----------

df_StoreType = df_sales.select([
    col("StoreType")
]).distinct()

# COMMAND ----------

df_StoreTypeId = df_StoreType.withColumn("StoreTypeId", hashkey(upper(col("StoreType"))).cast("bigint"))

# COMMAND ----------

df_StoreTypeNA= spark.createDataFrame([
    ("N/A", -1)
], ["StoreType", "StoreTypeId"])

# COMMAND ----------

df_dimStoreType = df_StoreTypeId.union(df_StoreTypeNA)

# COMMAND ----------

display(df_dimStoreType)

# COMMAND ----------

df_dimStoreType.write.format("delta").mode("overwrite").save("/FileStore/tables/DIM_StoreType")