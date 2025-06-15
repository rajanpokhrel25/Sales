# Databricks notebook source
from pyspark.sql import SparkSession
from pyspark.sql.functions import (col,upper, hash as hashKey)

# COMMAND ----------

spark = SparkSession.builder.getOrCreate()

# COMMAND ----------

df_sales = spark.read.option("header", True).option("interSchema", True).csv("/FileStore/tables/fact_sales.csv")

# COMMAND ----------

display(df_sales)

# COMMAND ----------

df_Region = df_sales.select([
    col("Region")
]).distinct()

# COMMAND ----------

df_RegionId = df_Region.withColumn("RegionId", hashKey(upper(col("Region"))).cast("bigint"))

# COMMAND ----------

df_RegionNA= spark.createDataFrame([
    ("N/A", -1)
], ["Region","RegionId"])

# COMMAND ----------

df_dimRegion= df_RegionNA.union(df_RegionId)

# COMMAND ----------

display(df_dimRegion)

# COMMAND ----------

df_dimRegion.write.format("delta").mode("overwrite").save("/FileStore/tables/DIM_Region")