# Databricks notebook source
from pyspark.sql import SparkSession
from pyspark.sql.functions import (col, upper, cast, hash as hashkey)

# COMMAND ----------

spark= SparkSession.builder.getOrCreate()

# COMMAND ----------

df_sales= spark.read.option("header", True).option("inferSchema", True).csv("/FileStore/tables/fact_sales.csv")

# COMMAND ----------

df_SalesRep= df_sales.select([
    col("SalesRep")
]).distinct()

# COMMAND ----------

df_SalesRepId = df_SalesRep.withColumn("SalesRepId", hashkey(upper(col("SalesRep"))).cast("bigint"))

# COMMAND ----------

df_SalesRepNA= spark.createDataFrame([
    ("N/A", -1)
],["SalesRep", "SalesRepId"] )

# COMMAND ----------

df_dimSalesRep= df_SalesRepId.union(df_SalesRepNA)

# COMMAND ----------

display(df_dimSalesRep)

# COMMAND ----------

df_dimSalesRep.write.format("delta").mode("overwrite").save("/FileStore/tables/DIM_SalesRep")