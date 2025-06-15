# Databricks notebook source
from pyspark.sql import SparkSession
from pyspark.sql.functions import (col, upper, hash as hashkey, cast)

# COMMAND ----------

spark= SparkSession.builder.getOrCreate()

# COMMAND ----------

df_sales= spark.read.option("header", True).option("inferSchema", True).csv("/FileStore/tables/fact_sales.csv")

# COMMAND ----------

display(df_sales)

# COMMAND ----------

df_ProductSubCategory = df_sales.select([
    col("ProductSubCategory")
]).distinct()

# COMMAND ----------

df_ProductSubCategoryId = df_ProductSubCategory.withColumn("ProductSubCategoryId", hashkey(upper(col("ProductSubCategory"))).cast("bigint"))

# COMMAND ----------

df_ProductSubCategoryIdNA= spark.createDataFrame([
    ("N/A", -1)
], ["ProductSubCategory", "ProductSubCategoryId"])

# COMMAND ----------

df_dimProductSubCategory = df_ProductSubCategoryId.union(df_ProductSubCategoryIdNA)

# COMMAND ----------

df_dimProductSubCategory.write.format("delta").mode("overwrite").save("/FileStore/tables/DIM_ProductSubCategory")