# Databricks notebook source
from pyspark.sql import SparkSession
from pyspark.sql.functions import (col, hash as hashKey, upper)

# COMMAND ----------

spark = SparkSession.builder.getOrCreate()

# COMMAND ----------

df_sales = spark.read.option("header", True).option("inferSchema", True).csv("/FileStore/tables/fact_sales.csv")

# COMMAND ----------

display(df_sales)

# COMMAND ----------

df_productCaategory = df_sales.select([
    col("ProductCategory")
]).distinct()

# COMMAND ----------

df_productCaategoryId = df_productCaategory.withColumn("ProductCategoryId", hashKey(upper(col("ProductCategory"))).cast("bigint"))

# COMMAND ----------

df_productCaategoryNA = spark.createDataFrame([
    ("N/A", -1)
], ["ProductCategory", "ProductCategoryId"])

# COMMAND ----------

df_dim_ProductCategory= df_productCaategoryId.union(df_productCaategoryNA)

# COMMAND ----------

display(df_dim_ProductCategory)

# COMMAND ----------

df_dim_ProductCategory.write.format("delta").mode("overwrite").save("/FileStore/tables/DIM_ProductCategory")