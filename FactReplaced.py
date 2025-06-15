# Databricks notebook source
# MAGIC %md
# MAGIC ### Joining Dim Tables 
# MAGIC

# COMMAND ----------

# MAGIC %md 
# MAGIC #### Joining Region Dim
# MAGIC

# COMMAND ----------

from pyspark.sql import SparkSession
from pyspark.sql.functions import (col)

# COMMAND ----------

spark= SparkSession.builder.getOrCreate()

# COMMAND ----------

df_sales= spark.read.option("header", True).option("inferSchema", True).csv("/FileStore/tables/fact_sales.csv")

# COMMAND ----------

df_dimRegion = spark.read.option("header", True).option("InferSchema", True).delta("/FileStore/tables/DIM_Region")