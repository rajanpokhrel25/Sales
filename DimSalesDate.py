# Databricks notebook source
from pyspark.sql import SparkSession
from pyspark.sql.functions import (date_format,month, year,col, upper, cast,when, hash as hashkey)

# COMMAND ----------

spark= SparkSession.builder.getOrCreate()

# COMMAND ----------

df_sales= spark.read.option("header", True).option("inferSchema", True).csv("/FileStore/tables/fact_sales.csv")

# COMMAND ----------

df_SalesDate = df_sales.select([
    col("SalesDate")
]).distinct()

# COMMAND ----------

df_SalesDateId = df_SalesDate.withColumn("SalesDateId", hashkey(upper(col("SalesDate"))).cast("bigint"))

# COMMAND ----------

df_SalesDateNA = spark.createDataFrame([
    ("N/A", -1)
], ["SalesDate", "SalesDateId"])

# COMMAND ----------

df_dimSalesDate= df_SalesDateNA.union(df_SalesDateId)

# COMMAND ----------



# COMMAND ----------

df_dimSalesDate = df_dimSalesDate.withColumn("Month", date_format("SalesDate", "MMMM"))

# COMMAND ----------

df_dimSalesDate= df_dimSalesDate.withColumn("Year",year(df_dimSalesDate["SalesDate"]))

# COMMAND ----------

df_dimSalesDate= df_dimSalesDate.withColumn("Semester",\
                                            when(month(df_dimSalesDate["SalesDate"]).between(1,6), "Semester 1" ) \
                                                .when(month(df_dimSalesDate["SalesDate"]).between(7,12), "Semester 2")\
                                                .otherwise("Invalid Date"))

# COMMAND ----------

df_dimSalesDate= df_dimSalesDate.withColumn("Quater", when(month(df_dimSalesDate["SalesDate"]).between(1,4), "Q1") \
                                            .when(month(df_dimSalesDate["SalesDate"]).between(5,8), "Q2") \
                                               .when(month(df_dimSalesDate["SalesDate"]).between(9,12), "Q3")\
                                                   .otherwise("Invalid Date"))

# COMMAND ----------

display(df_dimSalesDate)

# COMMAND ----------

df_dimSalesDate.write.format("delta").mode("overwrite").save("/FileStore/tables/DIM_SalesDate")