# Databricks notebook source
# MAGIC %md
# MAGIC ###Joining all Dim with Fact Table 

# COMMAND ----------

from pyspark.sql import SparkSession
from pyspark.sql.functions import (when, to_date)

# COMMAND ----------

spark= SparkSession.builder.getOrCreate()

# COMMAND ----------

# MAGIC %run ./DimRegion

# COMMAND ----------

# MAGIC %md
# MAGIC ####Joining Region Dim with current fact table

# COMMAND ----------

df_salesJoin= df_sales.join(df_dimRegion, df_sales.Region == df_dimRegion.Region, how="inner")

# COMMAND ----------

display(df_salesJoin)

# COMMAND ----------

# MAGIC %md 
# MAGIC ####Joining Product Category

# COMMAND ----------

# MAGIC %md 

# COMMAND ----------

# MAGIC %run ./DimProductCategory

# COMMAND ----------

df_salesJoin= df_salesJoin.join(df_dim_ProductCategory, df_salesJoin.ProductCategory == df_dim_ProductCategory.ProductCategory , how="inner")


# COMMAND ----------

display(df_salesJoin)

# COMMAND ----------

# MAGIC %md 
# MAGIC #### Joining ProductSubCategory

# COMMAND ----------

# MAGIC %run ./DimProductSubCategory

# COMMAND ----------

df_salesJoin= df_salesJoin.join(df_dimProductSubCategory, df_salesJoin.ProductSubCategory==df_dimProductSubCategory.ProductSubCategory , how="inner")

# COMMAND ----------

display(df_salesJoin)

# COMMAND ----------

# MAGIC %md 
# MAGIC #### Joining SalesChannel 

# COMMAND ----------

# MAGIC %run ./DimSalesChannel

# COMMAND ----------

df_salesJoin= df_salesJoin.join(df_dimSalesChannel, df_salesJoin.SalesChannel == df_dimSalesChannel.SalesChannel, how="inner")

# COMMAND ----------

# MAGIC %md 
# MAGIC #### Joining CustomerSegment

# COMMAND ----------

# MAGIC %run ./DimCustomerSegment

# COMMAND ----------

df_salesJoin = df_salesJoin.join(df_dimCustomerSegment, df_salesJoin.CustomerSegment == df_dimCustomerSegment.CustomerSegment , how="inner")

# COMMAND ----------

# MAGIC %md 
# MAGIC #### Joining SalesRep 

# COMMAND ----------

# MAGIC %run ./DimSalesRep

# COMMAND ----------

df_salesJoin = df_salesJoin.join(df_dimSalesRep, df_salesJoin.SalesRep == df_dimSalesRep.SalesRep, how="inner")

# COMMAND ----------

# MAGIC %md 
# MAGIC #### Joining StoreType

# COMMAND ----------

# MAGIC %run ./DimStoreType

# COMMAND ----------

df_salesJoin= df_salesJoin.join(df_dimStoreType, df_salesJoin.StoreType == df_dimStoreType.StoreType , how= "inner")

# COMMAND ----------

# MAGIC %md 
# MAGIC #### Joining Sales Date 

# COMMAND ----------

# MAGIC %run ./DimSalesDate

# COMMAND ----------

df1 = df_salesJoin.withColumn("SalesDate1", to_date(col("SalesDate")))

# COMMAND ----------

df_joinDate = df_salesJoin.join(df_dimSalesDate, df_salesJoin.SalesDate == df_dimSalesDate.SalesDate , how = "inner")

# COMMAND ----------

display(df1)

# COMMAND ----------

# MAGIC %md
# MAGIC #### Except Sales Date Join

# COMMAND ----------

df_factSales= df_salesJoin.select([
    col("RegionId"), 
    col("ProductCategoryId"), 
    col("ProductSubCategoryId"), 
    col("SalesChannelId"), 
    col("CustomerSegmentId"), 
    col("SalesRepID"), 
    col("StoreTypeId"), 
    col("UnitsSold"), 
    col("Revenue")
])

# COMMAND ----------

display(df_factSales)

# COMMAND ----------


df_factSales.write.format("delta").mode("overwrite").save("/FileStore/tables/Fact_Sales_Final")