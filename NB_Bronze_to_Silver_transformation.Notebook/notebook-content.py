# Fabric notebook source

# METADATA ********************

# META {
# META   "kernel_info": {
# META     "name": "synapse_pyspark"
# META   },
# META   "dependencies": {
# META     "lakehouse": {
# META       "default_lakehouse": "5010b322-daf9-4aa4-9bb2-5c28f6c1e634",
# META       "default_lakehouse_name": "LH_mavenmart_bronze",
# META       "default_lakehouse_workspace_id": "2869e8b2-b7be-4489-a03e-7704f314fce5",
# META       "known_lakehouses": [
# META         {
# META           "id": "5010b322-daf9-4aa4-9bb2-5c28f6c1e634"
# META         },
# META         {
# META           "id": "074869bc-7b4e-4e67-80ff-e81d792f0488"
# META         }
# META       ]
# META     }
# META   }
# META }

# MARKDOWN ********************

# ### Processing raw data to store in Silver Lakehouse for further processing

# MARKDOWN ********************

# ##### Importing required modules

# CELL ********************

# Load required module 
import requests
import pandas as pd
from datetime import timedelta
from pyspark.sql.functions import when, col, isnan, isnull,dayofmonth, month, quarter, year, round, to_date, substring, trunc, date_format

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ##### 1.1 Loading the products datasets into delta format.

# CELL ********************

# Path to access the products dataset
bronze_products_path = "abfss://mavenmart_pipe@onelake.dfs.fabric.microsoft.com/LH_mavenmart_bronze.Lakehouse/Tables/dbo/products"

# Read the dataset into a dataframe
products = spark.read.format("delta").load(bronze_products_path)


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ##### 1.2 Processing the dataset to change data type and generate new columns.

# CELL ********************

# transform data type using string function, create and round new discount_price columns to 2 digits, and replace null value using isNull and when condition.
products_new = products.withColumn("product_sku", col("product_sku").cast("string"))\
                       .withColumn("discount_price", round(col("product_retail_price") * 0.9, 2))\
                       .withColumn("recyclable", when(col("recyclable").isNull(), 0)\
                       .otherwise(col("recyclable")))\
                       .withColumn("low_fat", when(col("low_fat").isNull(), 0)\
                       .otherwise(col("low_fat")))
                       

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ##### 1.3 Save the products_new dataset into gold lakehouse.

# CELL ********************

# Path to gold lakehouse
silver_products_new_path = "abfss://mavenmart_pipe@onelake.dfs.fabric.microsoft.com/LH_mavenmart_silver.Lakehouse/Tables/dbo/products_new"

# Write the dataset into the gold lakehouse 
products_new.write.format("delta").mode("overwrite").save(silver_products_new_path)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ##### 2.1 Loading the customers datasets into delta format.

# CELL ********************

# Path to access the products dataset
bronze_customers_path = "abfss://mavenmart_pipe@onelake.dfs.fabric.microsoft.com/LH_mavenmart_bronze.Lakehouse/Tables/dbo/customers"

# Read the dataset into a dataframe
customers = spark.read.format("delta").load(bronze_customers_path)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ##### 2.2 Processing the dataset to change data type and generate new columns.

# CELL ********************

# Transform the customers dataset to change data type, create new columns
customers_new = customers.withColumn('customer_acct_num', col('customer_acct_num').cast('string'))\
                         .withColumn('customer_postal_code', col('customer_postal_code').cast('string'))\
                         .withColumn('birthdate', to_date(col('birthdate'), "M/d/yyyy"))\
                         .withColumn('birthyear', year('birthdate').cast('string'))\
                         .withColumn('has_children', when(col('total_children') == 0, "no")\
                         .otherwise('yes'))

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ##### 2.3 Save the customers_new dataset into silver lakehouse.

# CELL ********************

# Path to silver lakehouse
silver_customers_new_path = "abfss://mavenmart_pipe@onelake.dfs.fabric.microsoft.com/LH_mavenmart_silver.Lakehouse/Tables/dbo/customers_new"

# Write the dataset into the gold lakehouse 
customers_new.write.format("delta").mode("overwrite").save(silver_customers_new_path)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ##### 3.1 Loading the stores datasets into delta format.

# CELL ********************

# Path to access the products dataset
bronze_stores_path = "abfss://mavenmart_pipe@onelake.dfs.fabric.microsoft.com/LH_mavenmart_bronze.Lakehouse/Tables/dbo/stores"

# Read the dataset into a dataframe
stores = spark.read.format("delta").load(bronze_stores_path)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ##### 3.2 Processing the dataset to create new columns.

# CELL ********************

stores_new = stores.withColumn("area_phone", substring(col("store_phone"), 1, 3))

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ##### 3.3 Save the customers_new dataset into silver lakehouse.

# CELL ********************

# Path to silver lakehouse
silver_stores_new_path = "abfss://mavenmart_pipe@onelake.dfs.fabric.microsoft.com/LH_mavenmart_silver.Lakehouse/Tables/dbo/stores_new"

# Write the dataset into the gold lakehouse 
stores_new.write.format("delta").mode("overwrite").save(silver_stores_new_path)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ##### 4.1 Loading the calendar datasets into delta format.

# CELL ********************

# Path to access the products dataset
bronze_calendar_path = "abfss://mavenmart_pipe@onelake.dfs.fabric.microsoft.com/LH_mavenmart_bronze.Lakehouse/Tables/dbo/calendar"

# Read the dataset into a dataframe
calendar = spark.read.format("delta").load(bronze_calendar_path)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ##### 4.2 Processing the dataset to create new columns.

# CELL ********************

calendar_new = calendar.withColumn("date", to_date(col("date"), "M/d/yyyy"))\
                       .withColumn("year", year(col("date")))\
                       .withColumn("quarter_year", quarter(col("date")))\
                       .withColumn("start_month", trunc(col("date"), "month"))\
                       .withColumn("monday_start_week", trunc(col("date"), "week"))\
                       .withColumn("day_name", date_format(col("date"), "EEEE"))\
                       .withColumn("month_name", date_format(col("date"), "MMMM"))                 

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ##### 4.3 Save the calendar_new dataset into silver lakehouse.

# CELL ********************

# Path to silver lakehouse
silver_calendar_new_path = "abfss://mavenmart_pipe@onelake.dfs.fabric.microsoft.com/LH_mavenmart_silver.Lakehouse/Tables/dbo/calendar_new"

# Write the dataset into the gold lakehouse 
calendar_new.write.format("delta").mode("overwrite").save(silver_calendar_new_path)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ##### 5. Load and save the datasets regions and returns_1997_1998 into silver Lakehouse

# MARKDOWN ********************

# 5.1. Reading the return dataset.

# CELL ********************

# Path to access the returns dataset
bronze_returns_path = "abfss://mavenmart_pipe@onelake.dfs.fabric.microsoft.com/LH_mavenmart_bronze.Lakehouse/Tables/dbo/returns_1997_1998"

# Read the dataset into a dataframe
returns = spark.read.format("delta").load(bronze_returns_path)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ###### 5.2 Create month and year from date_return into returns dataset

# CELL ********************

returns_97_98 = returns.withColumn("return_date", to_date(col("return_date"), "M/d/yyyy"))\
                       .withColumn("month_return", month(col("return_date")))\
                       .withColumn("year_return", year(col("return_date")))

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ###### 5.3 Reading the regions dataset

# CELL ********************

# Path to access the products dataset
bronze_regions_path = "abfss://mavenmart_pipe@onelake.dfs.fabric.microsoft.com/LH_mavenmart_bronze.Lakehouse/Tables/dbo/regions"

# Read the dataset into a dataframe
regions = spark.read.format("delta").load(bronze_regions_path)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ###### 5.4 Save the returns and regions datasets into Silver Lakehouse.

# CELL ********************

# Path to silver lakehouse
silver_regions_path = "abfss://mavenmart_pipe@onelake.dfs.fabric.microsoft.com/LH_mavenmart_silver.Lakehouse/Tables/dbo/regions"
silver_returns_97_98_path = "abfss://mavenmart_pipe@onelake.dfs.fabric.microsoft.com/LH_mavenmart_silver.Lakehouse/Tables/dbo/returns_97_98"

# Write the dataset into the gold lakehouse 
regions.write.format("delta").mode("overwrite").save(silver_regions_path)
returns_97_98.write.format("delta").mode("overwrite").save(silver_returns_97_98_path)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ##### 6.1 Loading the transactions datasets into delta format.

# MARKDOWN ********************

# ###### 6.1.1 Loading the transactions from 1997

# CELL ********************

# Path to access the returns dataset
bronze_transactions_1997_path = "abfss://mavenmart_pipe@onelake.dfs.fabric.microsoft.com/LH_mavenmart_bronze.Lakehouse/Tables/dbo/transactions_1997"

# Read the dataset into a dataframe
transac_1997 = spark.read.format("delta").load(bronze_transactions_1997_path)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ###### 6.1.2 Loading the transactions from 1997

# CELL ********************

# Path to access the returns dataset
bronze_transactions_1998_path = "abfss://mavenmart_pipe@onelake.dfs.fabric.microsoft.com/LH_mavenmart_bronze.Lakehouse/Tables/dbo/transactions_1998"

# Read the dataset into a dataframe
transac_1998 = spark.read.format("delta").load(bronze_transactions_1998_path)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ###### 6.2 Merge the two datasets transactions_1997 and transactions_1998.

# CELL ********************

# Append the transactions_1997 datasets to the transactions_1998 using the union() function.
transactions = transac_1997.union(transac_1998)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ##### 6.3 Save the transactions dataset into the silver lakehouse.

# CELL ********************

# Path to silver lakehouse
silver_transactions_path = "abfss://mavenmart_pipe@onelake.dfs.fabric.microsoft.com/LH_mavenmart_silver.Lakehouse/Tables/dbo/transactions"

# Write the dataset into the gold lakehouse 
transactions.write.format("delta").mode("overwrite").save(silver_transactions_path)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
