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
from pyspark.sql.functions import when, col, isnan, isnull,dayofmonth, month, quarter, year, round, to_date

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

# ##### 1.3 Save the customers_new dataset into silver lakehouse.

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
