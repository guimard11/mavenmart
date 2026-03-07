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
# META         }
# META       ]
# META     }
# META   }
# META }

# MARKDOWN ********************

# ### Extracting data sets from a GitHub repository to store them in Bronze Lakehouse.

# MARKDOWN ********************

# #### Importing required module

# CELL ********************

import pandas as pd
import requests

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ##### 1.1 Importing the products dataset

# CELL ********************

# Setting the path to upload the products dataset.
products_url = "https://raw.githubusercontent.com/guimard11/mavenmart/main/MavenMarket_Products.csv"

# Loading the dataset using the read_csv pandas function.
products_pandas = pd.read_csv(products_url)


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ##### 1.2 Save the products dataset into the bronze Lakehouse

# CELL ********************

# Convert Pandas DataFrame to Spark DataFrame
products_spark = spark.createDataFrame(products_pandas)

# Write as Delta table
products_spark.write.format("delta").mode("overwrite").saveAsTable("products")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ##### 2.1 Importing the customers dataset

# CELL ********************

# Setting the path to upload the products dataset.
customers_url = "https://raw.githubusercontent.com/guimard11/mavenmart/main/MavenMarket_Customers.csv"

# Loading the dataset using the read_csv pandas function.
customers_pandas = pd.read_csv(customers_url)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ##### 2.2 Save the customers dataset into the bronze Lakehouse

# CELL ********************

# Convert Pandas DataFrame to Spark DataFrame
customers_spark = spark.createDataFrame(customers_pandas)

# Write as Delta table
customers_spark.write.format("delta").mode("overwrite").saveAsTable("customers")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ##### 3.1 Importing the region dataset

# CELL ********************

# Setting the path to upload the products dataset.
region_url = "https://raw.githubusercontent.com/guimard11/mavenmart/main/MavenMarket_Regions.csv"

# Loading the dataset using the read_csv pandas function.
regions_pandas = pd.read_csv(region_url)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ##### 3.2 Save the region dataset into the bronze Lakehouse

# CELL ********************

# Convert Pandas DataFrame to Spark DataFrame
regions_spark = spark.createDataFrame(regions_pandas)

# Write as Delta table
regions_spark.write.format("delta").mode("overwrite").saveAsTable("regions")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ##### 4.1 Importing the calendar dataset

# CELL ********************

# Setting the path to upload the products dataset.
calendar_url = "https://raw.githubusercontent.com/guimard11/mavenmart/main/MavenMarket_Calendar.csv"

# Loading the dataset using the read_csv pandas function.
calendar_pandas = pd.read_csv(calendar_url)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ##### 4.2 Save the calendar dataset into the bronze Lakehouse

# CELL ********************

# Convert Pandas DataFrame to Spark DataFrame
calendar_spark = spark.createDataFrame(calendar_pandas)

# Write as Delta table
calendar_spark.write.format("delta").mode("overwrite").saveAsTable("calendar")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ##### 4.1 Importing the stores dataset

# CELL ********************

# Setting the path to upload the products dataset.
store_url = "https://raw.githubusercontent.com/guimard11/mavenmart/main/MavenMarket_Stores.csv"

# Loading the dataset using the read_csv pandas function.
store_pandas = pd.read_csv(store_url)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ##### 4.2 Save the Stores dataset into the bronze Lakehouse

# CELL ********************

# Convert Pandas DataFrame to Spark DataFrame
store_spark = spark.createDataFrame(store_pandas)

# Write as Delta table
store_spark.write.format("delta").mode("overwrite").saveAsTable("stores")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ##### 5.1 Importing the Transactions_1997 dataset

# CELL ********************

# Setting the path to upload the products dataset.
transac97_url = "https://raw.githubusercontent.com/guimard11/mavenmart/main/MavenMarket_Transactions_1997.csv"

# Loading the dataset using the read_csv pandas function.
transac97_pandas = pd.read_csv(transac97_url)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ##### 5.2 Save the Transactions_1997 dataset into the bronze Lakehouse

# CELL ********************

# Convert Pandas DataFrame to Spark DataFrame
transac97_spark = spark.createDataFrame(transac97_pandas)

# Write as Delta table
transac97_spark.write.format("delta").mode("overwrite").saveAsTable("transactions_1997")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ##### 6.1 Importing the Transactions_1998 dataset

# CELL ********************

# Setting the path to upload the products dataset.
transac98_url = "https://raw.githubusercontent.com/guimard11/mavenmart/main/MavenMarket_Transactions_1998.csv"

# Loading the dataset using the read_csv pandas function.
transac98_pandas = pd.read_csv(transac98_url)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ##### 6.2 Save the Transactions_1998 dataset into the bronze Lakehouse

# CELL ********************

# Convert Pandas DataFrame to Spark DataFrame
transac98_spark = spark.createDataFrame(transac98_pandas)

# Write as Delta table
transac98_spark.write.format("delta").mode("overwrite").saveAsTable("transactions_1998")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ##### 7.1 Importing the Returns_1997_1998 dataset

# CELL ********************

# Setting the path to upload the products dataset.
return_url = "https://raw.githubusercontent.com/guimard11/mavenmart/main/MavenMarket_Returns_1997-1998.csv"

# Loading the dataset using the read_csv pandas function.
return_pandas = pd.read_csv(return_url)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ##### 7.2 Save the Return_1997_1998 dataset into the bronze Lakehouse

# CELL ********************

# Convert Pandas DataFrame to Spark DataFrame
return_spark = spark.createDataFrame(return_pandas)

# Write as Delta table
return_spark.write.format("delta").mode("overwrite").saveAsTable("returns_1997_1998")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
