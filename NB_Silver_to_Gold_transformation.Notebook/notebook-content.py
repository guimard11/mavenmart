# Fabric notebook source

# METADATA ********************

# META {
# META   "kernel_info": {
# META     "name": "synapse_pyspark"
# META   },
# META   "dependencies": {
# META     "lakehouse": {
# META       "default_lakehouse": "074869bc-7b4e-4e67-80ff-e81d792f0488",
# META       "default_lakehouse_name": "LH_mavenmart_silver",
# META       "default_lakehouse_workspace_id": "2869e8b2-b7be-4489-a03e-7704f314fce5",
# META       "known_lakehouses": [
# META         {
# META           "id": "074869bc-7b4e-4e67-80ff-e81d792f0488"
# META         },
# META         {
# META           "id": "23b11646-756d-4e0a-9edb-4d6fe09eb6bc"
# META         }
# META       ]
# META     }
# META   }
# META }

# MARKDOWN ********************

# #### Processing the datasets to build a semantic model. 

# MARKDOWN ********************

# ###### 1. Read the products_new dataset

# CELL ********************

# Path to access the products dataset
silver_products_new_path = "abfss://mavenmart_pipe@onelake.dfs.fabric.microsoft.com/LH_mavenmart_silver.Lakehouse/Tables/dbo/products_new"

# Read the dataset into a dataframe
products_new = spark.read.format("delta").load(silver_products_new_path)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ###### 2. Read the calendar_new dataset

# CELL ********************

# Path to access the products dataset
silver_calendar_new_path = "abfss://mavenmart_pipe@onelake.dfs.fabric.microsoft.com/LH_mavenmart_silver.Lakehouse/Tables/dbo/calendar_new"

# Read the dataset into a dataframe
calendar_new = spark.read.format("delta").load(silver_calendar_new_path)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ###### 3. Read the regions dataset

# CELL ********************

# Path to access the products dataset
silver_regions_path = "abfss://mavenmart_pipe@onelake.dfs.fabric.microsoft.com/LH_mavenmart_silver.Lakehouse/Tables/dbo/regions"

# Read the dataset into a dataframe
regions = spark.read.format("delta").load(silver_regions_path)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ###### 4. Read the returns_97_98 dataset

# CELL ********************

# Path to access the products dataset
silver_returns_97_98_path = "abfss://mavenmart_pipe@onelake.dfs.fabric.microsoft.com/LH_mavenmart_silver.Lakehouse/Tables/dbo/returns_97_98"

# Read the dataset into a dataframe
returns_97_98 = spark.read.format("delta").load(silver_returns_97_98_path)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ###### 5. Read the stores dataset

# CELL ********************

# Path to access the products dataset
silver_stores_new_path = "abfss://mavenmart_pipe@onelake.dfs.fabric.microsoft.com/LH_mavenmart_silver.Lakehouse/Tables/dbo/stores_new"

# Read the dataset into a dataframe
stores_new = spark.read.format("delta").load(silver_stores_new_path)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ###### 6. Read the transactions dataset

# CELL ********************

# Path to access the products dataset
silver_transactions_path = "abfss://mavenmart_pipe@onelake.dfs.fabric.microsoft.com/LH_mavenmart_silver.Lakehouse/Tables/dbo/transactions"

# Read the dataset into a dataframe
transactions = spark.read.format("delta").load(silver_transactions_path)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ###### 7. Read the customers_new dataset

# CELL ********************

# Path to access the products dataset
silver_customers_new_path = "abfss://mavenmart_pipe@onelake.dfs.fabric.microsoft.com/LH_mavenmart_silver.Lakehouse/Tables/dbo/customers_new"

# Read the dataset into a dataframe
customers_new = spark.read.format("delta").load(silver_customers_new_path)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
