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

# ##### Importing required module

# CELL ********************

from pyspark.sql.functions import col

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

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

# CELL ********************

# Check all data type comply with expected data format
print(products_new.dtypes)

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

# CELL ********************

# Check all data type comply with expected data format
print(calendar_new.dtypes)

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

# CELL ********************

# Check all data type comply with expected data format
print(regions.dtypes)

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

# CELL ********************

# Check if all data type is compliant with expected data format
print(returns_97_98.dtypes)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ###### 5. Read and process the stores dataset

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

# CELL ********************

# Check all data type comply with expected data format
print(stores_new.dtypes)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# Transform the area_phone data type into number
stores_new = stores_new.withColumn("area_phone", col("area_phone").cast("int"))

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

# CELL ********************

# Check all data type comply with expected data format
print(transactions.dtypes)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# Transform colums related to date from string to date format
transactions = transactions.withColumn('transaction_date', col('transaction_date').cast("date"))\
                           .withColumn('stock_date', col('stock_date').cast("date"))

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

# CELL ********************

# Check all data type comply with expected data format
print(customers_new.dtypes)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# Transform the acc_open_date from string to date
customers_new = customers_new.withColumn('acct_open_date', col('acct_open_date').cast("date"))

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# Handle dataset privacy information by removing all of them
customers = customers_new.select('customer_id', 'customer_acct_num', 'customer_city', 'customer_state_province', 'customer_country', 'birthdate', 'marital_status', 'yearly_income', 'gender', 'total_children', 'num_children_at_home', 'education', 'acct_open_date', 'member_card', 'occupation', 'homeowner', 'birthyear', 'has_children')

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ##### 8. Save all the enriched dataset into the gold Lakehouse

# MARKDOWN ********************

# ##### 8.1 Set all the path.

# CELL ********************

# Path to calendar_new into gold Lakehouse
gold_calendar_new_path = "abfss://mavenmart_pipe@onelake.dfs.fabric.microsoft.com/LH_mavenmart_gold.Lakehouse/Tables/dbo/calendar_new"

# Path to customers into gold Lakehouse
gold_customers_path = "abfss://mavenmart_pipe@onelake.dfs.fabric.microsoft.com/LH_mavenmart_gold.Lakehouse/Tables/dbo/customers"

# Path to products_new into gold Lakehouse
gold_products_new_path = "abfss://mavenmart_pipe@onelake.dfs.fabric.microsoft.com/LH_mavenmart_gold.Lakehouse/Tables/dbo/products_new"

# Path to regions into gold Lakehouse
gold_regions_path = "abfss://mavenmart_pipe@onelake.dfs.fabric.microsoft.com/LH_mavenmart_gold.Lakehouse/Tables/dbo/regions"

# Path to returns_97_98 into gold Lakehouse
gold_returns_97_98_path = "abfss://mavenmart_pipe@onelake.dfs.fabric.microsoft.com/LH_mavenmart_gold.Lakehouse/Tables/dbo/returns_97_98"

# Path to stores_new into gold Lakehouse
gold_stores_new_path = "abfss://mavenmart_pipe@onelake.dfs.fabric.microsoft.com/LH_mavenmart_gold.Lakehouse/Tables/dbo/stores_new"

# Path to calendar_new into gold Lakehouse
gold_transactions_path = "abfss://mavenmart_pipe@onelake.dfs.fabric.microsoft.com/LH_mavenmart_gold.Lakehouse/Tables/dbo/transactions"

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ##### 8.2 Write each of the dataset into gold Lakehouse.

# CELL ********************

# Write the calendar_new dataset into gold Lakehouse in delta format.
calendar_new.write.format("delta").mode("overwrite").save(gold_calendar_new_path)

# Write the customers dataset into gold Lakehouse in delta format.
customers.write.format("delta").mode("overwrite").save(gold_customers_path)

# Write the products_new dataset into gold Lakehouse in delta format.
products_new.write.format("delta").mode("overwrite").save(gold_products_new_path )

# Write the regions dataset into gold Lakehouse in delta format.
regions.write.format("delta").mode("overwrite").save(gold_regions_path)

# Write the returns_97_98 dataset into gold Lakehouse in delta format.
returns_97_98.write.format("delta").mode("overwrite").save(gold_returns_97_98_path)

# Write the stores_new dataset into gold Lakehouse in delta format.
stores_new.write.format("delta").mode("overwrite").save(gold_stores_new_path)

# Write the transactions dataset into gold Lakehouse in delta format.
transactions.write.format("delta").mode("overwrite").save(gold_transactions_path)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
