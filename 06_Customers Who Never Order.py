# Databricks notebook source
# MAGIC %md
# MAGIC # 06_Customers Who Never Order
# MAGIC
# MAGIC

# COMMAND ----------

# MAGIC %md
# MAGIC Write a Pyspark program to find all customers who never order anything.
# MAGIC
# MAGIC

# COMMAND ----------

from pyspark.sql.functions import *
from pyspark.sql.types import *

# COMMAND ----------

# Define the schema for the "Customers"
customers_schema = StructType([
    StructField("id", IntegerType(), True),
    StructField("name", StringType(), True)
])

# Define data for the "Customers"
customers_data = [
    (1, 'Joe'),
    (2, 'Henry'),
    (3, 'Sam'),
    (4, 'Max')
]

# Define the schema for the "Orders"
orders_schema = StructType([
    StructField("id", IntegerType(), True),
    StructField("customerid", IntegerType(), True)
])

# Define data for the "Orders"
orders_data = [
    (1, 3),
    (2, 1)
]

# COMMAND ----------

df_customers = spark.createDataFrame(customers_data, customers_schema)
df_orders = spark.createDataFrame(orders_data, orders_schema)




# COMMAND ----------

df_customers.show(5)
df_orders.show(5)


# COMMAND ----------

(
    df_customers.join(
                    df_orders
                    , df_customers['id'] == df_orders['customerid']
                    , how='left'
                )
                .filter(isnull(col('customerid')))
                .select('name')
                .show()
 )

# COMMAND ----------


