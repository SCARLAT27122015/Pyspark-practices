# Databricks notebook source
# MAGIC %md
# MAGIC # 11_Find Customer Referee

# COMMAND ----------

# MAGIC %md
# MAGIC Find the names of the customer that are not referred by the customer with id = 2. Return the result table in any order

# COMMAND ----------

from pyspark.sql.functions import *
from pyspark.sql.types import *
from pyspark.sql.window import *

# COMMAND ----------

# Define the schema for the Customer table 
schema = StructType([ StructField("id", IntegerType(), True), StructField("name", StringType(), True), StructField("referee_id", IntegerType(), True) ])
# Create an RDD with the data 
data = [ (1, 'Will', None), (2, 'Jane', None), (3, 'Alex', 2), (4, 'Bill', None), (5, 'Zack', 1), (6, 'Mark', 2) ]

# COMMAND ----------

df_customers = spark.createDataFrame(data, schema)
df_customers.show(5)

# COMMAND ----------

(
    df_customers.filter((~col('referee_id').isin(2)) | isnull(col('referee_id')) )
                .select('name')
                .show()
)

# COMMAND ----------


