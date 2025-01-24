# Databricks notebook source
# MAGIC %md
# MAGIC # Combine Two DF

# COMMAND ----------

# MAGIC %md
# MAGIC Write a Pyspark program to report the first name, last name, city, and state of each person in the Person dataframe. If the address of a personld 1  is not present in the Address dataframe, report null instead.   
# MAGIC

# COMMAND ----------

from pyspark.sql.types import *
from pyspark.sql.functions import *

# COMMAND ----------

# Define schema for the 'persons' table
persons_schema = StructType([
    StructField('personId', IntegerType(), True)
    , StructField('lastName', StringType(), True)
    , StructField('firstName', StringType(), True)
])

# Define schema for the 'addresses' table
addresses_schema = StructType([
    StructField("addressid", IntegerType(), True),
    StructField("personId", IntegerType(), True),
    StructField("city", StringType(), True),
    StructField("state", StringType(), True)
])

# Define data for the 'persons' table
persons_data = [
    (1, 'Wang', 'Allen'),
    (2, 'Alice', 'Bob')
]

# Define data for the 'addresses' table
addresses_data = [
    (1, 2, 'New York City', 'New York'),
    (2, 3, 'Leetcode', 'California')
]

# COMMAND ----------

df_persons = spark.createDataFrame(persons_data, persons_schema)
df_addresses = spark.createDataFrame(addresses_data, addresses_schema)

# COMMAND ----------

df_persons.show()
df_addresses.show()

# COMMAND ----------

(df_persons
    .join(df_addresses, on='personId', how='left')
    .select('firstName', 'lastName', 'city', 'state')
    .show()
)

# COMMAND ----------


