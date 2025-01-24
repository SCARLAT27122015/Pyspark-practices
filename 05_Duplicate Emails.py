# Databricks notebook source
# MAGIC %md
# MAGIC # Duplicate Emails
# MAGIC
# MAGIC

# COMMAND ----------

# MAGIC %md
# MAGIC Write a Pyspark program to report all the duplicate emails.
# MAGIC Note that it's guaranteed that the email field is not NULL.

# COMMAND ----------

from pyspark.sql.types import *
from pyspark.sql.functions import *

# COMMAND ----------

# Define the schema for the "employees" table
employees_schema = StructType([
    StructField('id', IntegerType(), True )
    , StructField('name', StringType(), True ),
    StructField("email", StringType(), True),
    StructField("salary", IntegerType(), True),
    StructField("managerld", IntegerType(), True)
])

# Define data for the "employees" table
employees_data = [
    (1, 'Joe', 'joe@email.com', 70000, 3),
    (2, 'Henry', 'henry@email.com', 80000, 4),
    (3, 'Sam', 'sam@email.com', 60000, None),
    (4, 'Max', 'joe@email.com', 90000, None)
]

# COMMAND ----------

df_employees = spark.createDataFrame(employees_data, employees_schema)

# COMMAND ----------

df_employees.show()

# COMMAND ----------

df_employees.exceptAll(df_employees.dropDuplicates(['email'])).show()



# COMMAND ----------

(df_employees.groupBy('email')
              .agg(count('*')
              .alias('email_count'))
              .filter(col('email_count') > 1)
              .show()
)

# COMMAND ----------


