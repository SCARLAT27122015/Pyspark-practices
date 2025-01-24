# Databricks notebook source
# MAGIC %md
# MAGIC # 04_Employees Earning More Than Their Managers

# COMMAND ----------

# MAGIC %md
# MAGIC Write a Pyspark program to find Employees Earning More Than Their Managers
# MAGIC
# MAGIC

# COMMAND ----------

from pyspark.sql.types import *
from pyspark.sql.functions import *

# COMMAND ----------

# Define the schema for the "employee" table
employees_schema = StructType([
    StructField('id', IntegerType(), True)
    , StructField('name', StringType(), True)
    , StructField('salary', IntegerType(), True )
    , StructField('managerid', IntegerType(), True)
])

# COMMAND ----------



# Define data for the "employees" table
employees_data = [
    (1, 'Joe', 70000, 3),
    (2, 'Henry', 80000, 4),
    (3, 'Sam', 60000, None),
    (4, 'Max', 90000, None)
]

# COMMAND ----------

df_employees = spark.createDataFrame(employees_data, employees_schema).alias('df_e')
df_managers = spark.createDataFrame(employees_data, employees_schema).alias('df_m')


# COMMAND ----------

df_managers.show()

# COMMAND ----------

(
    df_employees.join(
        df_managers
        , df_employees['managerid'] == df_managers['id']
        , how='left'
    )
    .filter(col('df_e.salary') > col('df_m.salary'))
    .select('df_e.name')
    .show()
 )

# COMMAND ----------


