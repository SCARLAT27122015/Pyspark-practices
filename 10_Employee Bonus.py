# Databricks notebook source
# MAGIC %md
# MAGIC # 10_Employee Bonus

# COMMAND ----------

# MAGIC %md
# MAGIC Write a solution to report the name and bonus amount of each employee with a bonus less than 1000. Return the result table in any order

# COMMAND ----------

from pyspark.sql.functions import *
from pyspark.sql.types import *
from pyspark.sql.window import *

# COMMAND ----------

# Define the schema for the "Employee" 
employee_schema = StructType([ StructField("empId", IntegerType(), True), StructField("name", StringType(), True), StructField("supervisor", IntegerType(), True), StructField("salary", IntegerType(), True) ])
# Define data for the "Employee" 
employee_data = [ (3, 'Brad', None, 4000), (1, 'John', 3, 1000), (2, 'Dan', 3, 2000), (4, 'Thomas', 3, 4000) ]
# Define the schema for the "Bonus" 
bonus_schema = StructType([ StructField("empId", IntegerType(), True), StructField("bonus", IntegerType(), True) ])

# COMMAND ----------

# Define data for the "Bonus" 
bonus_data = [ (2, 500), (4, 2000) ]

# COMMAND ----------

df_employee = spark.createDataFrame(employee_data, employee_schema)
df_bonus = spark.createDataFrame(bonus_data, bonus_schema)

# COMMAND ----------

df_employee.show()

# COMMAND ----------

df_bonus.show()

# COMMAND ----------

(
    df_employee.join(
                        df_bonus
                        , df_employee['empId'] == df_bonus['empId']
                        , how='left'
                    )
                .filter(col('bonus') < 1000)
                .select('name', 'bonus')
               .show()
)

# COMMAND ----------


