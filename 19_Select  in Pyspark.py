# Databricks notebook source
# MAGIC %md
# MAGIC # 19_Select in pyspark

# COMMAND ----------

# MAGIC %md
# MAGIC Write a pyspark code perform below function 
# MAGIC * Get all employee detail from emp_df whose "Gender" end with 'le' and contain 4 letters. The Underscore(_) Wildcard Character represents any single character. 
# MAGIC * Get all employee detail from EmployeeDetail table whose "FirstName" start with # 'A' and contain 5 letters. 
# MAGIC * Get all unique "Department" from EmployeeDetail table. 
# MAGIC * Get the highest "Salary" from EmployeeDetail table.

# COMMAND ----------

from pyspark.sql.functions import *
from pyspark.sql.types import *
from pyspark.sql.window import *

# COMMAND ----------

data = [ [1, "Vikas", "Ahlawat", 600000.0, "2013-02-15 11:16:28.290", "IT", "Male"], [2, "nikita", "Jain", 530000.0, "2014-01-09 17:31:07.793", "HR", "Female"], [3, "Ashish", "Kumar", 1000000.0, "2014-01-09 10:05:07.793", "IT", "Male"], [4, "Nikhil", "Sharma", 480000.0, "2014-01-09 09:00:07.793", "HR", "Male"], [5, "anish", "kadian", 500000.0, "2014-01-09 09:31:07.793", "Payroll", "Male"], ]

schema = StructType([ StructField("EmployeeID", IntegerType(), True), StructField("First_Name", StringType(), True), StructField("Last_Name", StringType(), True), StructField("Salary", DoubleType(), True), StructField("Joining_Date", StringType(), True), StructField("Department", StringType(), True), StructField("Gender", StringType(), True) ])

# COMMAND ----------

df_employee = spark.createDataFrame(data, schema)

# COMMAND ----------

df_employee.display()

# COMMAND ----------

# MAGIC %md
# MAGIC ### Get all employee detail from emp_df whose "Gender" end with 'le' and contain 4 letters. The Underscore(_) Wildcard Character represents any single character.

# COMMAND ----------

(
    df_employee.filter(
                    (col('Gender').like('%le'))
                    & (length(col('Gender')) ==4 )   
                                     
                ).display()
 )

# COMMAND ----------

# MAGIC %md
# MAGIC ### Get all employee detail from EmployeeDetail table whose "FirstName" start with # 'A' and contain 5 letters.

# COMMAND ----------

(
  df_employee.filter(
                (lower(col('First_Name')).like('a%'))
                & (length(col('First_Name')) == 5)
              ).display()
)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Get all unique "Department" from EmployeeDetail table.

# COMMAND ----------

(
  df_employee.select('Department')
             .distinct()
             .display()
)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Get the highest "Salary" from EmployeeDetail table.

# COMMAND ----------

(
  df_employee.agg(max('Salary'))
             .display()
)
