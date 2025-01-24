# Databricks notebook source
# MAGIC %md
# MAGIC # 18_Select in pyspark

# COMMAND ----------

# MAGIC %md
# MAGIC Write a pyspark code perform below function 
# MAGIC * Get all employee details from EmployeeDetail table whose "FirstName" contains 'k' 
# MAGIC * Get all employee details from EmployeeDetail table whose "FirstName" end with 'h' 
# MAGIC * Get all employee detail from EmployeeDetail table whose "FirstName" start with any single character between 'a-p'

# COMMAND ----------

from pyspark.sql.functions import *
from pyspark.sql.types import *
from pyspark.sql.window import *

# COMMAND ----------

schema = StructType([ StructField("EmployeeID", IntegerType(), True), StructField("First_Name", StringType(), True), StructField("Last_Name", StringType(), True),StructField("Salary", DoubleType(), True), StructField("Joining_Date", StringType(), True), StructField("Department", StringType(), True), StructField("Gender", StringType(), True) ])

data = [ [1, "Vikas", "Ahlawat", 600000.0, "2013-02-15 11:16:28.290", "IT", "Male"], [2, "nikita", "Jain", 530000.0, "2014-01-09 17:31:07.793", "HR", "Female"], [3, "Ashish", "Kumar", 1000000.0, "2014-01-09 10:05:07.793", "IT", "Male"], [4, "Nikhil", "Sharma", 480000.0, "2014-01-09 09:00:07.793", "HR", "Male"], [5, "anish", "kadian", 500000.0, "2014-01-09 09:31:07.793", "Payroll", "Male"], ]

# COMMAND ----------

df_employees = spark.createDataFrame(data, schema)

# COMMAND ----------

df_employees.display()

# COMMAND ----------

# MAGIC %md
# MAGIC ###  All employee details from EmployeeDetail table whose "FirstName" contains 'k'

# COMMAND ----------

df_employees.filter(lower('First_Name').like('%k%')).display()

# COMMAND ----------

# MAGIC %md
# MAGIC ### Get all employee details from EmployeeDetail table whose "FirstName" end with 'h'

# COMMAND ----------

df_employees.filter(lower('First_Name').endswith('h')).display()

# COMMAND ----------

# MAGIC %md
# MAGIC ### Get all employee detail from EmployeeDetail table whose "FirstName" start with any single character between 'a-p'

# COMMAND ----------

df_employees.filter(lower('First_Name').rlike('^[a-p]')).display()

# COMMAND ----------


