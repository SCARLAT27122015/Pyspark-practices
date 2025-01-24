# Databricks notebook source
# MAGIC %md
# MAGIC # 23_Date in pyspark

# COMMAND ----------

# MAGIC %md
# MAGIC Write a pyspark code perform below function 
# MAGIC * Get all employee details from EmployeeDetail table whose joining month is Jan(1). 
# MAGIC * Get all employee details from EmployeeDetail table whose joining date between 2013-01-01" and "2013-12-01". 
# MAGIC * Get how many employee exist in "EmployeeDetail" table. 
# MAGIC * Select all employee detail with First name "Vikas","Ashish", and "Nikhil".

# COMMAND ----------

from pyspark.sql.functions import *
from pyspark.sql.window import *
from pyspark.sql.types import *

# COMMAND ----------

data = [ [1, "Vikas", "Ahlawat", 600000.0, "2013-02-15 11:16:28.290", "IT", "Male"], [2, "nikita", "Jain", 530000.0, "2014-01-09 17:31:07.793", "HR", "Female"], [3, "Ashish", "Kumar", 1000000.0, "2014-01-09 10:05:07.793", "IT", "Male"], [4, "Nikhil", "Sharma", 480000.0, "2014-01-09 09:00:07.793", "HR", "Male"], [5, "anish", "kadian", 500000.0, "2014-01-09 09:31:07.793", "Payroll", "Male"], ]

schema = StructType([ StructField("EmployeeID", IntegerType(), True), StructField("First_Name", StringType(), True), StructField("Last_Name", StringType(), True), StructField("Salary", DoubleType(), True), StructField("Joining_Date", StringType(), True), StructField("Department", StringType(), True), StructField("Gender", StringType(), True) ])

df_employee = spark.createDataFrame(data, schema)
df_employee.display()

# COMMAND ----------

# MAGIC %md
# MAGIC ### Get all employee details from EmployeeDetail table whose joining month is Jan(1).

# COMMAND ----------

df_employee.filter(month(col('Joining_Date'))==1).display()

# COMMAND ----------

# MAGIC %md
# MAGIC ### Get all employee details from EmployeeDetail table whose joining date between 2013-01-01" and "2013-12-01".

# COMMAND ----------

df_employee.filter( (to_timestamp(col('Joining_Date')) >= '2013-01-01') & (to_timestamp(col('Joining_Date')) <= '2013-12-01')).display()

# COMMAND ----------

df_employee.filter( col('Joining_Date').between('2013-01-01', '2013-12-01')).display()

# COMMAND ----------

# MAGIC %md
# MAGIC ### Get how many employee exist in "EmployeeDetail" table.

# COMMAND ----------

df_employee.select('EmployeeID').distinct().count()

# COMMAND ----------

# MAGIC %md
# MAGIC ### Select all employee detail with First name "Vikas","Ashish", and "Nikhil".

# COMMAND ----------

df_employee.filter(lower(col('First_Name')).isin(['vikas', 'ashish', 'nikhil']) ).display()

# COMMAND ----------


