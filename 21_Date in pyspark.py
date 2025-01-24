# Databricks notebook source
# MAGIC %md
# MAGIC # 21_Date in pyspark

# COMMAND ----------

# MAGIC %md
# MAGIC Write a pyspark code perform below function 
# MAGIC * Get only Year part of "JoiningDate" 
# MAGIC * Get only Month part of "JoiningDate". 
# MAGIC * Get only date part of "JoiningDate". 
# MAGIC * Get the current system date using DataFrame API 
# MAGIC * Get the current UTC date and time using DataFrame API

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
# MAGIC ### Get only Year part of "JoiningDate"

# COMMAND ----------

(
    df_employee.withColumn(
        'year'
        , date_format(
            to_timestamp(col('Joining_Date'))
            , 'yyyy'           
        )
    )
    .display()
)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Get only Month part of "JoiningDate".

# COMMAND ----------

(
    df_employee.withColumn(
        'month'
        , date_format(
            to_timestamp(col('Joining_Date'))
            , 'MM'           
        )
    )
    .display()
)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Get only date part of "JoiningDate".

# COMMAND ----------

(
    df_employee.withColumn(
        'date'
        , date_format(
            to_timestamp(col('Joining_Date'))
            , 'MM/dd/yyyy'           
        )
    )
    .display()
)

# COMMAND ----------


