# Databricks notebook source
# MAGIC %md
# MAGIC # 22_Date in pyspark

# COMMAND ----------

# MAGIC %md
# MAGIC Write a pyspark code perform below function 
# MAGIC * Get the first name, current date, joiningdate and diff between current date and joining date in months. 
# MAGIC * Get the first name, current date, joiningdate and diff between current date and joining date in days. 
# MAGIC * Get all employee details from EmployeeDetail table whose joining year is 2013

# COMMAND ----------

from pyspark.sql.functions import *
from pyspark.sql.types import *
from pyspark.sql.window import *

# COMMAND ----------

data = [ [1, "Vikas", "Ahlawat", 600000.0, "2013-02-15 11:16:28.290", "IT", "Male"], [2, "nikita", "Jain", 530000.0, "2014-01-09 17:31:07.793", "HR", "Female"], [3, "Ashish", "Kumar", 1000000.0, "2014-01-09 10:05:07.793", "IT", "Male"], [4, "Nikhil", "Sharma", 480000.0, "2014-01-09 09:00:07.793", "HR", "Male"], [5, "anish", "kadian", 500000.0, "2014-01-09 09:31:07.793", "Payroll", "Male"], ]

schema = StructType([ StructField("EmployeeID", IntegerType(), True), StructField("First_Name", StringType(), True), StructField("Last_Name", StringType(), True), StructField("Salary", DoubleType(), True), StructField("Joining_Date", StringType(), True), StructField("Department", StringType(), True), StructField("Gender", StringType(), True) ])

# COMMAND ----------

df_employee = spark.createDataFrame(data, schema)
df_employee.display()

# COMMAND ----------

# MAGIC %md
# MAGIC ### Get the first name, current date, joiningdate and diff between current date and joining date in months.

# COMMAND ----------

(
  df_employee.select('First_Name', 'Joining_Date')
             .withColumns({
                'current_date': current_date()
                , 'diff': months_between(
                            current_date()
                            , to_timestamp(col('Joining_Date'))
                          )
              })
              .display()
)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Get the first name, current date, joiningdate and diff between current date and joining date in days.

# COMMAND ----------

(
  df_employee.select('First_Name', 'Joining_Date')
             .withColumns({
                'current_date': current_date()
                , 'diff': datediff(
                            current_date()
                            , to_timestamp(col('Joining_Date'))
                          )
              })
              .display()
)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Get all employee details from EmployeeDetail table whose joining year is 2013

# COMMAND ----------

df_employee.filter(year(col('Joining_Date')) == '2013' ).display()


# COMMAND ----------


