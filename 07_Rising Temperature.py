# Databricks notebook source
# MAGIC %md
# MAGIC # 07_Rising Temperature
# MAGIC
# MAGIC

# COMMAND ----------

# MAGIC %md
# MAGIC Write a solution to find all dates' Id with higher temperatures compared to its previous dates (yesterday).
# MAGIC
# MAGIC Return 1  the result table in any order. 2  

# COMMAND ----------

from pyspark.sql.functions import *
from pyspark.sql.types import *
from pyspark.sql.window import *

# COMMAND ----------

# Define the schema for the "Weather" table
weather_schema = StructType([
    StructField("id", IntegerType(), True),
    StructField("recordDate", StringType(), True),
    StructField("temperature", IntegerType(), True)
])

# Define data for the "Weather" table
weather_data = [
    (1, '2015-01-01', 10),
    (2, '2015-01-02', 25),
    (3, '2015-01-03', 20),
    (4, '2015-01-04', 30)
]

# COMMAND ----------

df_weather = spark.createDataFrame(weather_data, weather_schema)

# COMMAND ----------

df_weather.show(5)

# COMMAND ----------

#windowSpec  = Window.partitionBy("id").orderBy("salary")
windowSpec  = Window.partitionBy().orderBy("recordDate")


(
    df_weather.withColumns({
        'lag': lag('temperature', 1).over(windowSpec) 
        , 'eval': col('temperature') > col('lag')
    })
    .filter('eval')
    .select('id')
    .show()
)

# COMMAND ----------


