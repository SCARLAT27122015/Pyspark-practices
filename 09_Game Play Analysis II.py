# Databricks notebook source
# MAGIC %md
# MAGIC # 09_Game Play Analysis II
# MAGIC

# COMMAND ----------

# MAGIC %md
# MAGIC Write a pyspark code that reports the device that is first
# MAGIC logged in for each player.
# MAGIC Return the result table in any order.

# COMMAND ----------

from pyspark.sql.functions import *
from pyspark.sql.types import *
from pyspark.sql.window import *

# COMMAND ----------

# Define the schema for the "Activity"
activity_schema = StructType([
StructField("player_id", IntegerType(), True),
StructField("device_id", IntegerType(), True),
StructField("event_date", StringType(), True),
StructField("games_played", IntegerType(), True)
])
# Define data for the "Activity"
activity_data = [
(1, 2, '2016-03-01', 5),
(1, 2, '2016-05-02', 6),
(2, 3, '2017-06-25', 1),
(3, 1, '2016-03-02', 0),
(3, 4, '2018-07-03', 5)
]


# COMMAND ----------

df_devices = spark.createDataFrame(activity_data, activity_schema)

# COMMAND ----------

df_devices.show(5)

# COMMAND ----------

window_partition = Window.partitionBy('player_id').orderBy('event_date')

(
    df_devices.withColumn('rnk', rank().over(window_partition))
              .filter(col('rnk') == 1)
              .select('player_id', 'device_id')
              .show()
)

# COMMAND ----------


