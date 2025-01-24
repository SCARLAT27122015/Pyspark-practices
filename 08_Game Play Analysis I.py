# Databricks notebook source
# MAGIC %md
# MAGIC # 08_Game Play Analysis I
# MAGIC
# MAGIC

# COMMAND ----------

# MAGIC %md
# MAGIC Write a solution to find the first login date for each player.
# MAGIC
# MAGIC Return the result table in any order. 1 

# COMMAND ----------

from pyspark.sql.types import *
from pyspark.sql.functions import *
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

df_activity = spark.createDataFrame(activity_data, activity_schema)

# COMMAND ----------

df_activity.show(5)

# COMMAND ----------

orig_cols = df_activity.columns

window_part = Window.partitionBy('player_id').orderBy('event_date')

(
    df_activity.withColumn(
        'rnk'
        , rank().over(window_part)
    )
    .filter(col('rnk') == 1)
    .select(col('player_id'), col('event_date').alias('first_login'))
    .show()
)

# COMMAND ----------


