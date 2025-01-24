# Databricks notebook source
# MAGIC %md
# MAGIC # 15_Teams Power Users

# COMMAND ----------

# MAGIC %md
# MAGIC Write a pyspark code to identify the top 2 Power Users who sent the highest number of messages on Microsoft Teams in August 2022. Display the IDs of these 2 users along with the total number of messages they sent. Output the results in descending order based on the count of the messages.

# COMMAND ----------

from pyspark.sql.functions import *
from pyspark.sql.types import *
from pyspark.sql.window import *

# COMMAND ----------

schema = StructType([ StructField("message_id", IntegerType(), True), StructField("sender_id", IntegerType(), True), StructField("receiver_id", IntegerType(), True), StructField("content", StringType(), True), StructField("sent_date", StringType(), True), ])

data = [ (901, 3601, 4500, 'You up?', '2022-08-03 00:00:00'), (902, 4500, 3601, 'Only if you\'re buying', '2022-08-03 00:00:00'), (743, 3601, 8752, 'Let\'s take this offline', '2022-06-14 00:00:00'), (922, 3601, 4500, 'Get on the call', '2022-08-10 00:00:00'), ]

# COMMAND ----------

df_data = spark.createDataFrame(data, schema)

# COMMAND ----------

df_data.show()

# COMMAND ----------

window_spec = Window.partitionBy().orderBy(col('total_messages').desc() )

# COMMAND ----------

(
    df_data.groupBy('sender_id')
           .agg(
               count('message_id').alias('total_messages')
            )
           .withColumn('rnk', dense_rank().over(window_spec) )
           .filter(col('rnk') <= 2)
           .select('sender_id', 'total_messages')
           .orderBy(col('total_messages').desc())
           .show()
)

# COMMAND ----------


