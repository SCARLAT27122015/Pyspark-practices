# Databricks notebook source
# MAGIC %md
# MAGIC > # 12_Cities With Completed Trades

# COMMAND ----------

# MAGIC %md
# MAGIC Write a pypsark code to retrieve the top three cities that have the highest number of completed trade orders listed in descending order. Output the city name and the corresponding number of completed trade orders.

# COMMAND ----------

from pyspark.sql.functions import *
from pyspark.sql.types import *
from pyspark.sql.window import *

# COMMAND ----------

trades_schema = StructType([ StructField("order_id", IntegerType(), True), StructField("user_id", IntegerType(), True), StructField("price", FloatType(), True), StructField("quantity", IntegerType(), True), StructField("status", StringType(), True), StructField("timestamp", StringType(), True) ])

# COMMAND ----------

users_schema = StructType([ StructField("user_id", IntegerType(), True), StructField("city", StringType(), True), StructField("email", StringType(), True), StructField("signup_date", StringType(), True) ])

# COMMAND ----------

trades_data = [ (100101, 111, 9.80, 10, 'Cancelled', '2022-08-17 12:00:00'), (100102, 111, 10.00, 10, 'Completed', '2022-08-17 12:00:00'), (100259, 148, 5.10, 35, 'Completed', '2022-08-25 12:00:00'), (100264, 148, 4.80, 40, 'Completed', '2022-08-26 12:00:00'), (100305, 300, 10.00, 15, 'Completed', '2022-09-05 12:00:00'), (100400, 178, 9.90, 15, 'Completed', '2022-09-09 12:00:00'), (100565, 265, 25.60, 5, 'Completed', '2022-12-19 12:00:00') ]

# COMMAND ----------

users_data = [ (111, 'San Francisco', 'rrok10@gmail.com', '2021-08-03 12:00:00'), (148, 'Boston', 'sailor9820@gmail.com', '2021-08-20 12:00:00'), (178, 'San Francisco', 'harrypotterfan182@gmail.com', '2022-01-05 12:00:00'), (265, 'Denver', 'shadower_@hotmail.com', '2022-02-26 12:00:00'), (300, 'San Francisco', 'houstoncowboy1122@hotmail.com', '2022-06-30 12:00:00') ]

# COMMAND ----------

df_trades = spark.createDataFrame(trades_data, trades_schema)
df_users = spark.createDataFrame(users_data, users_schema)

# COMMAND ----------

df_trades.show(5)

# COMMAND ----------

df_users.show(5)

# COMMAND ----------

(
    df_users.join(
                df_trades
                , df_users['user_id'] == df_trades['user_id']
                , how='right'
            ).groupBy('city')
            .agg(
                count('order_id').alias('total_orders')
            )
            .orderBy(
                col('total_orders')
                , ascending=[False]
            )
            .show()
)

# COMMAND ----------


