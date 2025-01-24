# Databricks notebook source
# MAGIC %md
# MAGIC # 14_Purchasing Activity by Product Type
# MAGIC

# COMMAND ----------

# MAGIC %md
# MAGIC We have been given purchasing activity DF and we need
# MAGIC to find out cumulative purchases of each product over
# MAGIC time.
# MAGIC

# COMMAND ----------

from pyspark.sql.functions import *
from pyspark.sql.window import *
from pyspark.sql.types import *

# COMMAND ----------

# Define schema for the DataFrame
schema = StructType([
StructField("order_id", IntegerType(), True),
StructField("product_type", StringType(), True),
StructField("quantity", IntegerType(), True),
StructField("order_date", StringType(), True),
])
# Define data
# Define data
data = [
(213824, 'printer', 20, "2022-06-27 "),
(212312, 'hair dryer', 5, "2022-06-28 "),
(132842, 'printer', 18, "2022-06-28 "),
(284730, 'standing lamp', 8, "2022-07-05 ")
]

# COMMAND ----------

df_purchases = spark.createDataFrame(data, schema)

# COMMAND ----------

display(df_purchases)

# COMMAND ----------

#define window for calculating cumulative sum
my_window = (Window.partitionBy('product_type').orderBy('order_date')
             .rowsBetween(Window.unboundedPreceding, 0))

# COMMAND ----------

(
    df_purchases.withColumn(
        'cum_sum'
        , sum(col('quantity')).over(my_window)
    ).show()
)

# COMMAND ----------


