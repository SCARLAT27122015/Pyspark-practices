# Databricks notebook source
# MAGIC %md
# MAGIC # Ads Performance
# MAGIC
# MAGIC

# COMMAND ----------

# MAGIC %md
# MAGIC Write an pyspark code to find the ctr of each Ad. Round ctr to 2 decimal points. Order the result table by ctr in descending order and by ad_id in ascending order in case of a tie.
# MAGIC
# MAGIC *Ctr = Clicked / (Clicked + Viewed)*

# COMMAND ----------

from pyspark.sql.types import *
from pyspark.sql.functions import *

# COMMAND ----------

# Define the schema for the Ads table
schema = StructType([
    StructField('AD_ID', IntegerType(), True)
    , StructField('USER_ID', IntegerType(), True)
    , StructField('ACTION', StringType(), True)

])


# Define the data for the Ads table
data = [
    (1, 1, 'Clicked'),
    (2, 2, 'Clicked'),
    (3, 3, 'Viewed'),
    (5, 5, 'Ignored'),
    (1, 7, 'Ignored'),
    (2, 7, 'Viewed'),
    (3, 5, 'Clicked'),
    (1, 4, 'Viewed'),
    (2, 11, 'Viewed'),
    (1, 2, 'Clicked')
]


# COMMAND ----------

df = spark.createDataFrame(data, schema)

# COMMAND ----------

df.display()

# COMMAND ----------

(df.groupBy('AD_ID').agg(
        sum( 
            when(
                col('ACTION') == 'Clicked'
                , 1
            ).otherwise(0)
        ).alias('clicked_count')
        , sum(
            when(
                col('ACTION') == 'Viewed'
                , 1
            ).otherwise(0)
        ).alias('viewed_count')
    )
    .withColumn(
        'rate'
        , round(
            col('clicked_count') / (col('clicked_count') + col('viewed_count'))
            , 2
        )
    
    ).select('AD_ID', 'rate')
    .sort('rate', ascending=False)
    .show()
)

# COMMAND ----------


