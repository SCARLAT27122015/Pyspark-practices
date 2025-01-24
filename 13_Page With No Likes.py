# Databricks notebook source
# MAGIC %md
# MAGIC # 13_Page With No Likes

# COMMAND ----------

# MAGIC %md
# MAGIC Write a pyspark code to return the IDs of the Facebook pages
# MAGIC that have zero likes. The output should be sorted in
# MAGIC ascending order based on the page IDs.

# COMMAND ----------

from pyspark.sql.functions import *
from pyspark.sql.types import *
from pyspark.sql.window import *

# COMMAND ----------

# Define the schema for the pages
pages_schema = StructType([
StructField("page_id", IntegerType(), True),
StructField("page_name", StringType(), True)
])
# Define the schema for the page_likes table
page_likes_schema = StructType([
StructField("user_id", IntegerType(), True),
StructField("page_id", IntegerType(), True),
StructField("liked_date", StringType(), True)
])
# Create an RDD with the data for pages
pages_data = [
(20001, 'SQL Solutions'),
(20045, 'Brain Exercises'),
(20701, 'Tips for Data Analysts')
]
# Create an RDD with the data for page_likes table
page_likes_data = [
(111, 20001, '2022-04-08 00:00:00'),
(121, 20045, '2022-03-12 00:00:00'),
(156, 20001, '2022-07-25 00:00:00')
]

# COMMAND ----------

df_pages_data = spark.createDataFrame(pages_data, pages_schema).alias('df_pages_data')
df_likes = spark.createDataFrame(page_likes_data, page_likes_schema).alias('df_likes')

# COMMAND ----------

df_pages_data.show(5)

# COMMAND ----------

df_likes.show(5)

# COMMAND ----------

(df_pages_data.join(
                    df_likes
                    , df_pages_data['page_id'] == df_likes['page_id']
                    , how='left'
               )
               .filter(isnull('liked_date'))
               .select('df_pages_data.page_id')
               .orderBy('df_pages_data.page_id',asc=[True])
               .show()
)

# COMMAND ----------


