# Databricks notebook source
# MAGIC %md
# MAGIC # Actors and Directors Who Cooperated At Least Three Times
# MAGIC
# MAGIC

# COMMAND ----------

# MAGIC %md
# MAGIC Write a pyspark Program for a report that provides the pairs (actor_id, director_id) where the actor has cooperated with the director at least 3 times.
# MAGIC
# MAGIC

# COMMAND ----------

from pyspark.sql.types import *
from pyspark.sql.functions import *

# COMMAND ----------

schema = StructType([
    StructField("ActorId", IntegerType(), True),
    StructField("DirectorId", IntegerType(), True),
    StructField("timestamp", IntegerType(), True)
])

data = [
    (1, 1, 0),
    (1, 1, 1),
    (1, 1, 2),
    (1, 2, 3),
    (1, 2, 4),
    (2, 1, 5),
    (2, 1, 6)
]

# COMMAND ----------

df = spark.createDataFrame(data, schema)

# COMMAND ----------

df.display()

# COMMAND ----------

(df.groupBy('ActorId', 'DirectorId')
   .count()
   .filter(col('count') >=3)
   .select('ActorId', 'DirectorId')
   .show()
)
