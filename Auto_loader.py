# Databricks notebook source
from pyspark.sql.types import *
from pyspark.sql.functions import *

# COMMAND ----------

static_df = spark.read.format('csv').option('inferSchema', True).option('header', True).load('/Volumes/workspace/default/test_practice/EMPLOYEE/emp_1.csv')
schema = static_df.schema
static_df.write.format('delta').saveAsTable('emp')

# COMMAND ----------

df = spark.readStream.format('csv').schema(schema).load('/Volumes/workspace/default/test_practice/EMPLOYEE/emp_1.csv',header=True)

# COMMAND ----------

df = spark.readStream \
  .format("cloudFiles") \
  .option("cloudFiles.format", "csv") \
  .option('header',True) \
  .schema(schema) \
  .load("/Volumes/workspace/default/test_practice/EMPLOYEE/")

query = df.writeStream \
  .format("delta") \
  .outputMode("append") \
  .option('mergeSchema' , True) \
  .option("checkpointLocation", "/Volumes/workspace/default/test_practice/checkpoint4/") \
  .trigger(availableNow=True) \
  .toTable("emp_data_1")

# COMMAND ----------

df = spark.readStream.option('header', True).table('workspace.default.emp')
display(df, checkpointLocation="/Volumes/workspace/default/test_practice/checkpoint")

# COMMAND ----------


