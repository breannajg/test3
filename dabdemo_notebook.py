# Databricks notebook source
# Databricks notebook source

# COMMAND ----------

# Restart Python after installing the Python wheel.
dbutils.library.restartPython()

# COMMAND ----------

#from dabdemo.addcol import with_status

# Filename: addcol.py
import pyspark.sql.functions as F

def with_status(df):
  return df.withColumn("status", F.lit("checked"))
  
df = (spark.createDataFrame(
  schema = ["first_name", "last_name", "email"],
  data = [
    ("paula", "white", "paula.white@example.com"),
    ("john", "baer", "john.baer@example.com")
  ]
))

new_df = with_status(df)

display(new_df)

# Expected output:
#
# +------------+-----------+-------------------------+---------+
# │ first_name │ last_name │ email                   │ status  │
# +============+===========+=========================+=========+
# │ paula      │ white     │ paula.white@example.com │ checked │
# +------------+-----------+-------------------------+---------+
# │ john       │ baer      │ john.baer@example.com   │ checked │
# +------------+-----------+-------------------------+---------+

# COMMAND ----------


