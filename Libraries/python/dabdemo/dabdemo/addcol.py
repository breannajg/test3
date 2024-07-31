# Filename: addcol.py
#The addcol.py file contains a library function that is built later into a Python wheel file and then installed on Azure Databricks clusters. It is a simple function that adds a new column, populated by a literal, to an Apache Spark DataFrame:
import pyspark.sql.functions as F

def with_status(df):
  return df.withColumn("status", F.lit("checked"))