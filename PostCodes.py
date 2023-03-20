# Databricks notebook source
# MAGIC %md
# MAGIC ![Hotel](https://img.ideal-postcodes.co.uk/uk-postcode-components.gif)
# MAGIC 
# MAGIC This notebook will show Postcode Data for UK Postcodes

# COMMAND ----------

from pyspark.sql.functions import *
from pyspark.sql.types import *

# COMMAND ----------

# MAGIC %run "./Functions"

# COMMAND ----------

# File location and type
file_location = "/FileStore/tables/postcodes"
file_type = "csv"

# CSV options
infer_schema = "true"
first_row_is_header = "false"
delimiter = ","

# The applied options are for CSV files. For other file types, these will be ignored.
df = spark.read.format(file_type) \
  .option("inferSchema", infer_schema) \
  .option("header", first_row_is_header) \
  .option("sep", delimiter) \
  .load(file_location) \
  .where("_c11 like '%TS24 9N%'") \
  .withColumnRenamed("_c0", "postCode") \
  .withColumnRenamed("_c12", "Area") \
  .withColumnRenamed("_c13", "outCode") \
  .withColumnRenamed("_c16", "inCode") \
  .withColumnRenamed("_c14", "Sector") \
  .withColumnRenamed("_c6", "country") \
  .withColumnRenamed("_c7", "latitude") \
  .withColumnRenamed("_c8","longitude") \
  .withColumn("fileName", input_file_name()) \
  .select("postCode", "area", "outCode", "inCode", "sector", "country", "latitude", "longitude", "fileName")
    




# COMMAND ----------

display(df)
