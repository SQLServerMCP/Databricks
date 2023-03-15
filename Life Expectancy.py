# Databricks notebook source
# MAGIC %md
# MAGIC ![Hotel](https://www.medylife.com/blog/wp-content/uploads/2019/11/Life-Expectancy-01-250x175.jpg)
# MAGIC 
# MAGIC Life expectancy data

# COMMAND ----------

from pyspark.sql.functions import *
from pyspark.sql.types import *

# COMMAND ----------

# MAGIC %run "./Functions"

# COMMAND ----------

# File location and type
file_location = "/FileStore/tables/life"
file_type = "csv"

# CSV options
infer_schema = "true"
first_row_is_header = "true"
delimiter = ","

# The applied options are for CSV files. For other file types, these will be ignored.
df = spark.read.format(file_type) \
  .option("inferSchema", infer_schema) \
  .option("header", first_row_is_header) \
  .option("sep", delimiter) \
  .load(file_location) \
 
    

df.createOrReplaceTempView("LifeData")

# COMMAND ----------

countries = spark.sql("select distinct(country_code) from LifeData").rdd.map(lambda row : row[0]).collect()
countries.sort()

# COMMAND ----------

dbutils.widgets.dropdown("country", "GBR", [str(x) for x in countries])

# COMMAND ----------

df = df.where(col("country_code") == dbutils.widgets.get('country'))

# COMMAND ----------

display(df)
