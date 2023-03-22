# Databricks notebook source
# MAGIC %md
# MAGIC ![ETL](https://blog.bismart.com/hubfs/Imported_Blog_Media/ETL/20190604_imagen2.jpg)
# MAGIC 
# MAGIC Get data from client source files, loaded into blob storage using ADF and then loaded here for transformation and exporting into Contacts table.

# COMMAND ----------

from pyspark.sql.functions import *
from pyspark.sql.types import *

# COMMAND ----------

# MAGIC %run "./Functions"

# COMMAND ----------

# File location and type
file_location = "/FileStore/tables/ETL"
file_type = "csv"

# CSV options
infer_schema = "true"
first_row_is_header = "true"
delimiter = ","

# The applied options are for CSV files. For other file types, these will be ignored.
df_bookings = spark.read.format(file_type) \
  .option("inferSchema", infer_schema) \
  .option("header", first_row_is_header) \
  .option("sep", delimiter) \
  .load(file_location) \
  .withColumn("fileName", input_file_name()) 
  
    



# COMMAND ----------

display(df_bookings)

# COMMAND ----------

# File location and type
file_location = "/FileStore/tables/Customers"
file_type = "csv"

# CSV options
infer_schema = "true"
first_row_is_header = "true"
delimiter = ","

# The applied options are for CSV files. For other file types, these will be ignored.
df_customer = spark.read.format(file_type) \
  .option("inferSchema", infer_schema) \
  .option("header", first_row_is_header) \
  .option("sep", delimiter) \
  .load(file_location) \
  .withColumn("fileName", input_file_name()) \
  .drop("Latitude","Longitude")

# COMMAND ----------

display(df_customer)

# COMMAND ----------

# File location and type
file_location = "/FileStore/tables/Resorts"
file_type = "csv"

# CSV options
infer_schema = "true"
first_row_is_header = "true"
delimiter = ","

# The applied options are for CSV files. For other file types, these will be ignored.
df_resorts = spark.read.format(file_type) \
  .option("inferSchema", infer_schema) \
  .option("header", first_row_is_header) \
  .option("sep", delimiter) \
  .load(file_location) \
  .withColumn("fileName", input_file_name()) 

# COMMAND ----------

display(df_resorts)

# COMMAND ----------

df_all = df_bookings\
    .join(df_customer, df_bookings.CustomerID == df_customer.ID, "inner") \
    .join(df_resorts, df_resorts.ID == df_bookings.ResortID, "inner") \
    .select("BookingDate", "DepartDate", "ReturnDate", "CustomerID", "FirstName", "Surname", "PhoneNumber", "ResortID", "Resort", "SalesAmount", "Latitude", "Longitude")

# COMMAND ----------

display(df_all)
