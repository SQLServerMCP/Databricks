# Databricks notebook source
from pyspark.sql.functions import *

# COMMAND ----------


def kmtomiles (km:float) -> float:
    return km / 8 * 5


kmtomilesudf = udf(kmtomiles)

