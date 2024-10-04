# Databricks notebook source
from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql.types import *
from delta import *
from datetime import *

spark.sql("USE dwh.common")


# COMMAND ----------

# MAGIC %sql
# MAGIC REFRESH MATERIALIZED VIEW dwh.common.vm_dim_cliente
