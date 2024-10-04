# Databricks notebook source
from pyspark.sql import * 
from pyspark.sql.functions import * 
from pyspark.sql.types import * 
from delta import * 
from datetime import * 

# Modificamos algunos valores de las propiedades de la sesión para mejorarla 
spark.conf.set("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
spark.conf.set("fs.azure", "fs.azure.NativeAzureFileSystem")

# Creamos la vista materializada en el esquema cri
spark.sql("USE exploracion.cri")

# COMMAND ----------

spark.sql(
    """
    CREATE MATERIALIZED VIEW IF NOT EXISTS Vm_ProductosCri 
    AS 
    SELECT 
        IdProducto
        ,NombreProducto
        ,IdMarca
        ,IdCategoria
        ,AnioModelo
        ,Precio
    FROM 
        exploracion.reg.productosdetalle
    WHERE 
        IdProducto >= 1 
        AND IdProducto <= 100
    """
)


# COMMAND ----------

# Creamos la vista materializada en el esquema pan
spark.sql("USE exploracion.pan")

# COMMAND ----------

spark.sql(
    """
    CREATE MATERIALIZED VIEW IF NOT EXISTS Vm_ProductosPan 
    AS 
    SELECT 
        IdProducto
        ,NombreProducto
        ,IdMarca
        ,IdCategoria
        ,AnioModelo
        ,Precio
    FROM 
        exploracion.reg.productosdetalle
    WHERE 
        IdProducto >= 101 
        AND IdProducto <= 200
    """
)
