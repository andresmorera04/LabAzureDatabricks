# Databricks notebook source
from pyspark.sql import * 
from pyspark.sql.functions import * 
from pyspark.sql.types import * 
from delta import * 
from datetime import * 

# Modificamos algunos valores de las propiedades de la sesión para mejorarla 
spark.conf.set("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
spark.conf.set("fs.azure", "fs.azure.NativeAzureFileSystem")

# Creamos la tabla delta
spark.sql("USE exploracion.reg")

spark.sql(
    """
    CREATE TABLE IF NOT EXISTS ResumenCategoriasProductos
    (
        IdCategoria INT NOT NULL
        ,CantidadProductos INT NOT NULL 
        ,TotalPrecio DOUBLE NOT NULL
        ,PromedioPrecio DOUBLE NOT NULL
    )
    USING DELTA 
    TBLPROPERTIES ('delta.autoOptimize.optimizeWrite' = true, 'delta.autoOptimize.autoCompact' = true)
    CLUSTER BY (IdCategoria)
    """
)

# COMMAND ----------

# Por medio de Query generamos la logica de los datos
dfDatosProcesados = spark.sql(
    """
        SELECT 
            A.IdCategoria
            ,A.CantidadProductos
            ,A.TotalPrecio 
            ,A.PromedioPrecio
        FROM 
            (
            SELECT 
                IdCategoria
                ,COUNT(DISTINCT IdProducto) AS CantidadProductos
                ,SUM(Precio) AS TotalPrecio 
                ,AVG(Precio) AS PromedioPrecio
            FROM 
                exploracion.reg.productosdetalle
            GROUP BY 
                IdCategoria
            ) AS A 
            LEFT JOIN ResumenCategoriasProductos AS B 
                ON A.IdCategoria = B.IdCategoria
        WHERE 
            B.IdCategoria IS NULL
    """)

dfResumenCategoriasProductos = DeltaTable.forName(spark, "exploracion.reg.ResumenCategoriasProductos")

dfResumenCategoriasProductos.alias("A").merge(
    dfDatosProcesados.alias("B"), 
    "A.IdCategoria = B.IdCategoria"
).whenNotMatchedInsertAll().execute()

