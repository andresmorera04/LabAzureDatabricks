# Databricks notebook source
from pyspark.sql import * 
from pyspark.sql.functions import * 
from pyspark.sql.types import * 
from delta import * 
from datetime import * 
# bronze/test_coe/sandbox_cri/bikestores/production/product/2024
# Manejamos los parametros del Notebook por medio de los Widgets


dbutils.widgets.text("datalake", "", "Nombre del Datalake")
dbutils.widgets.text("contenedor", "", "Nombre del Contenedor")
dbutils.widgets.text("rutaParquetBronce", "", "Ruta del Archivo Parquet")
dbutils.widgets.text("nombreParquet", "", "Nombre del Archivo Parquet")

# Obtenemos los valores de los parametros del noitebook
datalake = dbutils.widgets.get("datalake")
contenedor = dbutils.widgets.get("contenedor")
rutaParquetBronce = dbutils.widgets.get("rutaParquetBronce")
nombreParquet = dbutils.widgets.get("nombreParquet")

# Modificamos algunos valores de las propiedades de la sesión para mejorarla 
spark.conf.set("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
spark.conf.set("fs.azure", "fs.azure.NativeAzureFileSystem")

# Creamos la tabla delta
spark.sql("USE exploracion.reg")

spark.sql(
    """
    CREATE TABLE IF NOT EXISTS ProductosDetalle
    (
        IdProducto INT NOT NULL 
        ,NombreProducto STRING NOT NULL 
        ,IdMarca INT NOT NULL 
        ,IdCategoria INT NOT NULL
        ,AnioModelo INT NOT NULL 
        ,Precio DOUBLE NOT NULL
    )
    USING DELTA 
    TBLPROPERTIES ('delta.autoOptimize.optimizeWrite' = true, 'delta.autoOptimize.autoCompact' = true)
    CLUSTER BY (IdProducto)
    """
)



# COMMAND ----------

# Procedemos a leer el archivo parquet
rutaParquetBronce = "abfss://" + contenedor + "@" + datalake + ".dfs.core.windows.net/" + rutaParquetBronce + nombreParquet
dfBronce = spark.read.format("parquet").load(rutaParquetBronce)
dfBronce.createOrReplaceTempView("tmp_ProductosBronce")

# COMMAND ----------

# Hacemos el Query de SQL con la lógica y guardamos su resultado en el Data Frame dfDatosProcesados 
dfDatosProcesados = spark.sql("""
            SELECT 
                A.IdProducto
                ,A.NombreProducto
                ,A.IdMarca
                ,A.IdCategoria
                ,A.AnioModelo
                ,A.Precio
            FROM 
                (
                SELECT 
                    product_id AS IdProducto
                    ,product_name AS NombreProducto
                    ,brand_id AS IdMarca
                    ,category_id AS IdCategoria
                    ,model_year AS AnioModelo
                    ,list_price AS Precio
                FROM 
                    tmp_ProductosBronce
                WHERE
                    product_name IS NOT NULL 
                    AND product_name != 'NULL'
                ) AS A
                LEFT JOIN ProductosDetalle AS B 
                    ON A.IdProducto = B.IdProducto
            WHERE 
                B.IdProducto IS NULL 
          """)

# Cargamos la tabla delta ProductosDetalle en el Data Frame dfProductosDetalle

dfProductosDetalle = DeltaTable.forName(spark, "exploracion.reg.ProductosDetalle")

# Usando merge de pyspark, agregamos los datos a la tabla delta

dfProductosDetalle.alias("a").merge(
    dfDatosProcesados.alias("b"),
    "a.IdProducto = b.IdProducto"
).whenNotMatchedInsertAll().execute()

