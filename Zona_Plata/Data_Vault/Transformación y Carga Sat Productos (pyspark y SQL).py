# Databricks notebook source
# Inicializamos las librerías a Utilizar
from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql.types import *
from delta import *
from datetime import * 

# Desplegamos la sesión de spark para el trabajo distribuido de los nodos del cluster
spark = SparkSession.builder.appName("PipelineSatProductos").config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension").config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog").config("fs.azure", "fs.azure.NativeAzureFileSystem").getOrCreate()

# Leemos el JSON con los parámetros del notebook
cuentaDatalake = "stacownlab30"
contenedorDatalake = "datalake"
dfParametros = spark.read.json(f"abfss://{contenedorDatalake}@{cuentaDatalake}.dfs.core.windows.net/Silver/Configuracion/parametros.json", multiLine=True)

# Variables de fecha de proceso y dias a recargar 
fechaProceso = dfParametros.first()["fechaproceso"]
diascargar = dfParametros.first()["diascargar"]
fechaFin = datetime(int(fechaProceso[0:4]), int(fechaProceso[5:7]), int(fechaProceso[8:10]), 23, 59, 59)
fechaInicio = datetime(int(fechaProceso[0:4]), int(fechaProceso[5:7]), int(fechaProceso[8:10]), 00, 00, 00) - timedelta(days= int(diascargar))

# configuramos los parametros para acceder al datalake del storage account de Azure
cuentaDatalake = dfParametros.first()["cuentadatalake"]
contenedorDatalake = dfParametros.first()["contenedordatalake"]
# scopeSecreto = dfParametros.first()["secretoscopedatabricks"]
# secreto = dfParametros.first()["secretodatalakekeyvault"]
# llaveCuentaDatalake = dbutils.secrets.get(scope= scopeSecreto, key= secreto)

# Configuramos spark con la cuenta de almacenamiento del datalake en Azure
# spark.conf.set(f"fs.azure.account.key.{cuentaDatalake}.dfs.core.windows.net", llaveCuentaDatalake)

# Creamos el Catalogo de Zona_Plata donde estará alojado la base de datos DataVault
spark.sql("CREATE CATALOG IF NOT EXISTS data_vault")

# Creamos la base de datos (esquema en databricks)
spark.sql("CREATE DATABASE IF NOT EXISTS data_vault.common")

# Creamos la tabla delta de hub_productos de tipo tabla no adminisrada o externa
spark.sql("USE data_vault.common")
spark.sql(f"""
          CREATE TABLE IF NOT EXISTS sat_productos (
              hk_productos BINARY,
              nombre STRING,
              categoria STRING,
              marca STRING,
              modelo INT,
              precio_unitario DOUBLE,
              hk_diff BINARY,
              fecha_registro TIMESTAMP,
              nombre_fuente STRING,
              anio_particion INT,
              mes_particion INT
          )
          USING DELTA 
          PARTITIONED BY (anio_particion, mes_particion) 
          LOCATION 'abfss://{contenedorDatalake}@{cuentaDatalake}.dfs.core.windows.net/Silver/Deltas/Data_Vault/common/sat_productos'
          """)

# Habilitamos el AutoOptimizer en la tabla delta para aumentar la capacidad y el rendimiento
spark.sql("""
            ALTER TABLE sat_productos
            SET TBLPROPERTIES (
                delta.autoOptimize.optimizeWrite = true,
                delta.autoOptimize.autoCompact = true
            );
          """)


# COMMAND ----------

# procedemos a armar la ruta de la zona de bronce donde estan los archivos parquet
# dado que productos es una tabla maestra, se debe tomar el archivo más reciente en base a la fechaProceso
# rutaBronceProductos = "abfss://"+contenedorDatalake+"@"+cuentaDatalake+".dfs.core.windows.net/Bronze/devvertixddnsnet/bikestores/production/products/{"
iterador = fechaInicio.year
listaRutasParquetsProductos = []
listaRutasParquetsCategorias = []
listaRutasParquetsMarcas = []
while iterador <= fechaFin.year:
    rutaBronceProductos = f"abfss://{contenedorDatalake}@{cuentaDatalake}.dfs.core.windows.net/Bronze/devvertixddnsnet/bikestores/production/products/{iterador}"
    lista = dbutils.fs.ls(rutaBronceProductos)
    listaRutasParquetsProductos = listaRutasParquetsProductos + lista 

    rutaBronceCategorias = f"abfss://{contenedorDatalake}@{cuentaDatalake}.dfs.core.windows.net/Bronze/devvertixddnsnet/bikestores/production/categories/{iterador}"
    lista = dbutils.fs.ls(rutaBronceCategorias)
    listaRutasParquetsCategorias = listaRutasParquetsCategorias + lista 

    rutaBronceMarcas = f"abfss://{contenedorDatalake}@{cuentaDatalake}.dfs.core.windows.net/Bronze/devvertixddnsnet/bikestores/production/brands/{iterador}"
    lista = dbutils.fs.ls(rutaBronceMarcas)
    listaRutasParquetsMarcas = listaRutasParquetsMarcas + lista 
    
    iterador = iterador + 1

esquemaRutaParquets = StructType([
    StructField("path", StringType()),
    StructField("name", StringType()),
    StructField("size", IntegerType()),
    StructField("modificationTime", LongType())
])

dfRutasParquetsProductos = spark.createDataFrame(data= listaRutasParquetsProductos, schema= esquemaRutaParquets)
dfRutasParquetsCategorias = spark.createDataFrame(data= listaRutasParquetsCategorias, schema= esquemaRutaParquets)
dfRutasParquetsMarcas = spark.createDataFrame(data= listaRutasParquetsMarcas, schema= esquemaRutaParquets)

archivoParquetBronceProductos = dfRutasParquetsProductos.orderBy(col("name").desc()).first()["path"]
archivoParquetBronceCategorias = dfRutasParquetsCategorias.orderBy(col("name").desc()).first()["path"]
archivoParquetBronceMarcas = dfRutasParquetsMarcas.orderBy(col("name").desc()).first()["path"]

dfProductosBronce = spark.read.format("parquet").load(archivoParquetBronceProductos)
dfCategoriasBronce = spark.read.format("parquet").load(archivoParquetBronceCategorias)
dfMarcaBronce = spark.read.format("parquet").load(archivoParquetBronceMarcas)

dfProductosBronce.createOrReplaceTempView("productos_bronce")
dfCategoriasBronce.createOrReplaceTempView("categorias_bronce")
dfMarcaBronce.createOrReplaceTempView("marcas_bronce")

# COMMAND ----------

# MAGIC %sql
# MAGIC INSERT INTO sat_productos 
# MAGIC (hk_productos, nombre, categoria, marca, modelo, precio_unitario, hk_diff, fecha_registro, nombre_fuente, anio_particion, mes_particion)
# MAGIC SELECT 
# MAGIC   D.hk_productos,
# MAGIC   D.nombre,
# MAGIC   D.categoria,
# MAGIC   D.marca,
# MAGIC   D.modelo,
# MAGIC   D.precio_unitario,
# MAGIC   D.hk_diff,
# MAGIC   D.fecha_registro,
# MAGIC   D.nombre_fuente,
# MAGIC   D.anio_particion,
# MAGIC   D.mes_particion
# MAGIC FROM 
# MAGIC (
# MAGIC SELECT
# MAGIC   CAST(
# MAGIC   sha2(CAST(
# MAGIC     CASE 
# MAGIC     WHEN isnull(A.product_id) = TRUE THEN -1
# MAGIC     ELSE A.product_id 
# MAGIC   END 
# MAGIC   AS STRING
# MAGIC   ), 256) 
# MAGIC   AS BINARY) AS hk_productos, 
# MAGIC   A.product_name AS nombre,
# MAGIC   B.category_name AS categoria,
# MAGIC   C.brand_name AS marca,
# MAGIC   A.model_year AS modelo,
# MAGIC   A.list_price AS precio_unitario,
# MAGIC   CAST(
# MAGIC   sha2(
# MAGIC   (CAST(A.product_id AS STRING) || A.product_name  ||  B.category_name  ||  C.brand_name  || CAST(A.model_year AS STRING) 
# MAGIC   || CAST(A.list_price AS STRING))
# MAGIC   , 512)
# MAGIC   AS BINARY)
# MAGIC    AS hk_diff,
# MAGIC   current_timestamp() AS fecha_registro,
# MAGIC   'Bronze/devvertixddnsnet/bikestores/production/' AS nombre_fuente,
# MAGIC   year(current_timestamp()) AS anio_particion, 
# MAGIC   month(current_timestamp()) AS mes_particion
# MAGIC FROM 
# MAGIC   productos_bronce AS A 
# MAGIC   INNER JOIN categorias_bronce AS B 
# MAGIC     ON A.category_id = B.category_id
# MAGIC   INNER JOIN marcas_bronce AS C 
# MAGIC     ON A.brand_id = C.brand_id 
# MAGIC ) AS D 
# MAGIC LEFT JOIN sat_productos AS E 
# MAGIC   ON D.hk_productos = E.hk_productos AND D.hk_diff = E.hk_diff
# MAGIC WHERE 
# MAGIC   E.hk_productos IS NULL 
# MAGIC   AND E.hk_diff IS NULL 
# MAGIC
# MAGIC
# MAGIC
# MAGIC

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM zona_plata.data_vault.sat_productos;
