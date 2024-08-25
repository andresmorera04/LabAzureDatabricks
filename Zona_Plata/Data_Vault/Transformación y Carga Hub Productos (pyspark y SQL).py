# Databricks notebook source
# MAGIC %md
# MAGIC # Notebook Hub Productos
# MAGIC Proceso de Transformación y carga de datos al Modelo de Data Vault en Zona Silver del Lakehouse.

# COMMAND ----------

# MAGIC %md
# MAGIC **Configuraciones Iniciales:**
# MAGIC - Librerías de Uso
# MAGIC - Lectura de los parámetros desde el archivo JSON
# MAGIC - Crear el esquema de Catalogo, base de datos y tabla delta
# MAGIC - Iniciar la sesión de spark

# COMMAND ----------

# Inicializamos las librerías a Utilizar
from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql.types import *
from delta import *
from datetime import * 

# Desplegamos la sesión de spark para el trabajo distribuido de los nodos del cluster
spark = SparkSession.builder.appName("PipelineHubProductos").config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension").config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog").config("fs.azure", "fs.azure.NativeAzureFileSystem").getOrCreate()

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
          CREATE TABLE IF NOT EXISTS hub_productos (
              hk_productos BINARY,
              bk_id_producto INT,
              fecha_registro TIMESTAMP,
              nombre_fuente STRING
          )
          USING DELTA
          LOCATION 'abfss://{contenedorDatalake}@{cuentaDatalake}.dfs.core.windows.net/Silver/Deltas/Data_Vault/common/hub_productos'
          """)

# Habilitamos el AutoOptimizer en la tabla delta para aumentar la capacidad y el rendimiento
spark.sql("""
            ALTER TABLE hub_productos
            SET TBLPROPERTIES (
                delta.autoOptimize.optimizeWrite = true,
                delta.autoOptimize.autoCompact = true
            );
          """)



# COMMAND ----------

# MAGIC %md
# MAGIC Leemos los datos lógicamente de acuerdo a la fechaproceso del json de parámetros y cargamos un DF que vamos a interpretrar como una tabla temporal. Importante rescatar que tomamos en cuenta los datos por el año de la fechaProceso y buscamos el más reciente de acuerdo al formato del nombre

# COMMAND ----------

# procedemos a armar la ruta de la zona de bronce donde estan los archivos parquet
# dado que productos es una tabla maestra, se debe tomar el archivo más reciente en base a la fechaProceso
# rutaBronceProductos = "abfss://"+contenedorDatalake+"@"+cuentaDatalake+".dfs.core.windows.net/Bronze/devvertixddnsnet/bikestores/production/products/{"
iterador = fechaInicio.year
listaRutasParquets = []
while iterador <= fechaFin.year:
    rutaBronceProductos = f"abfss://{contenedorDatalake}@{cuentaDatalake}.dfs.core.windows.net/Bronze/devvertixddnsnet/bikestores/production/products/{iterador}"
    lista = dbutils.fs.ls(rutaBronceProductos)
    listaRutasParquets = listaRutasParquets + lista 
    iterador = iterador + 1

esquemaRutaParquets = StructType([
    StructField("path", StringType()),
    StructField("name", StringType()),
    StructField("size", IntegerType()),
    StructField("modificationTime", LongType())
])

dfRutasParquets = spark.createDataFrame(data= listaRutasParquets, schema= esquemaRutaParquets)

archivoParquetBronce = dfRutasParquets.orderBy(col("name").desc()).first()["path"]

dfProductosBronce = spark.read.format("parquet").load(archivoParquetBronce)

dfProductosBronce.createOrReplaceTempView("productos_bronce")


# COMMAND ----------

# MAGIC %md
# MAGIC Utilizando SQL, realizamos la lógica para transformar los datos y crear el hub de productos

# COMMAND ----------

# MAGIC %sql 
# MAGIC INSERT INTO hub_productos (hk_productos, bk_id_producto, fecha_registro, nombre_fuente) 
# MAGIC SELECT 
# MAGIC   B.hk_producto
# MAGIC   ,B.bk_id_producto
# MAGIC   ,B.fecha_registro
# MAGIC   ,B.fuente_datos
# MAGIC FROM 
# MAGIC   (
# MAGIC   SELECT 
# MAGIC     CAST(sha2(CAST(A.product_id AS STRING), 256) AS BINARY) AS hk_producto
# MAGIC     ,A.product_id AS bk_id_producto
# MAGIC     ,current_timestamp() AS fecha_registro
# MAGIC     ,'Bronze/devvertixddnsnet/bikestores/production/products' AS fuente_datos
# MAGIC   FROM 
# MAGIC     (
# MAGIC     SELECT 
# MAGIC       CASE 
# MAGIC         WHEN isnull(product_id) = TRUE THEN -1
# MAGIC         ELSE product_id
# MAGIC       END AS product_id
# MAGIC     FROM 
# MAGIC       productos_bronce
# MAGIC     GROUP BY 
# MAGIC       CASE 
# MAGIC         WHEN isnull(product_id) = TRUE THEN -1
# MAGIC         ELSE product_id
# MAGIC       END
# MAGIC     ) AS A
# MAGIC   ) AS B 
# MAGIC   LEFT JOIN hub_productos AS C 
# MAGIC     ON B.bk_id_producto = C.bk_id_producto
# MAGIC WHERE 
# MAGIC   C.bk_id_producto IS NULL 

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM zona_plata.data_vault.hub_productos;

# COMMAND ----------

dbutils.secrets.list('sescdabrownlab30')
