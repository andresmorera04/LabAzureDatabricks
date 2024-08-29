# Databricks notebook source
# MAGIC %md
# MAGIC # Notebook Hub Clientes
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
spark = SparkSession.builder.appName("PipelineHubClientes").config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension").config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog").config("fs.azure", "fs.azure.NativeAzureFileSystem").getOrCreate()

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
spark.sql(f"""CREATE DATABASE IF NOT EXISTS data_vault.cri 
          MANAGED LOCATION 'abfss://{contenedorDatalake}@{cuentaDatalake}.dfs.core.windows.net/Silver/DeltasAdministradas/Data_Vault/cri'
          """)

# Creamos la tabla delta de hub_productos de tipo tabla no adminisrada o externa
existeTabla = spark.sql("""SELECT COUNT(*) AS existe FROM data_vault.information_schema.tables WHERE table_name = 'hub_clientes' AND table_schema = 'cri'""").first()["existe"]

if existeTabla == 0:
    spark.sql("USE data_vault.cri")
    spark.sql(f"""
          CREATE TABLE IF NOT EXISTS hub_clientes (
              hk_clientes BINARY,
              bk_id_cliente INT,
              fecha_registro TIMESTAMP,
              nombre_fuente STRING
          )
          USING DELTA
          CLUSTER BY (bk_id_cliente)
          """)
    
    # Habilitamos el AutoOptimizer en la tabla delta para aumentar la capacidad y el rendimiento
    spark.sql("""
            ALTER TABLE hub_clientes
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
# rutaBronceProductos = "abfss://"+contenedorDatalake+"@"+cuentaDatalake+".dfs.core.windows.net/Bronze/devvertixddnsnet/bikestores/sales/customers/{2023, 2024, 2025}/**"
iterador = fechaInicio.year
listaRutasParquets = []
while iterador <= fechaFin.year:
    rutaBronceProductos = f"abfss://{contenedorDatalake}@{cuentaDatalake}.dfs.core.windows.net/Bronze/devvertixddnsnet/bikestores/sales/customers/{iterador}"
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

dfClientesBronce = spark.read.format("parquet").load(archivoParquetBronce)

dfClientesBronce.createOrReplaceTempView("clientes_bronce")


# COMMAND ----------

# MAGIC %md
# MAGIC Utilizando SQL, realizamos la lógica para transformar los datos y crear el hub de productos

# COMMAND ----------

# MAGIC %sql 
# MAGIC INSERT INTO hub_clientes (hk_clientes, bk_id_cliente, fecha_registro, nombre_fuente) 
# MAGIC SELECT 
# MAGIC   B.hk_cliente
# MAGIC   ,B.bk_id_cliente
# MAGIC   ,B.fecha_registro
# MAGIC   ,B.fuente_datos
# MAGIC FROM 
# MAGIC   (
# MAGIC   SELECT 
# MAGIC     CAST(sha2(CAST(A.customer_id AS STRING), 256) AS BINARY) AS hk_cliente
# MAGIC     ,A.customer_id AS bk_id_cliente
# MAGIC     ,current_timestamp() AS fecha_registro
# MAGIC     ,'Bronze/devvertixddnsnet/bikestores/sales/customers' AS fuente_datos
# MAGIC   FROM 
# MAGIC     (
# MAGIC     SELECT 
# MAGIC       CASE 
# MAGIC         WHEN isnull(customer_id) = TRUE THEN -1
# MAGIC         ELSE customer_id 
# MAGIC       END AS customer_id 
# MAGIC     FROM 
# MAGIC       clientes_bronce
# MAGIC     GROUP BY 
# MAGIC       CASE 
# MAGIC         WHEN isnull(customer_id) = TRUE THEN -1
# MAGIC         ELSE customer_id 
# MAGIC       END
# MAGIC     ) AS A
# MAGIC   ) AS B 
# MAGIC   LEFT JOIN hub_clientes AS C 
# MAGIC     ON B.bk_id_cliente = C.bk_id_cliente
# MAGIC WHERE 
# MAGIC   C.bk_id_cliente IS NULL 
