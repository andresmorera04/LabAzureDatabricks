# Databricks notebook source
# Inicializamos las librerías a Utilizar
from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql.types import *
from delta import *
from datetime import * 

# Desplegamos la sesión de spark para el trabajo distribuido de los nodos del cluster
spark = SparkSession.builder.appName("PipelineSatClientes").config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension").config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog").config("fs.azure", "fs.azure.NativeAzureFileSystem").getOrCreate()

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
          CREATE TABLE IF NOT EXISTS sat_clientes (
              hk_clientes BINARY,
              nombre_cliente STRING,
              apellido_cliente STRING,
              calle STRING,
              ciudad STRING,
              estado STRING,
              codigo_zip STRING,
              hk_diff BINARY,
              fecha_registro TIMESTAMP,
              nombre_fuente STRING,
              anio_particion INT,
              mes_particion INT
          )
          USING DELTA 
          PARTITIONED BY (anio_particion, mes_particion) 
          LOCATION 'abfss://{contenedorDatalake}@{cuentaDatalake}.dfs.core.windows.net/Silver/Deltas/Data_Vault/common/sat_clientes'
          """)

# Habilitamos el AutoOptimizer en la tabla delta para aumentar la capacidad y el rendimiento
spark.sql("""
            ALTER TABLE sat_clientes
            SET TBLPROPERTIES (
                delta.autoOptimize.optimizeWrite = true,
                delta.autoOptimize.autoCompact = true
            );
          """)


spark.sql(f"""
          CREATE TABLE IF NOT EXISTS sat_clientes_telefonos (
              hk_clientes BINARY,
              codigo_pais STRING,
              numero_telefono INT,
              hk_diff BINARY,
              fecha_registro TIMESTAMP,
              nombre_fuente STRING,
              anio_particion INT,
              mes_particion INT
          )
          USING DELTA 
          PARTITIONED BY (anio_particion, mes_particion) 
          LOCATION 'abfss://{contenedorDatalake}@{cuentaDatalake}.dfs.core.windows.net/Silver/Deltas/Data_Vault/common/sat_clientes_telefonos'
          """)

# Habilitamos el AutoOptimizer en la tabla delta para aumentar la capacidad y el rendimiento
spark.sql("""
            ALTER TABLE sat_clientes_telefonos
            SET TBLPROPERTIES (
                delta.autoOptimize.optimizeWrite = true,
                delta.autoOptimize.autoCompact = true
            );
          """)


spark.sql(f"""
          CREATE TABLE IF NOT EXISTS sat_clientes_correos (
              hk_clientes BINARY,
              correo_electronico STRING,
              hk_diff BINARY,
              fecha_registro TIMESTAMP,
              nombre_fuente STRING,
              anio_particion INT,
              mes_particion INT
          )
          USING DELTA 
          PARTITIONED BY (anio_particion, mes_particion) 
          LOCATION 'abfss://{contenedorDatalake}@{cuentaDatalake}.dfs.core.windows.net/Silver/Deltas/Data_Vault/common/sat_clientes_correos'
          """)

# Habilitamos el AutoOptimizer en la tabla delta para aumentar la capacidad y el rendimiento
spark.sql("""
            ALTER TABLE sat_clientes_correos
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
listaRutasParquetsClientes = []
while iterador <= fechaFin.year:
    rutaBronceClientes = f"abfss://{contenedorDatalake}@{cuentaDatalake}.dfs.core.windows.net/Bronze/devvertixddnsnet/bikestores/sales/customers/{iterador}"
    lista = dbutils.fs.ls(rutaBronceClientes)
    listaRutasParquetsClientes = listaRutasParquetsClientes + lista 
    iterador = iterador + 1

esquemaRutaParquets = StructType([
    StructField("path", StringType()),
    StructField("name", StringType()),
    StructField("size", IntegerType()),
    StructField("modificationTime", LongType())
])

dfRutasParquetsClientes = spark.createDataFrame(data= listaRutasParquetsClientes, schema= esquemaRutaParquets)

archivoParquetBronceClientes = dfRutasParquetsClientes.orderBy(col("name").desc()).first()["path"]

dfClientesBronce = spark.read.format("parquet").load(archivoParquetBronceClientes)

dfClientesBronce.createOrReplaceTempView("clientes_bronce")

# COMMAND ----------

# MAGIC %sql
# MAGIC INSERT INTO sat_clientes (hk_clientes, nombre_cliente, apellido_cliente, calle, ciudad, estado, codigo_zip, hk_diff, fecha_registro, nombre_fuente, anio_particion, mes_particion)
# MAGIC SELECT 
# MAGIC   A.hk_clientes
# MAGIC   ,A.first_name AS nombre_cliente
# MAGIC   ,A.last_name AS apellido_cliente
# MAGIC   ,A.street AS calle
# MAGIC   ,A.city AS ciudad 
# MAGIC   ,A.state AS estado 
# MAGIC   ,A.zip_code AS codigo_zip 
# MAGIC   ,A.hk_diff
# MAGIC   ,current_timestamp() AS fecha_registro
# MAGIC   ,'Bronze/devvertixddnsnet/bikestores/sales/customers' AS nombre_fuente
# MAGIC   ,year(current_timestamp()) AS anio_particion
# MAGIC   ,month(current_timestamp()) AS mes_particion 
# MAGIC FROM 
# MAGIC   (
# MAGIC   SELECT 
# MAGIC     CAST(sha2(
# MAGIC     CAST((CASE 
# MAGIC       WHEN ISNULL(customer_id) = TRUE THEN -1
# MAGIC       ELSE customer_id 
# MAGIC     END) AS STRING)
# MAGIC     ,256) AS BINARY) AS hk_clientes
# MAGIC     ,first_name
# MAGIC     ,last_name
# MAGIC     ,street
# MAGIC     ,city
# MAGIC     ,state
# MAGIC     ,zip_code
# MAGIC     ,CAST(sha2(
# MAGIC     (CAST((CASE 
# MAGIC       WHEN ISNULL(customer_id) = TRUE THEN -1
# MAGIC       ELSE customer_id 
# MAGIC     END) AS STRING) || first_name || last_name || street || city || state || zip_code )
# MAGIC     , 512)
# MAGIC     AS BINARY) AS hk_diff
# MAGIC   FROM 
# MAGIC     clientes_bronce
# MAGIC   ) AS A 
# MAGIC   LEFT JOIN sat_clientes AS B 
# MAGIC     ON A.hk_clientes = B.hk_clientes AND A.hk_diff = B.hk_diff
# MAGIC WHERE 
# MAGIC   B.hk_clientes IS NULL 
# MAGIC   AND B.hk_diff IS NULL 
# MAGIC ;
# MAGIC
# MAGIC INSERT INTO sat_clientes_telefonos (hk_clientes, codigo_pais, numero_telefono, hk_diff, fecha_registro, nombre_fuente, anio_particion, mes_particion) 
# MAGIC SELECT 
# MAGIC   A.hk_clientes
# MAGIC   ,A.codigo_pais
# MAGIC   ,A.numero_telefono 
# MAGIC   ,A.hk_diff
# MAGIC   ,current_timestamp() AS fecha_registro
# MAGIC   ,'Bronze/devvertixddnsnet/bikestores/sales/customers' AS nombre_fuente
# MAGIC   ,year(current_timestamp()) AS anio_particion
# MAGIC   ,month(current_timestamp()) AS mes_particion 
# MAGIC FROM 
# MAGIC   ( 
# MAGIC   SELECT 
# MAGIC     CAST(sha2(
# MAGIC     CAST((CASE 
# MAGIC       WHEN ISNULL(customer_id) = TRUE THEN -1
# MAGIC       ELSE customer_id 
# MAGIC     END) AS STRING)
# MAGIC     ,256) AS BINARY) AS hk_clientes
# MAGIC     ,SUBSTRING(
# MAGIC         (CASE 
# MAGIC         WHEN isnull(phone) = TRUE THEN ''
# MAGIC         ELSE phone
# MAGIC       END)
# MAGIC       ,1, charindex(')', (CASE 
# MAGIC         WHEN isnull(phone) = TRUE THEN ''
# MAGIC         ELSE phone
# MAGIC     END)) ) AS codigo_pais 
# MAGIC     ,CASE 
# MAGIC     WHEN ISNULL(CAST(
# MAGIC       REPLACE(
# MAGIC       SUBSTRING((CASE 
# MAGIC         WHEN isnull(phone) = TRUE THEN ''
# MAGIC         ELSE phone
# MAGIC       END)
# MAGIC       ,(charindex(')', (CASE 
# MAGIC         WHEN isnull(phone) = TRUE THEN ''
# MAGIC         ELSE phone
# MAGIC       END)) + 1)
# MAGIC       ,25), '-', '') 
# MAGIC     AS INT ) 
# MAGIC     ) = TRUE THEN -1 
# MAGIC     ELSE  CAST(
# MAGIC       REPLACE(
# MAGIC       SUBSTRING((CASE 
# MAGIC         WHEN isnull(phone) = TRUE THEN ''
# MAGIC         ELSE phone
# MAGIC       END)
# MAGIC       ,(charindex(')', (CASE 
# MAGIC         WHEN isnull(phone) = TRUE THEN ''
# MAGIC         ELSE phone
# MAGIC       END)) + 1)
# MAGIC       ,25), '-', '') 
# MAGIC     AS INT )
# MAGIC     END AS numero_telefono
# MAGIC     ,CAST(sha2(
# MAGIC       (CAST((CASE 
# MAGIC         WHEN ISNULL(customer_id) = TRUE THEN -1
# MAGIC         ELSE customer_id 
# MAGIC       END) AS STRING) || 
# MAGIC       SUBSTRING(
# MAGIC         (CASE 
# MAGIC         WHEN isnull(phone) = TRUE THEN ''
# MAGIC         ELSE phone
# MAGIC       END)
# MAGIC       ,1, charindex(')', (CASE 
# MAGIC         WHEN isnull(phone) = TRUE THEN ''
# MAGIC         ELSE phone
# MAGIC       END)) ) || REPLACE(
# MAGIC       SUBSTRING((CASE 
# MAGIC         WHEN isnull(phone) = TRUE THEN ''
# MAGIC         ELSE phone
# MAGIC       END)
# MAGIC       ,(charindex(')', (CASE 
# MAGIC         WHEN isnull(phone) = TRUE THEN ''
# MAGIC         ELSE phone
# MAGIC       END)) + 1)
# MAGIC       ,25), '-', '') )
# MAGIC       , 512)
# MAGIC     AS BINARY) AS hk_diff
# MAGIC   FROM 
# MAGIC     clientes_bronce
# MAGIC   ) AS A 
# MAGIC   LEFT JOIN sat_clientes_telefonos AS B 
# MAGIC     ON A.hk_clientes = B.hk_clientes AND A.hk_diff = B.hk_diff
# MAGIC WHERE 
# MAGIC   B.hk_clientes IS NULL 
# MAGIC   AND B.hk_diff IS NULL 
# MAGIC ;
# MAGIC
# MAGIC INSERT INTO sat_clientes_correos (hk_clientes, correo_electronico, hk_diff, fecha_registro, nombre_fuente, anio_particion, mes_particion)
# MAGIC SELECT 
# MAGIC   A.hk_clientes
# MAGIC   ,A.correo_electronico
# MAGIC   ,A.hk_diff
# MAGIC   ,current_timestamp() AS fecha_registro
# MAGIC   ,'Bronze/devvertixddnsnet/bikestores/sales/customers' AS nombre_fuente
# MAGIC   ,year(current_timestamp()) AS anio_particion
# MAGIC   ,month(current_timestamp()) AS mes_particion 
# MAGIC FROM 
# MAGIC   ( 
# MAGIC   SELECT 
# MAGIC     CAST(sha2(
# MAGIC     CAST((CASE 
# MAGIC       WHEN ISNULL(customer_id) = TRUE THEN -1
# MAGIC       ELSE customer_id 
# MAGIC     END) AS STRING)
# MAGIC     ,256) AS BINARY) AS hk_clientes
# MAGIC     ,CASE 
# MAGIC       WHEN ISNULL(email) = TRUE THEN 'No Definido' 
# MAGIC       ELSE email
# MAGIC     END AS correo_electronico 
# MAGIC     ,CAST(
# MAGIC     sha2((CAST((CASE 
# MAGIC       WHEN ISNULL(customer_id) = TRUE THEN -1
# MAGIC       ELSE customer_id 
# MAGIC     END) AS STRING) || 
# MAGIC     CASE 
# MAGIC       WHEN ISNULL(email) = TRUE THEN 'No Definido' 
# MAGIC       ELSE email
# MAGIC     END ), 512)
# MAGIC     AS BINARY) AS hk_diff  
# MAGIC   FROM 
# MAGIC     clientes_bronce
# MAGIC   ) AS A 
# MAGIC   LEFT JOIN sat_clientes_correos AS B 
# MAGIC     ON A.hk_clientes = B.hk_clientes AND A.hk_diff = B.hk_diff
# MAGIC WHERE 
# MAGIC   B.hk_clientes IS NULL 
# MAGIC   AND B.hk_diff IS NULL 
# MAGIC ;
# MAGIC
# MAGIC
# MAGIC
# MAGIC
# MAGIC
# MAGIC
# MAGIC

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM zona_plata.data_vault.sat_clientes;

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM zona_plata.data_vault.sat_clientes_telefonos;

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM zona_plata.data_vault.sat_clientes_correos;
