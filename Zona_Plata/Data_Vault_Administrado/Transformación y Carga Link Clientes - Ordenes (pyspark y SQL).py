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
# spark = SparkSession.builder.appName("PipelineLinkClientesOrdenes").config("spark.sql.extensions", "io.delta.sql.# DeltasAdministradasparkSessionExtension").config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog").# config("fs.azure", "fs.azure.NativeAzureFileSystem").getOrCreate()

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
          MANAGED LOCATION 'abfss://{contenedorDatalake}@{cuentaDatalake}.dfs.core.windows.net/Silver/DeltasAdministradas/Data_Vault/cri'""")

existe = spark.sql("""SELECT COUNT(*) AS existe FROM data_vault.information_schema.tables WHERE table_name = 'lnk_clientes_ordenes' AND table_schema = 'cri'""").first()["existe"]

if existe == 0: 
    # Creamos la tabla delta de hub_productos de tipo tabla no adminisrada o externa
    spark.sql("USE data_vault.cri")
    spark.sql(f"""
          CREATE TABLE IF NOT EXISTS lnk_clientes_ordenes (
              hk_clientes_ordenes BINARY,
              hk_clientes BINARY,
              hk_ordenes BINARY,
              fecha_registro DATE,
              nombre_fuente STRING
          )
          USING DELTA
          CLUSTER BY (fecha_registro)
          """)
    
    # Habilitamos el AutoOptimizer en la tabla delta para aumentar la capacidad y el rendimiento
    spark.sql("""
            ALTER TABLE lnk_clientes_ordenes
            SET TBLPROPERTIES (
                delta.autoOptimize.optimizeWrite = true,
                delta.autoOptimize.autoCompact = true
            );
          """)


# COMMAND ----------

# procedemos a armar la ruta de la zona de bronce donde estan los archivos parquet
# dado que productos es una tabla maestra, se debe tomar el archivo más reciente en base a la fechaProceso
# rutaBronceProductos = "abfss://"+contenedorDatalake+"@"+cuentaDatalake+".dfs.core.windows.net/Bronze/devvertixddnsnet/bikestores/sales/customers/{"
iterador = str((fechaInicio - timedelta(days=1)))[0:10].replace("-", "")
limiteWhile = str((fechaFin + timedelta(days=1)))[0:10].replace("-", "")

esquemaOrdenes = StructType([
    StructField("order_id", IntegerType()),
    StructField("customer_id", IntegerType()),
    StructField("order_status", IntegerType()),
    StructField("order_date", TimestampType()),
    StructField("required_date", DateType()),
    StructField("shipped_date", DateType()),
    StructField("store_id", IntegerType()),
    StructField("staff_id", IntegerType())
])

dfParquetsBronceOrdenes = spark.createDataFrame(data=[], schema=esquemaOrdenes)

while int(iterador) <= int(limiteWhile):
    rutaBronceParquets = f"abfss://{contenedorDatalake}@{cuentaDatalake}.dfs.core.windows.net/Bronze/devvertixddnsnet/bikestores/sales/orders/{iterador[0:4]}/sales_orders_{iterador}.parquet"

    try:
        dbutils.fs.ls(rutaBronceParquets)
        dfTemp = spark.read.format("parquet").load(rutaBronceParquets)
        dfParquetsBronceOrdenes = dfParquetsBronceOrdenes.unionAll(dfTemp)
    except Exception as e:
        print(f"Ocurrio un error: posiblemente el parquet {rutaBronceParquets} no existe ")

    fechaTemp = date(int(iterador[0:4]), int(iterador[4:6]), int(iterador[6:8]))
    iterador = str(fechaTemp + timedelta(days=1))[0:10].replace("-", "")

# dfParquetsBronce = spark.read.format("parquet").load(archivoParquetBronce)

dfParquetsBronceOrdenes.createOrReplaceTempView("ordenes_bronce")



# COMMAND ----------

resultado = spark.sql(f"""
                        INSERT INTO lnk_clientes_ordenes (hk_clientes, hk_ordenes, hk_clientes_ordenes, fecha_registro, nombre_fuente)
                        SELECT 
                        B.hk_clientes
                        ,C.hk_ordenes
                        ,CAST(
                        sha2((CAST(B.bk_id_cliente AS STRING) ||
                        CAST(C.bk_codigo_orden AS STRING)), 256)
                        AS BINARY) AS hk_clientes_ordenes
                        ,current_date() AS fecha_registro 
                        ,'Bronze/devvertixddnsnet/bikestores/sales/orders' AS nombre_fuente 
                        FROM 
                        (
                        SELECT 
                            order_id
                            ,customer_id
                        FROM 
                            ordenes_bronce
                        WHERE 
                            order_date >= '{fechaInicio}'
                            AND order_date <= '{fechaFin}'
                        GROUP BY 
                            order_id
                            ,customer_id
                        ) AS A 
                        INNER JOIN hub_clientes AS B 
                            ON A.customer_id = B.bk_id_cliente 
                        INNER JOIN hub_ordenes AS C 
                            ON A.order_id = C.bk_codigo_orden
                        LEFT JOIN lnk_clientes_ordenes AS D 
                            ON B.hk_clientes = D.hk_clientes AND C.hk_ordenes = D.hk_ordenes 
                        WHERE 
                        D.hk_clientes IS NULL 
                        AND D.hk_ordenes IS NULL 
                      """)

resultado.show()
