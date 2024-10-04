# Databricks notebook source
# Inicializamos las librerías a Utilizar
from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql.types import *
from delta import *
from datetime import * 
import pandas as pd

# Desplegamos la sesión de spark para el trabajo distribuido de los nodos del cluster
spark = SparkSession.builder.appName("PipelineDimTiempo").config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension").config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog").config("fs.azure", "fs.azure.NativeAzureFileSystem").getOrCreate()

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
spark.sql("CREATE CATALOG IF NOT EXISTS dwh")

# Creamos la base de datos (esquema en databricks)
spark.sql("CREATE DATABASE IF NOT EXISTS dwh.common")

# Creamos la tabla delta de hub_productos de tipo tabla no adminisrada o externa
spark.sql("USE dwh.common")
spark.sql(f"""
          CREATE TABLE IF NOT EXISTS dim_tiempo (
              dim_tiempo_id INT NOT NULL,
              fecha DATE NOT NULL,
              mes INT NOT NULL,
              anio INT NOT NULL,
              trimestre INT NOT NULL,
              dia_semana INT NOT NULL,
              semestre INT NOT NULL,
              fecha_registro TIMESTAMP NOT NULL
          )
          USING DELTA
          LOCATION 'abfss://{contenedorDatalake}@{cuentaDatalake}.dfs.core.windows.net/Gold/Deltas/DWH/common/dim_tiempo'
          """)

# Habilitamos el AutoOptimizer en la tabla delta para aumentar la capacidad y el rendimiento
spark.sql("""
            ALTER TABLE dim_tiempo 
            SET TBLPROPERTIES (
                delta.autoOptimize.optimizeWrite = true,
                delta.autoOptimize.autoCompact = true
            );
          """)

# COMMAND ----------

iterador = str(fechaInicio)[0:10].replace("-", "")
finWhile = str(fechaFin)[0:10].replace("-", "")
lista = []
while int(iterador) <= int(finWhile) :
    lista.append(iterador)
    fechaTemp = date(int(iterador[0:4]), int(iterador[4:6]), int(iterador[6:8]))
    iterador = str((fechaTemp + timedelta(days= 1)))[0:10].replace("-", "")

dfPandasLista = pd.DataFrame(lista, columns=["fechaProceso"])

dfTiempo = spark.createDataFrame(dfPandasLista)

dfTiempo.createOrReplaceTempView("datos_fechas")

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC /* Eliminar los registros de las fechas coincidentes */
# MAGIC
# MAGIC DELETE FROM dim_tiempo 
# MAGIC WHERE dim_tiempo_id IN (SELECT fechaProceso FROM datos_fechas);
# MAGIC
# MAGIC /* insertar los registros de las respectivas fechas */
# MAGIC
# MAGIC INSERT INTO dim_tiempo (dim_tiempo_id, fecha, mes, anio, trimestre, dia_semana, semestre, fecha_registro)
# MAGIC SELECT 
# MAGIC   A.dim_tiempo_id 
# MAGIC   ,CAST((LEFT(A.fechaString, 4) || '-' || SUBSTRING(A.fechaString, 5, 2) || '-' || SUBSTRING(A.fechaString, 7, 2)) AS DATE) AS Fecha
# MAGIC   ,CAST(SUBSTRING(A.fechaString, 5, 2) AS INT) AS mes
# MAGIC   ,CAST(LEFT(A.fechaString, 4) AS INT) AS anio 
# MAGIC   ,DATE_PART('QTR', CAST((LEFT(A.fechaString, 4) || '-' || SUBSTRING(A.fechaString, 5, 2) || '-' || SUBSTRING(A.fechaString, 7, 2)) AS DATE)) AS trimestre 
# MAGIC   ,DATE_PART('DOW', CAST((LEFT(A.fechaString, 4) || '-' || SUBSTRING(A.fechaString, 5, 2) || '-' || SUBSTRING(A.fechaString, 7, 2)) AS DATE)) AS dia_semana
# MAGIC   ,CASE 
# MAGIC     WHEN CAST(SUBSTRING(A.fechaString, 5, 2) AS INT) <= 6 THEN 1
# MAGIC     ELSE 2
# MAGIC   END AS semestre
# MAGIC   ,current_timestamp() AS fecha_registro 
# MAGIC FROM 
# MAGIC   (
# MAGIC   SELECT 
# MAGIC     fechaProceso AS dim_tiempo_id
# MAGIC     ,CAST(fechaProceso AS STRING)  AS fechaString
# MAGIC   FROM 
# MAGIC     datos_fechas
# MAGIC   ) AS A
# MAGIC   ;
# MAGIC

# COMMAND ----------

# agregamos el registro dummy validando si el mismo existe o no existe
existeDummy = spark.sql("SELECT COUNT(*) AS existe FROM dim_tiempo WHERE dim_tiempo_id = 18000101").collect()[0]["existe"]

if existeDummy == 0 :
    spark.sql("""
              INSERT INTO dim_tiempo (dim_tiempo_id, fecha, mes, anio, trimestre, dia_semana, semestre, fecha_registro)
              VALUES (18000101, '1800-01-01', 1800, 1, 1, 1, 1, current_timestamp())
              """)


# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT 
# MAGIC   *
# MAGIC FROM 
# MAGIC   zona_oro.dwh.dim_tiempo;
