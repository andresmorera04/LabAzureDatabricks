# Databricks notebook source
# Primeros centralizamos en el primer bloque de codigo todas las bibliotecas a utilizar
from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql.types import *
from delta import *
from datetime import *

# Leemos del key vault el secreto que ya tiene la conexión por JDBC con el Azure SQL que tiene los parametos
conexionJdbcAzureSql = dbutils.secrets.get(scope= "dabrsescazurekeyvault", key= "secrjdbcazuresqlconfiguracion")

# Cargamos la tabla dbo.Parametros al Data Frame dfParametros
dfParametros = spark.read.format("jdbc").options(
    url=conexionJdbcAzureSql,
    dbtable="dbo.Parametros",
    driver="com.microsoft.sqlserver.jdbc.SQLServerDriver"
).load()

#Obtenemos todas las variables que vamos a necesitar:
fechaProceso = dfParametros.filter(col("Clave") == "fechaProceso").first().Valor
diasCargarBorrar = dfParametros.filter(col("Clave") == "diasCargarBorrar").first().Valor
dataLake = dfParametros.filter(col("Clave") == "LagoDatos").first().Valor
contenedorReg = dfParametros.filter(col("Clave") == "ContenedorLagoDatosReg").first().Valor
medallaBronce = dfParametros.filter(col("Clave") == "MedallaBronce").first().Valor
CatalogoDataVault = dfParametros.filter(col("Clave") == "DatabricksCatalogoDataVault").first().Valor

#Calculamos la Ventana de Tiempo:
fechaFin = datetime.strptime(fechaProceso, '%Y-%m-%d')
fechaInicio = fechaFin - timedelta(days=int(diasCargarBorrar))


# COMMAND ----------

#Procedemos a leer los datos que necesito de la medalla de bronce aprovechando las bondades 
# de los directorios con clave=valor

# Primero creamos la ruta de los directorios y cargamos esos metadatos
rutaBronceCliente = f"abfss://{contenedorReg}@{dataLake}.dfs.core.windows.net/{medallaBronce}/devvertixddnsnet/bikestores/sales/customers/"
dfClienteBronce = spark.read.format("parquet").load(rutaBronceCliente)

# Posteriormente crear una nueva columna construyendo una fecha a partir de los directorios clave=valor del DataLake
# esto con el objetivo de poder filtrar mejor los archivos parquets a Leer
dfClienteBronce = dfClienteBronce.withColumn("fechaArchivosParquet", concat(col("año"), lit("-"), col("mes"), lit("-"), col("dia")).cast("date"))
dfClienteBronce = dfClienteBronce.filter(col("fechaArchivosParquet") >= fechaInicio).filter(col("fechaArchivosParquet") <= fechaFin)
fechaArchivoReciente = dfClienteBronce.agg(max(col("fechaArchivosParquet")).alias("FechaArchivoReciente")).first()["FechaArchivoReciente"]
dfClienteBronce = dfClienteBronce.filter(col("fechaArchivosParquet") == fechaArchivoReciente)

display


