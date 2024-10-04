# Databricks notebook source
# Inicializamos las librerías a Utilizar
from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql.types import *
from delta import *
from datetime import * 

# Modificamos algunos valores de las propiedades de la sesión para mejorarla 
spark.conf.set("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
spark.conf.set("fs.azure", "fs.azure.NativeAzureFileSystem")

# Procedemos a realizar la conexión con la base de datos que tiene acceso a la tabla parametros
nombreServidor = "devvertix.ddns.net"
puerto = 5722
nombreBaseDatos = "CONFIGURACION"
conexionJdbc = f"jdbc:sqlserver://{nombreServidor}:{puerto};database={nombreBaseDatos};encrypt=true;trustServerCertificate=true;"
usuario = "integrator"
contraseña = dbutils.secrets.get(scope = 'sescdabrownlab30', key = 'secrdevvertixddnsnetbikestores')
tablaParametros = "dbo.Parametros"

propiedadesConexionSql = {
  "user" : usuario,
  "password" : contraseña,
  "driver" : "com.microsoft.sqlserver.jdbc.SQLServerDriver"
};

# Cargamos los datos de la tabla parametros a un Data Frame
dfParametros = spark.read.jdbc(url=conexionJdbc, table=tablaParametros, properties=propiedadesConexionSql)

# Cargamos las variables de parametros a partir del data frame dfParametros
fechaProceso = dfParametros.select("Valor").where(col("Clave") == "FechaProceso").orderBy(col("Codigo").desc()).first()["Valor"]
diascargar = dfParametros.select("Valor").where(col("Clave") == "DiasCargarBorrar").orderBy(col("Codigo").desc()).first()["Valor"]
cuentaDatalake = dfParametros.select("Valor").where(col("Clave") == "LagoDatos").orderBy(col("Codigo").desc()).first()["Valor"]
contenedorDatalake = dfParametros.select("Valor").where(col("Clave") == "ContenedorLagoDatos").orderBy(col("Codigo").desc()).first()["Valor"]
medallaBronce = dfParametros.select("Valor").where(col("Clave") == "MedallaBronce").orderBy(col("Codigo").desc()).first()["Valor"]
medallaPlata = dfParametros.select("Valor").where(col("Clave") == "MedallaPlata").orderBy(col("Codigo").desc()).first()["Valor"]

# Calculamos la ventana de tiempo con las variables fechaInicio y fechaFin
fechaFin = datetime(int(fechaProceso[0:4]), int(fechaProceso[5:7]), int(fechaProceso[8:10]), 23, 59, 59)
fechaInicio = datetime(int(fechaProceso[0:4]), int(fechaProceso[5:7]), int(fechaProceso[8:10]), 00, 00, 00) - timedelta(days= int(diascargar))

# Creamos la tabla delta de hub_productos de tipo tabla no adminisrada o externa
existeTabla = spark.sql("""SELECT COUNT(*) AS existe FROM DataVault.information_schema.tables WHERE table_name = 'Hub_Cli_Clientes' AND table_schema = 'reg'""").first()["existe"]

if existeTabla == 0:
    # Creamos la tabla delta de hub_productos de tipo tabla no adminisrada o externa
    spark.sql("USE DataVault.reg")
    spark.sql(f"""
          CREATE TABLE IF NOT EXISTS Sat_Cli_Clientes_correos (
              hk_clientes BINARY,
              correo_electronico STRING,
              hk_diff BINARY,
              fecha_registro DATE,
              nombre_fuente STRING
          )
          USING DELTA 
          CLUSTER BY (fecha_registro)
          """)
    
    # Habilitamos el AutoOptimizer en la tabla delta para aumentar la capacidad y el rendimiento
    spark.sql("""
            ALTER TABLE Sat_Cli_Clientes_correos
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
# MAGIC INSERT INTO Sat_Cli_Clientes_correos (hk_clientes, correo_electronico, hk_diff, fecha_registro, nombre_fuente )
# MAGIC SELECT 
# MAGIC   A.hk_clientes
# MAGIC   ,A.correo_electronico
# MAGIC   ,A.hk_diff
# MAGIC   ,current_date() AS fecha_registro
# MAGIC   ,'Bronze/devvertixddnsnet/bikestores/sales/customers' AS nombre_fuente
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
# MAGIC   WHERE 
# MAGIC     email IS NOT NULL
# MAGIC     OR email != 'NULL'
# MAGIC   ) AS A 
# MAGIC   LEFT JOIN Sat_Cli_Clientes_correos AS B 
# MAGIC     ON A.hk_clientes = B.hk_clientes AND A.hk_diff = B.hk_diff
# MAGIC WHERE 
# MAGIC   B.hk_clientes IS NULL 
# MAGIC   AND B.hk_diff IS NULL 
# MAGIC ;
# MAGIC
# MAGIC
