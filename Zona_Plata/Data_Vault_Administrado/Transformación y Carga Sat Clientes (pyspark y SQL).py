# Databricks notebook source
# MAGIC %md
# MAGIC # Notebook Proceso Batch para tabla delta Sat Clientes
# MAGIC Proceso de Transformación y carga de datos al Modelo de Data Vault en Zona Silver del Lakehouse.

# COMMAND ----------

# MAGIC %md
# MAGIC **Configuraciones Iniciales:**
# MAGIC - Librerías de Uso
# MAGIC - Lectura de los parámetros desde tabla de parametros y otras calculadas
# MAGIC - Validar Existencia de la tabla delta y en caso de no existir crearla
# MAGIC - Aplicar propiedades de optimización sobre las tablas delta

# COMMAND ----------

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

# Creamos el Catalogo de Zona_Plata donde estará alojado la base de datos DataVault
spark.sql("CREATE CATALOG IF NOT EXISTS data_vault")

# Creamos la base de datos (esquema en databricks)
spark.sql(f"""CREATE DATABASE IF NOT EXISTS data_vault.reg 
          MANAGED LOCATION 'abfss://{contenedorDatalake}@{cuentaDatalake}.dfs.core.windows.net/{medallaPlata}/DeltasAdministradas/Data_Vault/reg'""")

existe = spark.sql("""SELECT COUNT(*) AS existe FROM data_vault.information_schema.tables WHERE table_name = 'Sat_Cli_Clientes' AND table_schema = 'reg'""").first()["existe"] 

if existe == 0: 
    # Creamos la tabla delta de hub_productos de tipo tabla no adminisrada o externa
    spark.sql("USE data_vault.reg")
    spark.sql(f"""
          CREATE TABLE IF NOT EXISTS Sat_Cli_Clientes (
              Hk_Clientes BINARY NOT NULL COMMENT 'Llave HashKey de Clientes',
              NombreCliente STRING NOT NULL COMMENT 'Nombre del Cliente',
              ApellidosCliente STRING NOT NULL COMMENT 'Apellidos del Cliente',
              Calle STRING NOT NULL COMMENT 'Nombre de la Calle donde Vive el Cliente',
              Ciudad STRING NOT NULL COMMENT 'Nombre de la Ciudad donde Vive el Cliente',
              Estado STRING NOT NULL COMMENT 'Nombre del Estado donde Vive el Cliente',
              CodigoZip STRING NOT NULL COMMENT 'Codigo ZIP de donde vive el cliente',
              HkDiff BINARY NOT NULL COMMENT 'Llave Hash Key Diferenciadora, si hay registros nuevos los detecta',
              Fecharegistro TIMESTAMP NOT NULL COMMENT 'Fecha de Control donde se registra cuando ingresa un registro nuevo',
              NombreFuente STRING NOT NULL COMMENT 'Fuente de Datos en Bronce'
          )
          USING DELTA 
          COMMENT 'Medalla: Plata; Descripcion: La Tabla Satelite que contiene las variables cualitativas y cuantitativas de los clientes'
          CLUSTER BY (Fecharegistro) 
          """)
    
    # Habilitamos el AutoOptimizer en la tabla delta para aumentar la capacidad y el rendimiento
    spark.sql("""
            ALTER TABLE Sat_Cli_Clientes
            SET TBLPROPERTIES (
                'delta.autoOptimize.optimizeWrite' = true,
                'delta.autoOptimize.autoCompact' = true
            );
          """)


# COMMAND ----------

# MAGIC %md
# MAGIC **Segundo Bloque de código:**
# MAGIC - Lectura de archivos parquet, basandonos en la fechaFin a raíz del parametro fecha proceso.
# MAGIC - Si el archivo con esa fecha en el nombre no existe, obtenemos el archivo más reciente de la medalla de bronce específica de esta fuente
# MAGIC - Paso siguiente, cargamos un Data Frame con esos datos y creamos una vista Temporal para el procesamiento.

# COMMAND ----------

# procedemos a armar la ruta de la zona de bronce donde estan los archivos parquet
# dado que productos es una tabla maestra, se debe tomar el archivo más reciente en base a la fechaProceso

# Primero Validamos si el archivo con la fecha mas reciente existe, caso contrario vamos al directorio y tomamos el más reciente que haya
print(str(fechaFin)[0: 10].replace('-', ''))
rutaBronceProductos = f"abfss://{contenedorDatalake}@{cuentaDatalake}.dfs.core.windows.net/{medallaBronce}/devvertixddnsnet/bikestores/sales/customers/{fechaFin.year}/sales_customers_{str(fechaFin)[0: 10].replace('-', '')}.parquet"
listaRutasParquets = []

# El bloque del Try buscar leer el archivo en base a la fecha fin, armando toda la ruta y guardando la lista resultante
try:
    lista = dbutils.fs.ls(rutaBronceProductos)
    listaRutasParquets = listaRutasParquets + lista
    print(rutaBronceProductos)
# Si el Try falla, entonces se enlistan todos los archivos del directorio de la medalla de bronce correspondiente a este fuentes
# específica y se guarda el listado
except:
    iterador = fechaInicio.year
    while iterador <= fechaFin.year:
        rutaBronceProductos = f"abfss://{contenedorDatalake}@{cuentaDatalake}.dfs.core.windows.net/{medallaBronce}/devvertixddnsnet/bikestores/sales/customers/{iterador}"
        lista = dbutils.fs.ls(rutaBronceProductos)
        listaRutasParquets = listaRutasParquets + lista 
        iterador = iterador + 1
    print(rutaBronceProductos) 

esquemaRutaParquets = StructType([
    StructField("path", StringType()),
    StructField("name", StringType()),
    StructField("size", IntegerType()),
    StructField("modificationTime", LongType())
])
# se arma un Data Frame a base de la lista y el esquema anterior
dfRutasParquets = spark.createDataFrame(data= listaRutasParquets, schema= esquemaRutaParquets)

# Garantizamos obtener el path del archivo más reciente
archivoParquetBronce = dfRutasParquets.orderBy(col("name").desc()).first()["path"]

# cargamos el contenido del parquet en el Data Frame dfClientesBronce
dfClientesBronce = spark.read.format("parquet").load(archivoParquetBronce)

# Creamos la vista temporal tmp_tmp_clientes_bronce para procesa todo en Spark SQL
dfClientesBronce.createOrReplaceTempView("tmp_clientes_bronce")

# COMMAND ----------

# MAGIC %md
# MAGIC **Tercer Bloque de Código**
# MAGIC - Usando Spark SQL con pyspark, procedemos a crear la lógica de transformación, curación, limpieza y carga de los datos a la tabla delta final

# COMMAND ----------

# MAGIC %sql
# MAGIC INSERT INTO Sat_Cli_Clientes (Hk_Clientes, NombreCliente, ApellidosCliente, Calle, Ciudad, Estado, CodigoZip, HkDiff, Fecharegistro, NombreFuente)
# MAGIC SELECT 
# MAGIC   A.hk_clientes
# MAGIC   ,A.first_name AS NombreCliente
# MAGIC   ,A.last_name AS ApellidosCliente
# MAGIC   ,A.street AS Calle
# MAGIC   ,A.city AS Ciudad 
# MAGIC   ,A.state AS Estado 
# MAGIC   ,A.zip_code AS CodigoZip 
# MAGIC   ,A.hk_diff AS HkDiff
# MAGIC   ,current_timestamp() AS FechaRegistro
# MAGIC   ,'Bronze/devvertixddnsnet/bikestores/sales/customers' AS NombreFuente
# MAGIC FROM 
# MAGIC   (
# MAGIC   SELECT 
# MAGIC     CAST(sha2(
# MAGIC     CAST((CASE 
# MAGIC       WHEN ISNULL(customer_id) = TRUE THEN -1
# MAGIC       ELSE customer_id 
# MAGIC     END) AS STRING)
# MAGIC     ,256) AS BINARY) AS Hk_clientes
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
# MAGIC     tmp_clientes_bronce
# MAGIC   ) AS A 
# MAGIC   LEFT JOIN Sat_Cli_Clientes AS B 
# MAGIC     ON A.Hk_Clientes = B.Hk_Clientes AND A.hk_diff = B.HkDiff
# MAGIC WHERE 
# MAGIC   B.Hk_Clientes IS NULL 
# MAGIC   AND B.HkDiff IS NULL 
# MAGIC ;
# MAGIC
# MAGIC
# MAGIC
# MAGIC

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT 
# MAGIC   *
# MAGIC FROM data_vault.reg.Sat_Cli_Clientes
