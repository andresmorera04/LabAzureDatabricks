# Databricks notebook source
# MAGIC %md
# MAGIC # Notebook Proceso Batch para tabla delta Hub Clientes
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

# Creamos la tabla delta de hub_productos de tipo tabla no adminisrada o externa
existeTabla = spark.sql("""SELECT COUNT(*) AS existe FROM DataVault.information_schema.tables WHERE table_name = 'Hub_Cli_Clientes' AND table_schema = 'reg'""").first()["existe"]

if existeTabla == 0:
    spark.sql("USE DataVault.reg")
    spark.sql(f"""
          CREATE TABLE IF NOT EXISTS Hub_Cli_Clientes (
              Hk_Clientes BINARY NOT NULL COMMENT 'Hash Key de la tabla Hub',
              Bk_IdCliente INT NOT NULL COMMENT 'Llave de Negocio que identifica de forma única a los clientes',
              FechaRegistro TIMESTAMP NOT NULL COMMENT 'Fecha en la cual se guarda la tupla',
              NombreFuente STRING NOT NULL COMMENT 'Fuente de Datos de la Medalla de Bronce de donde vienen los datos'
          )
          USING DELTA 
          COMMENT 'Medalla: Plata; Descripcion: La Tabla Hub contiene la llave de negocio que identifica de forma única a cada Cliente bajo el Modelo de Data Vault'
          CLUSTER BY (Bk_IdCliente)
          """)
    
    # Habilitamos el AutoOptimizer en la tabla delta para aumentar la capacidad y el rendimiento
    spark.sql("""
            ALTER TABLE Hub_Cli_Clientes
            SET TBLPROPERTIES (
                delta.autoOptimize.optimizeWrite = true,
                delta.autoOptimize.autoCompact = true
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

# Creamos la vista temporal tmp_clientes_bronce para procesa todo en Spark SQL
dfClientesBronce.createOrReplaceTempView("tmp_clientes_bronce")


# COMMAND ----------

# MAGIC %md
# MAGIC **Tercer Bloque de Código**
# MAGIC - Usando Spark SQL con pyspark, procedemos a crear la lógica de transformación, curación, limpieza y carga de los datos a la tabla delta final

# COMMAND ----------

# En base a Spark SQL se realiza la lógica para transformar, curar, aplicar regla de negocio y posteriormente
# hacer la carga de datos 
resultado = spark.sql("""
                    INSERT INTO DataVault.reg.Hub_Cli_Clientes (Hk_Clientes, Bk_IdCliente, FechaRegistro, NombreFuente) 
                    SELECT 
                    B.Hk_Clientes
                    ,B.Bk_IdCliente
                    ,B.FechaRegistro
                    ,B.NombreFuente
                    FROM 
                    (
                    SELECT 
                        CAST(sha2(CAST(A.customer_id AS STRING), 256) AS BINARY) AS Hk_Clientes
                        ,A.customer_id AS Bk_IdCliente
                        ,current_timestamp() AS FechaRegistro
                        ,'Bronze/devvertixddnsnet/bikestores/sales/customers' AS NombreFuente 
                    FROM 
                        (
                        SELECT 
                        CASE 
                            WHEN isnull(customer_id) = TRUE THEN -1
                            ELSE customer_id 
                        END AS customer_id 
                        FROM 
                        tmp_clientes_bronce 
                        GROUP BY 
                        CASE 
                            WHEN isnull(customer_id) = TRUE THEN -1
                            ELSE customer_id 
                        END
                        ) AS A
                    ) AS B 
                    LEFT JOIN DataVault.reg.Hub_Cli_Clientes AS C 
                        ON B.Bk_IdCliente = C.Bk_IdCliente
                    WHERE 
                    C.Bk_IdCliente IS NULL 
                      """)

resultado.show()

