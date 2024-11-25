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
catalogoDataVault = dfParametros.filter(col("Clave") == "DatabricksCatalogoDataVault").first().Valor

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

# Por ultimo, creamos una vista temporal para consumir por spark SQL
dfClienteBronce.createOrReplaceTempView("tmpClientesBronce")


# COMMAND ----------

# Usamos el Catalogo que contendrá los datos en el Modelado de Data Vault
spark.sql(f"USE {catalogoDataVault}.reg")

# Creamos la tabla Delta Hub para almacenar los datos, siempre y cuando no exista
spark.sql(f"""
          CREATE TABLE IF NOT EXISTS Sat_Cli_Cliente_Correos 
          (
              Hk_Cliente BINARY NOT NULL COMMENT 'Llave Hash calculada a partir de la llave de negocio que identifica de forma unica cada Cliente'
              ,HkDiff BINARY NOT NULL COMMENT 'Llave Hash calculada con el objetivo controlar cambios en las demas variables de los clientes, para agregar solo los valores cambiantes'
              ,FechaRegistro TIMESTAMP NOT NULL COMMENT 'Fecha en la que almacena el registro en la tabla'
              ,CorreoElectronico STRING NOT NULL COMMENT 'Correo Electronico del Cliente'
              ,FuenteDatos STRING NOT NULL COMMENT 'Fuente de donde prviene el dato en la medalla de bronce'
          )
          USING DELTA
          COMMENT 'Medalla: Plata, Descripcion: Tabla Sat del Modelo de Data Vault con los datos de Correos Electronicos de los Clientes'
          TBLPROPERTIES ('delta.autoOptimize.optimizeWrite' = true, 'delta.autoOptimize.autoCompact' = true)
          CLUSTER BY (FechaRegistro)
          """)

# Aplicamos una lectura con transformación y extrayendo solo los registros nuevos
dfRegistrosNuevos = spark.sql(f"""
                            SELECT 
                                B.Hk_Cliente
                                ,B.HkDiff
                                ,B.FechaRegistro
                                ,B.CorreoElectronico
                                ,B.FuenteDatos
                            FROM 
                                (
                                SELECT 
                                    CAST(SHA2(CAST(A.customer_id AS STRING), 256) AS BINARY) AS Hk_Cliente 
                                    ,CAST(SHA2((CAST(A.customer_id AS STRING) || A.CorreoElectronico), 512) AS BINARY) AS HkDiff
                                    ,current_timestamp() AS FechaRegistro
                                    ,A.CorreoElectronico
                                    ,'{rutaBronceCliente}' AS FuenteDatos
                                FROM 
                                    (
                                    SELECT 
                                        customer_id
                                        ,COALESCE(NULLIF(TRIM(email), ''), 'No Definido') AS CorreoElectronico
                                    FROM 
                                        tmpClientesBronce
                                    WHERE 
                                        email IS NOT NULL 
                                    GROUP BY 
                                    ALL 
                                    ) AS A
                                ) AS B 
                                LEFT JOIN 
                                (
                                    SELECT 
                                    C.Hk_Cliente
                                    ,C.HkDiff
                                    FROM 
                                    (
                                    SELECT 
                                        Hk_Cliente
                                        ,HkDiff
                                        ,FechaRegistro
                                        ,ROW_NUMBER()OVER(PARTITION BY Hk_Cliente ORDER BY FechaRegistro DESC) AS UltimoRegistro
                                    FROM 
                                        Sat_Cli_Cliente_Correos
                                    ) AS C 
                                    WHERE 
                                    C.UltimoRegistro = 1
                                ) AS D 
                                    ON B.Hk_Cliente = D.Hk_Cliente AND B.HkDiff = D.HkDiff
                            WHERE 
                                D.Hk_Cliente IS NULL 
                                AND D.HkDiff IS NULL 
                              """)

# En caso de que el Data Frame de registros nuevos Si tenga nuevos clientes, se procede a insertarlos en la tabla 
# delta, caso contrario el proceso finaliza sin insercciones

if dfRegistrosNuevos.count() > 0:
    dfHub_Cli_Cliente = DeltaTable.forName(spark, f"{catalogoDataVault}.reg.Sat_Cli_Cliente_Correos")
    dfHub_Cli_Cliente.alias("A").merge(
        dfRegistrosNuevos.alias("B"), 
        "A.Hk_Cliente = B.Hk_Cliente AND A.HkDiff = B.HkDiff"
    ).whenNotMatchedInsertAll().execute()







# COMMAND ----------

# MAGIC %sql
# MAGIC -- Validamos los datos
# MAGIC
# MAGIC SELECT 
# MAGIC   COUNT(*) AS Q
# MAGIC FROM 
# MAGIC   Sat_Cli_Cliente_Correos
# MAGIC
# MAGIC       
