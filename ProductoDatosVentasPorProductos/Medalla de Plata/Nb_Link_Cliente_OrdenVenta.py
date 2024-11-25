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
rutaBronceOrdenVenta = f"abfss://{contenedorReg}@{dataLake}.dfs.core.windows.net/{medallaBronce}/devvertixddnsnet/bikestores/sales/orders/"
dfOrdenBronce = spark.read.format("parquet").load(rutaBronceOrdenVenta)

# Posteriormente crear una nueva columna construyendo una fecha a partir de los directorios clave=valor del DataLake
# esto con el objetivo de poder filtrar mejor los archivos parquets a Leer
dfOrdenBronce = dfOrdenBronce.withColumn("fechaArchivosParquet", concat(col("año"), lit("-"), col("mes"), lit("-"), col("dia")).cast("date"))
dfOrdenBronce = dfOrdenBronce.filter(col("fechaArchivosParquet") >= fechaInicio).filter(col("fechaArchivosParquet") <= fechaFin)

# Por ultimo, creamos una vista temporal para consumir por spark SQL
dfOrdenBronce.createOrReplaceTempView("tmpOrdenesVenta")


# COMMAND ----------

# Usamos el Catalogo que contendrá los datos en el Modelado de Data Vault
spark.sql(f"USE {catalogoDataVault}.reg")

# Creamos la tabla Delta Hub para almacenar los datos, siempre y cuando no exista
spark.sql(f"""
          CREATE TABLE IF NOT EXISTS Link_Cli_Cliente_OrdenVenta
          (
              HK_Cliente_OrdenVenta BINARY NOT NULL COMMENT 'Llave Hash de la tabla Link calculada a partir de las Llaves de Negocio del Hub Cliente y OrdenVenta. Identifica de forma unica cada relación entre los Hubs de CLientes y Ordenes Ventas'
              ,Hk_Cliente BINARY NOT NULL COMMENT 'Llave Hash calculada a partir de la llave de negocio que identifica de forma unica cada Cliente'
              ,Hk_OrdenVenta BINARY NOT NULL COMMENT 'Llave Hash calculada a partir de la llave de negocio que identifica de forma unica cada Orden de Venta'
              ,FechaRegistro TIMESTAMP NOT NULL COMMENT 'Fecha en la que almacena el registro en la tabla'
              ,FuenteDatos STRING NOT NULL COMMENT 'Fuente de donde prviene el dato en la medalla de bronce'
          )
          USING DELTA
          COMMENT 'Medalla: Plata, Descripcion: Tabla Link del Modelo de Data Vault que relaciona las tablas Hub Clientes y Hub Orden Venta'
          TBLPROPERTIES ('delta.autoOptimize.optimizeWrite' = true, 'delta.autoOptimize.autoCompact' = true)
          CLUSTER BY (FechaRegistro)
          """)

# Aplicamos una lectura con transformación y extrayendo solo los registros nuevos
dfRegistrosNuevos = spark.sql(f"""
                            SELECT 
                                B.HK_Cliente_OrdenVenta
                                ,B.Hk_Cliente
                                ,B.Hk_OrdenVenta
                                ,B.FechaRegistro
                                ,B.FuenteDatos
                            FROM 
                                (
                                SELECT 
                                    CAST(SHA2((CAST(A.order_id AS STRING) || CAST(A.customer_id AS STRING)), 256) AS BINARY) AS HK_Cliente_OrdenVenta 
                                    ,CAST(SHA2(CAST(A.customer_id AS STRING), 256) AS BINARY) AS Hk_Cliente
                                    ,CAST(SHA2(CAST(A.order_id AS STRING), 256) AS BINARY) AS Hk_OrdenVenta
                                    ,CURRENT_TIMESTAMP() AS FechaRegistro
                                    , '{rutaBronceOrdenVenta}' AS FuenteDatos
                                FROM 
                                    (
                                    SELECT 
                                        order_id
                                        ,customer_id 
                                    FROM 
                                        tmpOrdenesVenta
                                    WHERE 
                                        order_date >= '{str(fechaInicio)}' 
                                        AND order_date <= '{str(fechaFin)}'
                                    GROUP BY 
                                        ALL 
                                    ) AS A
                                ) AS B
                                LEFT JOIN Link_Cli_Cliente_OrdenVenta AS C 
                                    ON B.Hk_Cliente = C.Hk_Cliente AND B.Hk_OrdenVenta = C.Hk_OrdenVenta
                            WHERE 
                                C.Hk_Cliente IS NULL 
                                AND C.Hk_OrdenVenta IS NULL 
                              """)

# En caso de que el Data Frame de registros nuevos Si tenga nuevos clientes, se procede a insertarlos en la tabla 
# delta, caso contrario el proceso finaliza sin insercciones

if dfRegistrosNuevos.count() > 0:
    dfHub_Cli_Cliente = DeltaTable.forName(spark, f"{catalogoDataVault}.reg.Link_Cli_Cliente_OrdenVenta")
    dfHub_Cli_Cliente.alias("A").merge(
        dfRegistrosNuevos.alias("B"), 
        "A.Hk_Cliente = B.Hk_Cliente AND A.Hk_OrdenVenta = B.Hk_OrdenVenta"
    ).whenNotMatchedInsertAll().execute()




# COMMAND ----------

# MAGIC %sql
# MAGIC -- Validamos los datos
# MAGIC SELECT 
# MAGIC   COUNT(*) AS Q 
# MAGIC   ,COUNT(DISTINCT A.Hk_Cliente) AS Q_Clientes
# MAGIC   ,COUNT(DISTINCT A.Hk_OrdenVenta) AS Q_Ordenes
# MAGIC FROM 
# MAGIC   Link_Cli_Cliente_OrdenVenta AS A 
# MAGIC   INNER JOIN hub_cli_cliente AS B 
# MAGIC     ON A.Hk_Cliente = B.Hk_Cliente
# MAGIC   INNER JOIN hub_cli_ordenventa AS C 
# MAGIC     ON A.Hk_OrdenVenta = C.Hk_OrdenVenta
# MAGIC LIMIT 50
