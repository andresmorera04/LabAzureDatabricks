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

print(fechaFin, fechaInicio)

# COMMAND ----------

#Procedemos a leer los datos que necesito de la medalla de bronce aprovechando las bondades 
# de los directorios con clave=valor

# Primero creamos la ruta de los directorios y cargamos esos metadatos
rutaBronceOrdenes = f"abfss://{contenedorReg}@{dataLake}.dfs.core.windows.net/{medallaBronce}/devvertixddnsnet/bikestores/sales/orders/"
dfOrdenesBronce = spark.read.format("parquet").load(rutaBronceOrdenes)

# Posteriormente crear una nueva columna construyendo una fecha a partir de los directorios clave=valor del DataLake
# esto con el objetivo de poder filtrar mejor los archivos parquets a Leer
dfOrdenesBronce = dfOrdenesBronce.withColumn("fechaArchivosParquet", concat(col("año"), lit("-"), col("mes"), lit("-"), col("dia")).cast("date"))
dfOrdenesBronce = dfOrdenesBronce.filter(col("fechaArchivosParquet") >= fechaInicio).filter(col("fechaArchivosParquet") <= fechaFin)

# Por ultimo, creamos una vista temporal para consumir por spark SQL
dfOrdenesBronce.createOrReplaceTempView("tmpOrdenesBronce")

rutaBronceOrdenesDetalle = f"abfss://{contenedorReg}@{dataLake}.dfs.core.windows.net/{medallaBronce}/devvertixddnsnet/bikestores/sales/order_items/"
dfOrdenesDetalleBronce = spark.read.format("parquet").load(rutaBronceOrdenesDetalle)
dfOrdenesDetalleBronce = dfOrdenesDetalleBronce.withColumn("fechaArchivosParquet", concat(col("año"), lit("-"), col("mes"), lit("-"), col("dia")).cast("date"))
dfOrdenesDetalleBronce = dfOrdenesDetalleBronce.filter(col("fechaArchivosParquet") >= fechaInicio).filter(col("fechaArchivosParquet") <= fechaFin)

dfOrdenesDetalleBronce.createOrReplaceTempView("tmpOrdenesDetalleBronce")


# COMMAND ----------

# Usamos el Catalogo que contendrá los datos en el Modelado de Data Vault
spark.sql(f"USE {catalogoDataVault}.reg")

# Creamos la tabla Delta Hub para almacenar los datos, siempre y cuando no exista
spark.sql(f"""
          CREATE TABLE IF NOT EXISTS Sat_Cli_OrdenVentaEncabezado
          (
              Hk_OrdenVenta BINARY NOT NULL COMMENT 'Llave Hash calculada a partir de la llave de negocio que identifica de forma unica cada Orden de Venta'
              ,HkDiff BINARY NOT NULL COMMENT 'Llave Hash que determina si hay cambios o no en las variables existentes en la tabla'
              ,EstadoOrden INT NOT NULL COMMENT 'Estado de la Orden de Venta'
              ,FechaOrden TIMESTAMP NOT NULL COMMENT 'Fecha en la que se realiza la Orden de Venta'
              ,FechaRequeridaOrden DATE NOT NULL COMMENT 'Fecha en la que se requiere la Orden de Venta'
              ,FechaEnvioOrden DATE NOT NULL COMMENT 'Fecha en la que se envia la Orden de Venta'
              ,SubTotalOrdenVenta DOUBLE NOT NULL COMMENT 'Subtotal de la Orden de Venta'
              ,FechaRegistro TIMESTAMP NOT NULL COMMENT 'Fecha en la que almacena el registro en la tabla'
              ,FuenteDatos STRING NOT NULL COMMENT 'Fuente de donde prviene el dato en la medalla de bronce'
          )
          USING DELTA
          COMMENT 'Medalla: Plata, Descripcion: Tabla Hub del Modelo de Data Vault con las llaves de negocio de las Ordenes de Ventas'
          TBLPROPERTIES ('delta.autoOptimize.optimizeWrite' = true, 'delta.autoOptimize.autoCompact' = true)
          CLUSTER BY (FechaOrden)
          """)

# Aplicamos una lectura con transformación y extrayendo solo los registros nuevos
dfRegistrosNuevos = spark.sql(f"""
                            SELECT 
                                D.Hk_OrdenVenta
                                ,D.HkDiff
                                ,D.EstadoOrden
                                ,D.FechaOrden
                                ,D.FechaRequeridaOrden
                                ,D.FechaEnvioOrden
                                ,D.SubTotalOrdenVenta
                                ,D.FechaRegistro
                                ,D.FuenteDatos
                            FROM 
                                (
                                SELECT 
                                    CAST(SHA2(CAST(C.order_id AS STRING), 256) AS BINARY) AS Hk_OrdenVenta
                                    ,CAST(SHA2((CAST(C.EstadoOrden AS STRING) || CAST(C.FechaOrden AS STRING) || CAST(C.FechaRequeridaOrden AS STRING) || CAST(C.FechaEnvioOrden AS STRING) || CAST(C.SubTotalOrdenVenta AS STRING)), 512) AS BINARY) AS HkDiff
                                    ,CURRENT_TIMESTAMP() AS FechaRegistro
                                    ,C.EstadoOrden
                                    ,C.FechaOrden
                                    ,C.FechaRequeridaOrden
                                    ,C.FechaEnvioOrden
                                    ,C.SubTotalOrdenVenta
                                    ,'abfss://reg@stacownlab30.dfs.core.windows.net/bronze/devvertixddnsnet/bikestores/sales/[orders, order_items]/' AS FuenteDatos
                                FROM 
                                    (
                                    SELECT 
                                        A.order_id 
                                        ,COALESCE(A.order_status, -1) AS EstadoOrden 
                                        ,COALESCE(A.order_date, CAST('1800-01-01' AS TIMESTAMP)) AS FechaOrden
                                        ,COALESCE(A.required_date, CAST('1800-01-01' AS DATE)) AS FechaRequeridaOrden
                                        ,COALESCE(A.shipped_date, CAST('1800-01-01' AS DATE)) AS FechaEnvioOrden
                                        ,SUM(((B.quantity * B.list_price) * (1 - B.discount))) AS SubTotalOrdenVenta
                                    FROM 
                                        tmpOrdenesBronce AS A 
                                        INNER JOIN tmpOrdenesDetalleBronce AS B
                                            ON A.order_id = B.order_id
                                    WHERE 
                                        A.order_date >= '{str(fechaInicio)}' 
                                        AND A.order_date <= '{str(fechaFin)}'
                                    GROUP BY 
                                        A.order_id
                                        ,A.order_status
                                        ,A.order_date
                                        ,A.required_date
                                        ,A.shipped_date
                                    ) AS C
                                ) AS D 
                                LEFT JOIN 
                                (
                                SELECT 
                                    Hk_OrdenVenta
                                    ,HKDiff
                                FROM 
                                    Sat_Cli_OrdenVentaEncabezado 
                                WHERE 
                                    FechaOrden >= '{str(fechaInicio)}' 
                                    AND FechaOrden <= '{str(fechaFin)}'
                                ) AS E 
                                    ON D.Hk_OrdenVenta = E.Hk_OrdenVenta AND D.HkDiff = E.HkDiff
                            WHERE 
                                E.Hk_OrdenVenta IS NULL 
                                AND E.HkDiff IS NULL 
                              """)

# En caso de que el Data Frame de registros nuevos Si tenga nuevos clientes, se procede a insertarlos en la tabla 
# delta, caso contrario el proceso finaliza sin insercciones

if dfRegistrosNuevos.count() > 0:
    dfHub_Cli_Cliente = DeltaTable.forName(spark, f"{catalogoDataVault}.reg.Sat_Cli_OrdenVentaEncabezado")
    dfHub_Cli_Cliente.alias("A").merge(
        dfRegistrosNuevos.alias("B"), 
        "A.Hk_OrdenVenta = B.Hk_OrdenVenta AND A.HkDiff = B.HkDiff"
    ).whenNotMatchedInsertAll().execute()







# COMMAND ----------

# MAGIC %sql
# MAGIC -- Validamos los datos
# MAGIC SELECT * FROM Sat_Cli_OrdenVentaEncabezado -- LIMIT 50
# MAGIC -- DROP TABLE Sat_Cli_OrdenVentaEncabezado
