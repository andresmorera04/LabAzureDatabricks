# Databricks notebook source
# MAGIC %md
# MAGIC # Notebook Hub Ventas (Ordenes)
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
spark = SparkSession.builder.appName("PipelineSatOrdenes").config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension").config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog").config("fs.azure", "fs.azure.NativeAzureFileSystem").getOrCreate()

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
          CREATE TABLE IF NOT EXISTS sat_ordenes_encabezado (
              hk_ordenes BINARY,
              estado_orden STRING,
              fecha_orden TIMESTAMP,
              fecha_requerida DATE,
              fecha_entrega DATE,
              total DOUBLE,
              hk_diff BINARY,
              fecha_registro TIMESTAMP,
              nombre_fuente STRING,
              anio_particion INT,
              mes_particion INT
          )
          USING DELTA
          PARTITIONED BY (anio_particion, mes_particion)
          LOCATION 'abfss://{contenedorDatalake}@{cuentaDatalake}.dfs.core.windows.net/Silver/Deltas/Data_Vault/common/sat_ordenes_encabezado'
          """)

# Habilitamos el AutoOptimizer en la tabla delta para aumentar la capacidad y el rendimiento
# Solo se ejecuta la primera vez que creamos la tabla
spark.sql("""
            ALTER TABLE sat_ordenes_encabezado
            SET TBLPROPERTIES (
                delta.autoOptimize.optimizeWrite = true,
                delta.autoOptimize.autoCompact = true
            );
          """)


spark.sql(f"""
          CREATE TABLE IF NOT EXISTS sat_ordenes_detalle (
              hk_ordenes BINARY,
              id_linea_detalle BIGINT,
              id_producto INT,
              cantidad INT,
              precio_lista DOUBLE,
              descuento DOUBLE,
              hk_diff BINARY,
              fecha_registro TIMESTAMP,
              nombre_fuente STRING,
              anio_particion INT,
              mes_particion INT
          )
          USING DELTA
          PARTITIONED BY (anio_particion, mes_particion)
          LOCATION 'abfss://{contenedorDatalake}@{cuentaDatalake}.dfs.core.windows.net/Silver/Deltas/Data_Vault/common/sat_ordenes_detalle'
          """)

# Habilitamos el AutoOptimizer en la tabla delta para aumentar la capacidad y el rendimiento
# Solo se ejecuta la primera vez que creamos la tabla
spark.sql("""
            ALTER TABLE sat_ordenes_detalle
            SET TBLPROPERTIES (
                delta.autoOptimize.optimizeWrite = true,
                delta.autoOptimize.autoCompact = true
            );
          """)


# COMMAND ----------

# MAGIC %sql
# MAGIC /*
# MAGIC DROP TABLE IF EXISTS sat_ordenes_encabezado;
# MAGIC DROP TABLE IF EXISTS sat_ordenes_detalle;
# MAGIC */

# COMMAND ----------

# MAGIC %md
# MAGIC Leemos los datos lógicamente de acuerdo a la fechaproceso del json de parámetros y cargamos un DF que vamos a interpretrar como una tabla temporal. Importante rescatar que tomamos en cuenta los datos por el año de la fechaProceso y buscamos el más reciente de acuerdo al formato del nombre

# COMMAND ----------

# procedemos a armar la ruta de la zona de bronce donde estan los archivos parquet
# dado que productos es una tabla maestra, se debe tomar el archivo más reciente en base a la fechaProceso
# rutaBronceProductos = "abfss://"+contenedorDatalake+"@"+cuentaDatalake+".dfs.core.windows.net/Bronze/devvertixddnsnet/bikestores/sales/customers/{"
iterador = str((fechaInicio - timedelta(days=1)))[0:10].replace("-", "") # 20240730
limiteWhile = str((fechaFin + timedelta(days=1)))[0:10].replace("-", "") # 20240818

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

esquemaLineas = StructType([
    StructField("order_id", IntegerType()),
    StructField("item_id", IntegerType()),
    StructField("product_id", IntegerType()),
    StructField("quantity", IntegerType()),
    StructField("list_price", DoubleType()),
    StructField("discount", DoubleType())
])

dfParquetsBronceOrdenes = spark.createDataFrame(data=[], schema=esquemaOrdenes)
dfParquetsBronceLineas = spark.createDataFrame(data=[], schema=esquemaLineas)

while int(iterador) <= int(limiteWhile):
    rutaBronceParquets = f"abfss://{contenedorDatalake}@{cuentaDatalake}.dfs.core.windows.net/Bronze/devvertixddnsnet/bikestores/sales/orders/{iterador[0:4]}/sales_orders_{iterador}.parquet"

    rutaBronceParquetsLineas = f"abfss://{contenedorDatalake}@{cuentaDatalake}.dfs.core.windows.net/Bronze/devvertixddnsnet/bikestores/sales/order_items/{iterador[0:4]}/sales_order_items_{iterador}.parquet"

    try:
        dbutils.fs.ls(rutaBronceParquets)
        dfTemp = spark.read.format("parquet").load(rutaBronceParquets)
        dfTemp2 = spark.read.format("parquet").load(rutaBronceParquetsLineas)
        dfParquetsBronceOrdenes = dfParquetsBronceOrdenes.unionAll(dfTemp)
        dfParquetsBronceLineas = dfParquetsBronceLineas.unionAll(dfTemp2)
    except Exception as e:
        print(f"Ocurrio un error: posiblemente el parquet {rutaBronceParquets} no existe o es otro error: {e}")

    fechaTemp = date(int(iterador[0:4]), int(iterador[4:6]), int(iterador[6:8]))
    iterador = str(fechaTemp + timedelta(days=1))[0:10].replace("-", "")

# dfParquetsBronce = spark.read.format("parquet").load(archivoParquetBronce)

dfParquetsBronceOrdenes.createOrReplaceTempView("ordenes_bronce")
dfParquetsBronceLineas.createOrReplaceTempView("lineas_ordenes_bronce")


# COMMAND ----------

# MAGIC %md
# MAGIC Utilizando SQL, realizamos la lógica para transformar los datos y crear el hub de productos

# COMMAND ----------

resultado = spark.sql(f"""
                    INSERT INTO sat_ordenes_encabezado (hk_ordenes, estado_orden, fecha_orden, fecha_requerida, fecha_entrega, total, hk_diff, fecha_registro, nombre_fuente, anio_particion, mes_particion)
                    SELECT 
                    E.hk_ordernes
                    ,E.estado_orden
                    ,E.fecha_orden
                    ,E.fecha_requerida
                    ,E.fecha_entrega
                    ,E.Total
                    ,E.hk_diff
                    ,current_timestamp() AS fecha_registro
                    ,'Bronze/devvertixddnsnet/bikestores/sales/[orders, order_items]' AS nombre_fuente
                    ,YEAR(E.fecha_orden) AS anio_particion
                    ,MONTH(E.fecha_orden) AS mes_particion
                    FROM 
                    (
                    SELECT 
                        D.hk_ordernes
                        ,D.estado_orden
                        ,D.fecha_orden
                        ,D.fecha_requerida
                        ,D.fecha_entrega
                        ,D.Total
                        ,CAST(
                        sha2((CAST(D.order_id AS STRING) ||
                        D.estado_orden || 
                        CAST(D.fecha_orden AS STRING) ||
                        CAST(D.fecha_requerida AS STRING) ||
                        CAST(D.fecha_entrega AS STRING) ||
                        CAST(D.Total AS STRING))
                        , 512) AS BINARY) AS hk_diff
                    FROM 
                        (
                        SELECT 
                        B.order_id
                        ,B.hk_ordernes
                        ,B.customer_id
                        ,CASE 
                            WHEN B.order_status = 1 THEN 'Pendiente'
                            WHEN B.order_status = 2 THEN 'Procesando'
                            WHEN B.order_status = 3 THEN 'Rechazado'
                            WHEN B.order_status = 4 THEN 'Completado'
                            ELSE 'No Definido'
                        END AS estado_orden
                        ,B.order_date AS fecha_orden
                        ,B.required_date AS fecha_requerida
                        ,B.shipped_date AS fecha_entrega
                        ,B.store_id
                        ,B.staff_id
                        ,ROUND(SUM(((C.quantity * C.list_price)*(1+C.discount))), 4) AS Total 
                        FROM 
                        (
                        SELECT 
                            A.order_id
                            ,A.hk_ordernes
                            ,A.customer_id
                            ,A.order_status
                            ,A.order_date
                            ,A.required_date
                            ,A.shipped_date
                            ,A.store_id
                            ,A.staff_id
                        FROM 
                            (
                            SELECT 
                            order_id
                            ,CAST(sha2(CAST(order_id AS STRING), 256) AS BINARY) AS hk_ordernes
                            ,customer_id
                            ,order_status
                            ,order_date
                            ,required_date
                            ,shipped_date
                            ,store_id
                            ,staff_id
                            ,ROW_NUMBER()OVER(PARTITION BY order_id ORDER BY order_status) AS desduplicador
                            FROM 
                            ordenes_bronce
                            WHERE 
                            order_date >= '{fechaInicio}'
                            AND order_date <= '{fechaFin}'
                            ) AS A 
                        WHERE 
                            A.desduplicador = 1
                        ) AS B 
                        INNER JOIN lineas_ordenes_bronce AS C 
                            ON B.order_id = C.order_id
                        GROUP BY 
                        B.order_id
                        ,B.hk_ordernes
                        ,B.customer_id
                        ,B.order_status
                        ,B.order_date
                        ,B.required_date
                        ,B.shipped_date
                        ,B.store_id
                        ,B.staff_id
                        ) AS D
                    ) AS E 
                    LEFT JOIN sat_ordenes_encabezado AS F 
                        ON E.hk_ordernes = F.hk_ordenes AND E.hk_diff = F.hk_diff
                    WHERE 
                    F.hk_ordenes IS NULL 
                    AND F.hk_diff IS NULL 
          """)

resultado.show()

resultado = spark.sql(f"""
                        INSERT INTO sat_ordenes_detalle (hk_ordenes, id_linea_detalle, id_producto, cantidad, precio_lista, descuento, hk_diff, fecha_registro, nombre_fuente, anio_particion, mes_particion) 
                        SELECT 
                        C.hk_ordenes
                        ,C.id_linea_detalle
                        ,C.id_producto 
                        ,C.cantidad 
                        ,C.precio_lista 
                        ,C.descuento 
                        ,C.hk_diff
                        ,current_timestamp() AS fecha_registro
                        ,'Bronze/devvertixddnsnet/bikestores/sales/[orders, order_items]' AS nombre_fuente
                        ,YEAR(C.order_date) AS anio_particion
                        ,MONTH(C.order_date) AS mes_particion
                        FROM 
                        (
                        SELECT 
                            CAST(sha2(CAST(A.order_id AS STRING), 256) AS BINARY) AS hk_ordenes
                            ,A.item_id AS id_linea_detalle
                            ,A.product_id AS id_producto 
                            ,A.quantity AS cantidad 
                            ,A.list_price AS precio_lista 
                            ,A.discount AS descuento 
                            ,B.order_date
                            ,CAST(
                            sha2((CAST(A.order_id AS STRING) || 
                            CAST(A.item_id AS STRING) || 
                            CAST(A.product_id AS STRING) || 
                            CAST(A.quantity AS STRING) || 
                            CAST(A.list_price AS STRING) || 
                            CAST(A.discount AS STRING) || 
                            CAST(B.order_date AS STRING)), 512)
                            AS BINARY) AS hk_diff
                        FROM 
                            lineas_ordenes_bronce AS A 
                            INNER JOIN 
                            (
                            SELECT 
                            order_id 
                            ,MAX(order_date) AS order_date
                            FROM 
                            ordenes_bronce
                            WHERE 
                            order_date >= '{fechaInicio}'
                            AND order_date <= '{fechaFin}'
                            GROUP BY 
                            order_id 
                            ) AS B 
                            ON A.order_id = B.order_id
                        ) AS C 
                        LEFT JOIN sat_ordenes_detalle AS D 
                            ON C.hk_ordenes = D.hk_ordenes AND C.hk_diff = D.hk_diff
                        WHERE 
                        D.hk_ordenes IS NULL 
                        AND D.hk_diff IS NULL 
                      """)

resultado.show()
