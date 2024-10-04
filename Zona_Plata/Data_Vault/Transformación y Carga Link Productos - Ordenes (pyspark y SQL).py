# Databricks notebook source
# MAGIC %md
# MAGIC # Notebook Link Ordenes y Productos
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
spark = SparkSession.builder.appName("PipelineLinkProductosOrdenes").config("spark.sql.extensions", "io.delta.sql.DeltasAdministradasparkSessionExtension").config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog").config("fs.azure", "fs.azure.NativeAzureFileSystem").getOrCreate()

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

existe = spark.sql("""SELECT COUNT(*) AS existe FROM data_vault.information_schema.tables WHERE table_name = 'lnk_productos_ordenes' AND table_schema = 'cri'""").first()["existe"]

if existe == 0: 
    # Creamos la tabla delta de hub_productos de tipo tabla no adminisrada o externa
    spark.sql("USE data_vault.cri")
    spark.sql(f"""
          CREATE TABLE IF NOT EXISTS lnk_productos_ordenes (
              hk_productos_ordenes BINARY,
              hk_productos BINARY,
              hk_ordenes BINARY,
              fecha_registro DATE,
              nombre_fuente STRING
          )
          USING DELTA
          CLUSTER BY (fecha_registro)
          """)
    
    # Habilitamos el AutoOptimizer en la tabla delta para aumentar la capacidad y el rendimiento
    spark.sql("""
            ALTER TABLE lnk_productos_ordenes
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
        print(f"Ocurrio un error: posiblemente el parquet {rutaBronceParquets} no existe")

    fechaTemp = date(int(iterador[0:4]), int(iterador[4:6]), int(iterador[6:8]))
    iterador = str(fechaTemp + timedelta(days=1))[0:10].replace("-", "")

# dfParquetsBronce = spark.read.format("parquet").load(archivoParquetBronce)

dfParquetsBronceOrdenes.createOrReplaceTempView("ordenes_bronce")
dfParquetsBronceLineas.createOrReplaceTempView("lineas_ordenes_bronce")

# COMMAND ----------

# MAGIC %md
# MAGIC Utilizando SQL, realizamos la lógica para transformar los datos y crear el Link de Clientes y Ordenes

# COMMAND ----------

resultado = spark.sql(f"""
                        INSERT INTO lnk_productos_ordenes (hk_productos, hk_ordenes, hk_productos_ordenes, fecha_registro, nombre_fuente )
                        SELECT 
                        D.hk_productos 
                        ,E.hk_ordenes
                        ,CAST(
                        sha2((CAST(C.order_id AS STRING) || 
                        CAST(C.product_id AS STRING)), 256)
                        AS BINARY) AS hk_productos_ordenes 
                        ,current_timestamp() AS fecha_registro
                        ,'Bronze/devvertixddnsnet/bikestores/sales/[orders, order_items]' AS nombre_fuente
                        ,YEAR(current_timestamp()) AS anio_particion
                        ,MONTH(current_timestamp()) AS mes_particion 
                        FROM 
                        (
                        SELECT 
                            A.order_id 
                            ,A.product_id 
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
                        GROUP BY 
                            A.order_id 
                            ,A.product_id
                        ) AS C 
                        INNER JOIN hub_productos AS D 
                            ON C.product_id = D.bk_id_producto
                        INNER JOIN hub_ordenes AS E 
                            ON C.order_id = E.bk_codigo_orden 
                        LEFT JOIN lnk_productos_ordenes AS F 
                            ON D.hk_productos = F.hk_productos AND E.hk_ordenes = F.hk_ordenes
                        WHERE 
                        F.hk_productos IS NULL 
                        AND F.hk_ordenes IS NULL 
                      """)

resultado.show()
