# Databricks notebook source
# Inicializamos las librerías a Utilizar
from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql.types import *
from delta import *
from datetime import * 

# Desplegamos la sesión de spark para el trabajo distribuido de los nodos del cluster
spark = SparkSession.builder.appName("PipelineDimProducto").config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension").config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog").config("fs.azure", "fs.azure.NativeAzureFileSystem").getOrCreate()

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
          CREATE TABLE IF NOT EXISTS dim_producto (
              dim_producto_id INT NOT NULL,
              llave_negocio_producto INT NOT NULL,
              nombre STRING NOT NULL,
              categoria STRING NOT NULL,
              marca STRING NOT NULL,
              modelo INT NOT NULL,
              precio_unitario DOUBLE NOT NULL,
              fecha_registro TIMESTAMP NOT NULL
          )
          USING DELTA
          LOCATION 'abfss://{contenedorDatalake}@{cuentaDatalake}.dfs.core.windows.net/Gold/Deltas/DWH/common/dim_producto'
          """)

# Habilitamos el AutoOptimizer en la tabla delta para aumentar la capacidad y el rendimiento
spark.sql("""
            ALTER TABLE dim_producto 
            SET TBLPROPERTIES (
                delta.autoOptimize.optimizeWrite = true,
                delta.autoOptimize.autoCompact = true
            );
          """)

# COMMAND ----------



# Procedemos a generar la lógica respectiva para tranformar y cargar los datos de una dimension tipo 1

# agregamos el registro dummy validando si el mismo existe o no existe
existeDummy = spark.sql("SELECT COUNT(*) AS existe FROM dim_producto WHERE dim_producto_id = -1").collect()[0]["existe"]

if existeDummy == 0 :
    spark.sql("""
              INSERT INTO dim_producto (dim_producto_id, llave_negocio_producto, nombre, categoria, marca, modelo, precio_unitario, fecha_registro)
              VALUES (-1, -1, 'No Definido', 'No Definido', 'No Definido', -1, 0.00, current_timestamp())
              """)

# obtenemos el id maximo de la dimension
idMaximo = spark.sql("SELECT CASE WHEN MAX(dim_producto_id) IS NULL THEN 0 ELSE MAX(dim_producto_id) END AS dim_producto_id FROM dim_producto WHERE dim_producto_id >= 1").collect()[0]["dim_producto_id"]

# Obtener los Existentes con cambios 

dfDatosNuevosExistentes = spark.sql("""
                                    SELECT 
                                        D.dim_producto_id 
                                        ,D.llave_negocio_producto
                                        ,C.nombre
                                        ,C.categoria
                                        ,C.marca
                                        ,C.modelo
                                        ,C.precio_unitario
                                        ,current_timestamp() AS fecha_registro
                                    FROM 
                                    (
                                    SELECT 
                                        A.bk_id_producto
                                        ,B.nombre
                                        ,B.categoria
                                        ,B.marca
                                        ,B.modelo
                                        ,B.precio_unitario
                                    FROM 
                                        data_vault.common.hub_productos AS A 
                                        INNER JOIN 
                                        (
                                        SELECT 
                                            B2.hk_productos
                                            ,B2.nombre
                                            ,B2.categoria
                                            ,B2.marca
                                            ,B2.modelo
                                            ,B2.precio_unitario
                                        FROM 
                                        (
                                            SELECT 
                                            hk_productos
                                            ,nombre
                                            ,categoria
                                            ,marca
                                            ,modelo
                                            ,precio_unitario
                                            ,ROW_NUMBER()OVER(PARTITION BY hk_productos ORDER BY anio_particion DESC, mes_particion DESC, fecha_registro DESC) AS desduplicador
                                            FROM 
                                            data_vault.common.sat_productos 
                                        ) AS B2
                                        WHERE 
                                            B2.desduplicador = 1
                                        ) AS B
                                        ON A.hk_productos = B.hk_productos
                                    ) AS C 
                                    INNER JOIN dwh.common.dim_producto AS D 
                                        ON C.bk_id_producto = D.llave_negocio_producto
                                    WHERE
                                        (C.nombre != D.nombre)
                                        OR (C.categoria != D.categoria)
                                        OR (C.marca != D.marca)
                                        OR (C.modelo != D.modelo)
                                        OR (C.precio_unitario != D.precio_unitario)
                                    """)

# Obtener los nuevos 
dfTempNuevos = spark.sql(f"""
                         SELECT 
                            (ROW_NUMBER()OVER(ORDER BY C.bk_id_producto) + {str(idMaximo)}) AS dim_producto_id
                            ,C.bk_id_producto AS llave_negocio_producto
                            ,C.nombre
                            ,C.categoria
                            ,C.marca
                            ,C.modelo
                            ,C.precio_unitario
                            ,current_timestamp() AS fecha_registro
                        FROM 
                        (
                        SELECT 
                            A.bk_id_producto
                            ,B.nombre
                            ,B.categoria
                            ,B.marca
                            ,B.modelo
                            ,B.precio_unitario
                        FROM 
                            data_vault.common.hub_productos AS A 
                            INNER JOIN 
                            (
                            SELECT 
                                B2.hk_productos
                                ,B2.nombre
                                ,B2.categoria
                                ,B2.marca
                                ,B2.modelo
                                ,B2.precio_unitario
                            FROM 
                            (
                                SELECT 
                                hk_productos
                                ,nombre
                                ,categoria
                                ,marca
                                ,modelo
                                ,precio_unitario
                                ,ROW_NUMBER()OVER(PARTITION BY hk_productos ORDER BY anio_particion DESC, mes_particion DESC, fecha_registro DESC) AS desduplicador
                                FROM 
                                data_vault.common.sat_productos 
                            ) AS B2
                            WHERE 
                                B2.desduplicador = 1
                            ) AS B
                            ON A.hk_productos = B.hk_productos
                        ) AS C 
                        LEFT JOIN dwh.common.dim_producto AS D 
                            ON C.bk_id_producto = D.llave_negocio_producto
                        WHERE
                            D.llave_negocio_producto IS NULL
                         """)

dfDatosNuevosExistentes = dfDatosNuevosExistentes.unionAll(dfTempNuevos)

# Escribimos tanto los datos nuevos como los existentes con cambios usando pyspark con la función merge que es recomendada para estos casos

if dfDatosNuevosExistentes.isEmpty() == False : 
    dfDimCliente = DeltaTable.forName(spark, "dwh.common.dim_producto")
    dfDimCliente.alias("dim").merge(
        dfDatosNuevosExistentes.alias("plata"),
        "dim.llave_negocio_producto = plata.llave_negocio_producto"
    ).whenMatchedUpdateAll().whenNotMatchedInsertAll().execute()




# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC SELECT 
# MAGIC   *
# MAGIC FROM 
# MAGIC   dwh.common.dim_producto;
# MAGIC
