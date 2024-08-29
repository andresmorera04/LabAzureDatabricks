# Databricks notebook source
# Inicializamos las librerías a Utilizar
from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql.types import *
from delta import *
from datetime import * 

# Desplegamos la sesión de spark para el trabajo distribuido de los nodos del cluster
spark = SparkSession.builder.appName("PipelineHecOrdenesVenta").config("spark.sql.extensions", "io.delta.sql.DeltasAdministradasparkSessionExtension").config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog").config("fs.azure", "fs.azure.NativeAzureFileSystem").getOrCreate()

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
spark.sql(f"""CREATE DATABASE IF NOT EXISTS dwh.cri
          MANAGED LOCATION 'abfss://{contenedorDatalake}@{cuentaDatalake}.dfs.core.windows.net/Gold/DeltasAdministradas/DWH/cri'""")

existeTabla = spark.sql("""SELECT COUNT(*) AS existe FROM dwh.information_schema.tables WHERE table_name = 'hec_ordenes_venta_resumen' AND table_schema = 'cri'""").first()["existe"]

if existeTabla == 0:
    # Creamos la tabla delta de hub_productos de tipo tabla no adminisrada o externa
    spark.sql("USE dwh.cri")
    spark.sql(f"""
          CREATE TABLE IF NOT EXISTS hec_ordenes_venta_resumen (
              dim_tiempo_id INT NOT NULL,
              dim_cliente_id INT NOT NULL,
              id_orden INT NOT NULL,
              estado_orden STRING NOT NULL,
              fecha_orden TIMESTAMP NOT NULL,
              fecha_requerida DATE NOT NULL,
              fecha_entrega DATE NOT NULL,
              total DOUBLE NOT NULL,
              fecha_registro TIMESTAMP NOT NULL
          )
          USING DELTA
          CLUSTER BY (dim_tiempo_id)
          """)
    
    # Habilitamos el AutoOptimizer en la tabla delta para aumentar la capacidad y el rendimiento
    spark.sql("""
            ALTER TABLE hec_ordenes_venta_resumen 
            SET TBLPROPERTIES (
                delta.autoOptimize.optimizeWrite = true,
                delta.autoOptimize.autoCompact = true
            );
          """)

# COMMAND ----------

# Eliminamos los datos de esas fechas para evitar duplicidad
resultado = spark.sql(f"""
                      DELETE FROM 
                        hec_ordenes_venta_resumen 
                      WHERE 
                        dim_tiempo_id >= {str(fechaInicio)[0:10].replace("-", "")}
                        AND dim_tiempo_id <= {str(fechaFin)[0:10].replace("-", "")}
                      """)

resultado.show()

# Agregamos los datos en la tabla de Hechos 
resultado = spark.sql(f"""
                      INSERT INTO hec_ordenes_venta_resumen (dim_tiempo_id, dim_cliente_id, id_orden, estado_orden, fecha_orden, fecha_requerida, fecha_entrega, total, fecha_registro) 
                      SELECT 
                        plata.dim_tiempo_id
                        ,CASE WHEN cli.llave_negocio_cliente IS NULL THEN -1 ELSE cli.dim_cliente_id END AS dim_cliente_id 
                        ,plata.bk_codigo_orden AS id_orden 
                        ,plata.estado_orden
                        ,plata.fecha_orden
                        ,plata.fecha_requerida
                        ,plata.fecha_entrega
                        ,plata.total
                        ,current_timestamp() AS fecha_registro 
                    FROM 
                        (
                        SELECT 
                            CAST(REPLACE(LEFT(B.fecha_orden, 10), '-', '') AS INT) AS dim_tiempo_id 
                            ,D.bk_id_cliente
                            ,A.bk_codigo_orden 
                            ,B.estado_orden
                            ,B.fecha_orden
                            ,B.fecha_requerida
                            ,B.fecha_entrega
                            ,B.total
                        FROM 
                            data_vault.cri.hub_ordenes AS A 
                            INNER JOIN 
                            (
                                SELECT 
                                    B2.hk_ordenes
                                    ,B2.estado_orden
                                    ,B2.fecha_orden
                                    ,B2.fecha_requerida
                                    ,B2.fecha_entrega
                                    ,B2.total
                                FROM 
                                    (
                                    SELECT 
                                        hk_ordenes
                                        ,estado_orden
                                        ,fecha_orden
                                        ,fecha_requerida
                                        ,fecha_entrega
                                        ,total
                                        ,ROW_NUMBER()OVER(PARTITION BY hk_ordenes ORDER BY fecha_registro DESC) AS desduplicador
                                    FROM 
                                        data_vault.cri.sat_ordenes_encabezado 
                                    WHERE 
                                        fecha_orden >= '{fechaInicio}'
                                        AND fecha_orden <= '{fechaFin}'
                                    ) AS B2
                                WHERE 
                                    B2.desduplicador = 1
                            ) AS B
                                ON A.hk_ordenes = B.hk_ordenes 
                            INNER JOIN 
                            (
                                SELECT 
                                    C2.hk_ordenes
                                    ,C2.hk_clientes
                                FROM 
                                    (
                                    SELECT 
                                        hk_ordenes
                                        ,hk_clientes
                                        ,ROW_NUMBER()OVER(PARTITION BY hk_ordenes ORDER BY fecha_registro DESC) AS desduplicador 
                                    FROM 
                                        data_vault.cri.lnk_clientes_ordenes
                                    ) AS C2 
                                WHERE 
                                    C2.desduplicador = 1
                            ) AS C 
                                ON A.hk_ordenes = C.hk_ordenes 
                            INNER JOIN data_vault.cri.hub_clientes AS D 
                                ON C.hk_clientes = D.hk_clientes
                        ) AS plata 
                        LEFT JOIN dwh.cri.dim_cliente AS cli 
                            ON plata.bk_id_cliente = cli.llave_negocio_cliente
                      """)

resultado.show()

# COMMAND ----------

existeTabla = spark.sql("""SELECT COUNT(*) AS existe FROM dwh.information_schema.tables WHERE table_name = 'hec_ordenes_venta_detalle' AND table_schema = 'cri'""").first()["existe"]

if existeTabla == 0:
    spark.sql(f"""
          CREATE TABLE IF NOT EXISTS hec_ordenes_venta_detalle (
              dim_tiempo_id INT NOT NULL,
              dim_cliente_id INT NOT NULL,
              dim_producto_id INT NOT NULL,
              id_orden INT NOT NULL,
              id_linea_detalle INT NOT NULL,
              estado_orden STRING NOT NULL,
              fecha_orden TIMESTAMP NOT NULL,
              fecha_requerida DATE NOT NULL,
              fecha_entrega DATE NOT NULL,
              cantidad INT NOT NULL,
              precio_lista_producto DOUBLE NOT NULL,
              descuento DOUBLE NOT NULL,
              fecha_registro TIMESTAMP NOT NULL
          )
          USING DELTA
          CLUSTER BY (dim_tiempo_id)
          """)
    
    # Habilitamos el AutoOptimizer en la tabla delta para aumentar la capacidad y el rendimiento
    spark.sql("""
            ALTER TABLE hec_ordenes_venta_detalle  
            SET TBLPROPERTIES (
                delta.autoOptimize.optimizeWrite = true,
                delta.autoOptimize.autoCompact = true
            );
          """)

# COMMAND ----------

resultado = spark.sql(f"""
                    DELETE FROM 
                        hec_ordenes_venta_detalle
                    WHERE 
                        dim_tiempo_id >= '{str(fechaInicio)[0:10].replace("-", "")}'
                        AND dim_tiempo_id <= '{str(fechaFin)[0:10].replace("-", "")}'
                      """)

resultado.show() 

resultado = spark.sql(f"""
                        INSERT INTO hec_ordenes_venta_detalle (dim_tiempo_id, dim_cliente_id, dim_producto_id, id_orden, id_linea_detalle, estado_orden, fecha_orden, fecha_requerida, fecha_entrega, cantidad, precio_lista_producto, descuento, fecha_registro) 
                        SELECT 
                        CAST(REPLACE(LEFT(B.fecha_orden, 10), '-', '') AS INT) AS dim_tiempo_id 
                        ,CASE WHEN G.llave_negocio_cliente IS NULL THEN -1 ELSE G.dim_cliente_id END AS dim_cliente_id 
                        ,CASE WHEN H.llave_negocio_producto IS NULL THEN -1 ELSE H.dim_producto_id END AS dim_producto_id
                        ,A.bk_codigo_orden AS id_orden 
                        ,C.id_linea_detalle
                        ,B.estado_orden
                        ,B.fecha_orden
                        ,B.fecha_requerida
                        ,B.fecha_entrega
                        ,C.cantidad
                        ,C.precio_lista AS precio_lista_producto 
                        ,C.descuento
                        ,current_timestamp() AS fecha_registro 
                        FROM 
                        data_vault.cri.hub_ordenes AS A 
                        INNER JOIN 
                        (
                            SELECT 
                                B2.hk_ordenes
                                ,B2.estado_orden
                                ,B2.fecha_orden
                                ,B2.fecha_requerida
                                ,B2.fecha_entrega
                                ,B2.total
                            FROM 
                                (
                                SELECT 
                                    hk_ordenes
                                    ,estado_orden
                                    ,fecha_orden
                                    ,fecha_requerida
                                    ,fecha_entrega
                                    ,total
                                    ,ROW_NUMBER()OVER(PARTITION BY hk_ordenes ORDER BY fecha_registro DESC) AS desduplicador
                                FROM 
                                    data_vault.cri.sat_ordenes_encabezado 
                                WHERE 
                                    fecha_orden >= '{fechaInicio}'
                                    AND fecha_orden <= '{fechaFin}'
                                ) AS B2
                            WHERE 
                                B2.desduplicador = 1
                        ) AS B
                            ON A.hk_ordenes = B.hk_ordenes
                        INNER JOIN 
                        (
                            SELECT 
                            C2.hk_ordenes
                            ,C2.id_linea_detalle
                            ,C2.id_producto
                            ,C2.cantidad
                            ,C2.precio_lista
                            ,C2.descuento
                            FROM 
                            (
                            SELECT 
                                hk_ordenes
                                ,id_linea_detalle
                                ,id_producto
                                ,cantidad
                                ,precio_lista
                                ,descuento
                                ,ROW_NUMBER()OVER(PARTITION BY hk_ordenes, id_linea_detalle ORDER BY fecha_registro DESC) AS desduplicador
                            FROM 
                                data_vault.cri.sat_ordenes_detalle
                            ) AS C2 
                            WHERE 
                            C2.desduplicador = 1
                        ) AS C
                            ON A.hk_ordenes = C.hk_ordenes 
                        INNER JOIN 
                        (
                            SELECT 
                                D2.hk_ordenes
                                ,D2.hk_clientes
                            FROM 
                                (
                                SELECT 
                                    hk_ordenes
                                    ,hk_clientes
                                    ,ROW_NUMBER()OVER(PARTITION BY hk_ordenes ORDER BY fecha_registro DESC) AS desduplicador 
                                FROM 
                                    data_vault.cri.lnk_clientes_ordenes
                                ) AS D2 
                            WHERE 
                                D2.desduplicador = 1 
                        ) AS D 
                            ON A.hk_ordenes = D.hk_ordenes 
                        INNER JOIN data_vault.cri.hub_clientes AS E
                            ON D.hk_clientes = E.hk_clientes 
                        INNER JOIN data_vault.cri.hub_productos AS F 
                            ON C.id_producto = F.bk_id_producto
                        LEFT JOIN dwh.cri.dim_cliente AS G 
                            ON E.bk_id_cliente = G.llave_negocio_cliente 
                        LEFT JOIN dwh.cri.dim_producto AS H 
                            ON F.bk_id_producto = H.llave_negocio_producto
                      """)

resultado.show()



# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC SELECT 
# MAGIC   *
# MAGIC FROM 
# MAGIC   dwh.cri.hec_ordenes_venta_detalle;
