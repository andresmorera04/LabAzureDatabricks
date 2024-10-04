# Databricks notebook source
# Inicializamos las librerías a Utilizar
from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql.types import *
from delta import *
from datetime import * 

# Desplegamos la sesión de spark para el trabajo distribuido de los nodos del cluster
spark = SparkSession.builder.appName("PipelineDimCliente").config("spark.sql.extensions", "io.delta.sql.DeltasAdministradasparkSessionExtension").config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog").config("fs.azure", "fs.azure.NativeAzureFileSystem").getOrCreate()

# Nos conectamos a la base de datos que tiene la tabla de parametros


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

existeTabla = spark.sql("""SELECT COUNT(*) AS existe FROM dwh.information_schema.tables WHERE table_name = 'dim_cliente' AND table_schema = 'cri'""").first()["existe"]

if existeTabla == 0:
    # Creamos la tabla delta de hub_productos de tipo tabla no adminisrada o externa
    spark.sql("USE dwh.cri")
    spark.sql(f"""
          CREATE TABLE IF NOT EXISTS dim_cliente (
              dim_cliente_id INT NOT NULL,
              llave_negocio_cliente INT NOT NULL,
              nombre STRING NOT NULL,
              apellido STRING NOT NULL,
              codigo_pais_telefono STRING NOT NULL,
              numero_telefono INT NOT NULL,
              correo_electronico STRING NOT NULL,
              calle STRING NOT NULL,
              ciudad STRING NOT NULL,
              estado STRING NOT NULL,
              codigo_zip STRING NOT NULL,
              fecha_registro TIMESTAMP NOT NULL
          )
          USING DELTA
          CLUSTER BY (dim_cliente_id) 
          """)
    
    # Habilitamos el AutoOptimizer en la tabla delta para aumentar la capacidad y el rendimiento
    spark.sql("""
            ALTER TABLE dim_cliente 
            SET TBLPROPERTIES (
                delta.autoOptimize.optimizeWrite = true,
                delta.autoOptimize.autoCompact = true
            );
          """)

# COMMAND ----------

# Procedemos a generar la lógica respectiva para tranformar y cargar los datos de una dimension tipo 1

# agregamos el registro dummy validando si el mismo existe o no existe
existeDummy = spark.sql("SELECT COUNT(*) AS existe FROM dim_cliente WHERE dim_cliente_id = -1").collect()[0]["existe"]

if existeDummy == 0 :
    spark.sql("""
              INSERT INTO dim_cliente (dim_cliente_id, llave_negocio_cliente, nombre, apellido, codigo_pais_telefono, numero_telefono, correo_electronico, calle, ciudad, estado, codigo_zip, fecha_registro)
              VALUES (-1, -1, 'No Definido', 'No Definido', '(N/D)', -1, 'No Definido', 'No Definido', 'No Definido', 'No Definido', 'No Definido', current_timestamp())
              """)

# obtenemos el id maximo de la dimension
idMaximo = spark.sql("SELECT CASE WHEN MAX(dim_cliente_id) IS NULL THEN 0 ELSE MAX(dim_cliente_id) END AS dim_cliente_id FROM dim_cliente WHERE dim_cliente_id >= 1").collect()[0]["dim_cliente_id"]

# Obtener los Existentes con cambios 

dfDatosNuevosExistentes = spark.sql("""
                                    SELECT 
                                        dim.dim_cliente_id 
                                        ,dim.llave_negocio_cliente AS llave_negocio_cliente 
                                        ,CASE WHEN plata.nombre_cliente IS NULL THEN dim.nombre ELSE plata.nombre_cliente END AS nombre
                                        ,CASE WHEN plata.apellido_cliente IS NULL THEN dim.apellido ELSE plata.apellido_cliente END AS apellido
                                        ,CASE WHEN plata.codigo_pais IS NULL THEN dim.codigo_pais_telefono ELSE plata.codigo_pais END AS codigo_pais_telefono
                                        ,CASE WHEN plata.numero_telefono IS NULL THEN dim.numero_telefono ELSE plata.numero_telefono END AS numero_telefono 
                                        ,CASE WHEN plata.correo_electronico IS NULL THEN dim.correo_electronico ELSE plata.correo_electronico END AS correo_electronico 
                                        ,CASE WHEN plata.calle IS NULL THEN dim.calle ELSE plata.calle END AS calle
                                        ,CASE WHEN plata.ciudad IS NULL THEN dim.ciudad ELSE plata.ciudad END AS ciudad
                                        ,CASE WHEN plata.estado IS NULL THEN dim.estado ELSE plata.estado END AS estado 
                                        ,CASE WHEN plata.codigo_zip IS NULL THEN dim.codigo_zip ELSE plata.codigo_zip END AS codigo_zip 
                                        ,current_timestamp() AS fecha_registro 
                                        FROM 
                                        (
                                        SELECT 
                                            A.bk_id_cliente
                                            ,B.nombre_cliente
                                            ,B.apellido_cliente
                                            ,D.codigo_pais
                                            ,D.numero_telefono
                                            ,C.correo_electronico
                                            ,B.calle
                                            ,B.ciudad
                                            ,B.estado
                                            ,B.codigo_zip
                                        FROM 
                                            data_vault.cri.hub_clientes AS A 
                                            INNER JOIN 
                                            (
                                            SELECT 
                                                B2.hk_clientes
                                                ,B2.nombre_cliente 
                                                ,B2.apellido_cliente
                                                ,B2.calle
                                                ,B2.ciudad
                                                ,B2.estado
                                                ,B2.codigo_zip
                                            FROM 
                                                (
                                                SELECT 
                                                    hk_clientes
                                                    ,nombre_cliente 
                                                    ,apellido_cliente
                                                    ,calle
                                                    ,ciudad
                                                    ,estado
                                                    ,codigo_zip
                                                    ,ROW_NUMBER()OVER(PARTITION BY hk_clientes ORDER BY fecha_registro DESC) AS desduplicador
                                                FROM 
                                                    data_vault.cri.sat_clientes 
                                                ) AS B2
                                            WHERE 
                                                B2.desduplicador = 1
                                            ) AS B 
                                            ON A.hk_clientes = B.hk_clientes
                                            INNER JOIN 
                                            (
                                            SELECT 
                                                C2.hk_clientes
                                                ,C2.correo_electronico
                                            FROM 
                                                (
                                                SELECT 
                                                hk_clientes
                                                ,correo_electronico
                                                ,ROW_NUMBER()OVER(PARTITION BY hk_clientes ORDER BY fecha_registro DESC) AS desduplicador
                                                FROM 
                                                data_vault.cri.sat_clientes_correos 
                                                ) AS C2
                                            WHERE 
                                                C2.desduplicador = 1
                                            ) AS C 
                                            ON A.hk_clientes = C.hk_clientes
                                            INNER JOIN 
                                            (
                                            SELECT 
                                                D2.hk_clientes
                                                ,D2.codigo_pais
                                                ,D2.numero_telefono
                                            FROM 
                                                (
                                                SELECT 
                                                hk_clientes
                                                ,CASE 
                                                    WHEN codigo_pais = '' THEN '(N/D)'
                                                    ELSE codigo_pais
                                                END codigo_pais
                                                ,numero_telefono
                                                ,ROW_NUMBER()OVER(PARTITION BY hk_clientes ORDER BY fecha_registro DESC) AS desduplicador
                                                FROM 
                                                data_vault.cri.sat_clientes_telefonos 
                                                ) AS D2 
                                            WHERE 
                                                D2.desduplicador = 1
                                            ) AS D 
                                            ON A.hk_clientes = D.hk_clientes
                                        ) AS plata 
                                        INNER JOIN dwh.cri.dim_cliente AS dim 
                                            ON plata.bk_id_cliente = dim.llave_negocio_cliente
                                        WHERE 
                                        (dim.nombre != plata.nombre_cliente)
                                        OR (dim.apellido != plata.apellido_cliente)
                                        OR (dim.codigo_pais_telefono != plata.codigo_pais)
                                        OR (dim.numero_telefono != plata.numero_telefono)
                                        OR (dim.correo_electronico != plata.correo_electronico)
                                        OR (dim.calle != plata.calle)
                                        OR (dim.ciudad != plata.ciudad)
                                        OR (dim.estado != plata.estado)
                                        OR (dim.codigo_zip != plata.codigo_zip)
                                    """)

# Obtener los nuevos 
dfTempNuevos = spark.sql(f"""
                         SELECT 
                            (ROW_NUMBER()OVER(ORDER BY plata.bk_id_cliente ASC) + {str(idMaximo)} ) AS dim_cliente_id 
                            ,plata.bk_id_cliente AS llave_negocio_cliente 
                            ,plata.nombre_cliente AS nombre
                            ,plata.apellido_cliente AS apellido
                            ,plata.codigo_pais AS codigo_pais_telefono
                            ,plata.numero_telefono AS numero_telefono 
                            ,plata.correo_electronico AS correo_electronico 
                            ,plata.calle AS calle
                            ,plata.ciudad AS ciudad
                            ,plata.estado AS estado 
                            ,plata.codigo_zip AS codigo_zip 
                            ,current_timestamp() AS fecha_registro 
                            FROM 
                            (
                            SELECT 
                                A.bk_id_cliente
                                ,B.nombre_cliente
                                ,B.apellido_cliente
                                ,D.codigo_pais
                                ,D.numero_telefono
                                ,C.correo_electronico
                                ,B.calle
                                ,B.ciudad
                                ,B.estado
                                ,B.codigo_zip
                            FROM 
                                data_vault.cri.hub_clientes AS A 
                                INNER JOIN 
                                (
                                SELECT 
                                    B2.hk_clientes
                                    ,B2.nombre_cliente 
                                    ,B2.apellido_cliente
                                    ,B2.calle
                                    ,B2.ciudad
                                    ,B2.estado
                                    ,B2.codigo_zip
                                FROM 
                                    (
                                    SELECT 
                                        hk_clientes
                                        ,nombre_cliente 
                                        ,apellido_cliente
                                        ,calle
                                        ,ciudad
                                        ,estado
                                        ,codigo_zip
                                        ,ROW_NUMBER()OVER(PARTITION BY hk_clientes ORDER BY fecha_registro DESC) AS desduplicador
                                    FROM 
                                        data_vault.cri.sat_clientes 
                                    ) AS B2
                                WHERE 
                                    B2.desduplicador = 1
                                ) AS B 
                                ON A.hk_clientes = B.hk_clientes
                                INNER JOIN 
                                (
                                SELECT 
                                    C2.hk_clientes
                                    ,C2.correo_electronico
                                FROM 
                                    (
                                    SELECT 
                                    hk_clientes
                                    ,correo_electronico
                                    ,ROW_NUMBER()OVER(PARTITION BY hk_clientes ORDER BY fecha_registro DESC) AS desduplicador
                                    FROM 
                                    data_vault.cri.sat_clientes_correos 
                                    ) AS C2
                                WHERE 
                                    C2.desduplicador = 1
                                ) AS C 
                                ON A.hk_clientes = C.hk_clientes
                                INNER JOIN 
                                (
                                SELECT 
                                    D2.hk_clientes
                                    ,D2.codigo_pais
                                    ,D2.numero_telefono
                                FROM 
                                    (
                                    SELECT 
                                    hk_clientes
                                    ,CASE 
                                        WHEN codigo_pais = '' THEN '(N/D)'
                                        ELSE codigo_pais
                                    END codigo_pais
                                    ,numero_telefono
                                    ,ROW_NUMBER()OVER(PARTITION BY hk_clientes ORDER BY fecha_registro DESC) AS desduplicador
                                    FROM 
                                    data_vault.cri.sat_clientes_telefonos 
                                    ) AS D2 
                                WHERE 
                                    D2.desduplicador = 1
                                ) AS D 
                                ON A.hk_clientes = D.hk_clientes
                            ) AS plata 
                            LEFT JOIN dwh.cri.dim_cliente AS dim 
                                ON plata.bk_id_cliente = dim.llave_negocio_cliente
                            WHERE 
                            dim.llave_negocio_cliente IS NULL
                         """)

dfDatosNuevosExistentes = dfDatosNuevosExistentes.unionAll(dfTempNuevos)

# Escribimos tanto los datos nuevos como los existentes con cambios usando pyspark con la función merge que es recomendada para estos casos

if dfDatosNuevosExistentes.isEmpty() == False : 
    dfDimCliente = DeltaTable.forName(spark, "dwh.cri.dim_cliente")
    dfDimCliente.alias("dim").merge(
        dfDatosNuevosExistentes.alias("plata"),
        "dim.llave_negocio_cliente = plata.llave_negocio_cliente"
        ,
    ).whenMatchedUpdateAll().whenNotMatchedInsertAll().execute()



# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM dwh.cri.dim_cliente;
