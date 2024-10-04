# Databricks notebook source
from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql.types import *
from delta import * 
from datetime import *

dbutils.widgets.text("Medalla", "", "Medalla")
dbutils.widgets.text("Catalogo", "", "Nombre Catalogo")

medalla = dbutils.widgets.get("Medalla")
catalogo = dbutils.widgets.get("Catalogo")

# print(medalla, catalogo)

existeCatalogo = spark.sql("SHOW CATALOGS").filter(col("catalog") == catalogo).count()

if existeCatalogo != 0 and medalla != "":

    dfEsquemasEtiquetar = spark.sql(
        f"""
        SELECT 
            A.schema_name
            ,ROW_NUMBER()OVER(ORDER BY A.schema_name ASC) iterador
        FROM 
            {catalogo}.information_schema.schemata AS A
            LEFT JOIN 
            (
                SELECT 
                    catalog_name
                    ,schema_name
                FROM 
                    {catalogo}.information_schema.schema_tags 
                WHERE 
                    tag_name = 'Medalla'
            ) AS B 
                ON A.catalog_name = B.catalog_name AND A.schema_name = B.schema_name
        WHERE 
            A.schema_name NOT IN ('information_schema', 'default')
            AND B.catalog_name IS NULL 
            AND B.schema_name IS NULL 
        """
    )

    dfTablasEtiquetar = spark.sql(
        f"""
        SELECT 
            A.schema_name
            ,B.table_name
            ,ROW_NUMBER()OVER(ORDER BY A.schema_name, B.table_name ASC) iterador
        FROM 
            {catalogo}.information_schema.schemata AS A
            INNER JOIN {catalogo}.information_schema.tables AS B
                ON A.catalog_name = B.table_catalog AND A.schema_name = B.table_schema
            LEFT JOIN 
            (
                SELECT 
                    catalog_name
                    ,schema_name
                FROM 
                    {catalogo}.information_schema.table_tags 
                WHERE 
                    tag_name = 'Medalla'
            ) AS C 
                ON A.catalog_name = C.catalog_name AND A.schema_name = C.schema_name
        WHERE 
            A.schema_name NOT IN ('information_schema', 'default')
            AND C.catalog_name IS NULL 
            AND C.schema_name IS NULL 
        """
    )

    if dfEsquemasEtiquetar.count() > 0:

        maxIterador = dfEsquemasEtiquetar.agg(max(col("iterador")).alias("maxIterador")).first()["maxIterador"]
        for i in range(maxIterador):
            nombreEsquema = dfEsquemasEtiquetar.filter(col("iterador") == (i+1)).first()["schema_name"]
            spark.sql(
                f""" 
                ALTER SCHEMA {catalogo}.{nombreEsquema} SET TAGS ('Medalla' = '{medalla}')
                """)

    if dfTablasEtiquetar.count() > 0: 
        maxIterador = dfTablasEtiquetar.agg(max(col("iterador")).alias("maxIterador")).first()["maxIterador"]
        for i in range(maxIterador):
            nombreEsquema = dfTablasEtiquetar.filter(col("iterador") == (i+1)).first()["schema_name"]
            nombreTabla = dfTablasEtiquetar.filter(col("iterador") == (i+1)).first()["table_name"]
            spark.sql(
                f"""
                ALTER TABLE {catalogo}.{nombreEsquema}.{nombreTabla} SET TAGS ('Medalla' = '{medalla}')
                """
            )

else:
    print("No se puede procesar el catalogo o la medalla")

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT 
# MAGIC     A.schema_name
# MAGIC     ,B.table_name
# MAGIC     ,ROW_NUMBER()OVER(ORDER BY A.schema_name, B.table_name ASC) iterador
# MAGIC FROM 
# MAGIC     data_vault.information_schema.schemata AS A
# MAGIC     INNER JOIN data_vault.information_schema.tables AS B
# MAGIC         ON A.catalog_name = B.table_catalog AND A.schema_name = B.table_schema
# MAGIC     LEFT JOIN 
# MAGIC     (
# MAGIC         SELECT 
# MAGIC             catalog_name
# MAGIC             ,schema_name
# MAGIC         FROM 
# MAGIC             data_vault.information_schema.table_tags 
# MAGIC         WHERE 
# MAGIC             tag_name = 'Medalla'
# MAGIC     ) AS C 
# MAGIC         ON A.catalog_name = C.catalog_name AND A.schema_name = C.schema_name
# MAGIC WHERE 
# MAGIC     A.schema_name NOT IN ('information_schema', 'default')
# MAGIC     AND C.catalog_name IS NULL 
# MAGIC     AND C.schema_name IS NULL 
# MAGIC
# MAGIC
# MAGIC
# MAGIC
# MAGIC  

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM data_vault.information_schema.table_tags
