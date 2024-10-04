# Databricks notebook source
# Inicializamos las librerías a Utilizar
from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql.types import *
from delta import *
from datetime import * 

spark.sql("USE dwh.cri")

spark.sql("""
          CREATE MATERIALIZED VIEW vw_DimClienteCri 
          COMMENT 'Producto de Datos: Customer360. Tipo: 1. Descripción: Vista Materializada con datos de los Clientes de Costa Rica'
          AS 
            SELECT 
              dim_cliente_id, 
              llave_negocio_cliente, 
              nombre, 
              apellido,
              codigo_pais_telefono, 
              numero_telefono, 
              correo_electronico,
              calle,
              ciudad,
              estado,
              codigo_zip,
              fecha_registro
            FROM 
              dwh.common.dim_cliente
          """)


spark.sql("REFRESH MATERIALIZED VIEW vw_DimClienteCri;")

