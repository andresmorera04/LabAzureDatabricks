# Databricks notebook source
# MAGIC %sql
# MAGIC USE data_vault.common;
# MAGIC
# MAGIC
# MAGIC CREATE OR REPLACE FUNCTION Ufn_EnmascararTarjetaCredito (ValorTarjeta STRING)
# MAGIC RETURNS STRING 
# MAGIC RETURN 
# MAGIC   CASE
# MAGIC     WHEN is_account_group_member('IngenieriaDatos') THEN ValorTarjeta 
# MAGIC     ELSE ('**** **** **** ' || RIGHT(ValorTarjeta, 4))
# MAGIC   END 
# MAGIC
# MAGIC

# COMMAND ----------

# MAGIC %sql
# MAGIC USE data_vault.common;
# MAGIC
# MAGIC
# MAGIC CREATE OR REPLACE FUNCTION Ufn_EnmascararCorreoElectronico (ValorCorreo STRING)
# MAGIC RETURNS STRING 
# MAGIC RETURN 
# MAGIC   CASE
# MAGIC     WHEN is_account_group_member('IngenieriaDatos') THEN ValorCorreo 
# MAGIC     ELSE '***************@*****'
# MAGIC   END 
