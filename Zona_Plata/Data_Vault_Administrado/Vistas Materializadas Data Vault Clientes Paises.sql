-- Databricks notebook source
USE DataVault.cri;

-- COMMAND ----------

CREATE OR REPLACE MATERIALIZED VIEW Vm_Hub_Cli_Clientes 
AS
SELECT  
    Hk_Clientes
    ,Bk_IdCliente
    ,FechaRegistro
    ,NombreFuente
FROM 
    DataVault.reg.hub_cli_clientes
WHERE 
    Bk_IdCliente >= 1
    AND Bk_IdCliente <= 600    
;

-- COMMAND ----------

USE DataVault.pan;

-- COMMAND ----------

CREATE OR REPLACE MATERIALIZED VIEW Vm_Hub_Cli_Clientes 
AS
SELECT  
    Hk_Clientes
    ,Bk_IdCliente
    ,FechaRegistro
    ,NombreFuente
FROM 
    DataVault.reg.hub_cli_clientes
WHERE 
    Bk_IdCliente >= 601
    AND Bk_IdCliente <= 900 
;
