-- Databricks notebook source
show tables;

-- COMMAND ----------

show tables in global_temp

-- COMMAND ----------

describe detail smartphones

-- COMMAND ----------

-- MAGIC %fs ls dbfs:/user/hive/warehouse/smartphones/_delta_log/

-- COMMAND ----------

select * from text.`dbfs:/user/hive/warehouse/smartphones/_delta_log/00000000000000000000.json`;

-- COMMAND ----------

