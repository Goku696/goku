-- Databricks notebook source
DESCRIBE HISTORY employee


-- COMMAND ----------

select * from employee


-- COMMAND ----------

select * from employee @v1

-- COMMAND ----------

select * from employee timestamp as of '2023-04-07T11:32:39.000+0000'

-- COMMAND ----------

DELETE from employee

-- COMMAND ----------

SELECT * FROM employee

-- COMMAND ----------

RESTORE table employee to version as of 4

-- COMMAND ----------

select * from employee

-- COMMAND ----------

DESCRIBE HISTORY employee

-- COMMAND ----------

RESTORE TABLE employee to version as of 3

-- COMMAND ----------

select * from employee

-- COMMAND ----------

DESCRIBE HISTORY employee

-- COMMAND ----------

DESCRIBE DETAIL employee

-- COMMAND ----------

optimize employee zorder by id

-- COMMAND ----------

DESCRIBE DETAIL employee

-- COMMAND ----------

-- MAGIC %fs ls dbfs:/user/hive/warehouse/employee

-- COMMAND ----------

vacuum employee

-- COMMAND ----------

-- MAGIC %fs ls dbfs:/user/hive/warehouse/employee

-- COMMAND ----------

SET spark.databricks.delta.retentionDurationCheck.enabled = false

-- COMMAND ----------

vacuum employee retain 0 hours

-- COMMAND ----------

-- MAGIC %fs ls dbfs:/user/hive/warehouse/employee

-- COMMAND ----------

select * from employee

-- COMMAND ----------

select * from employee @v 1

-- COMMAND ----------

