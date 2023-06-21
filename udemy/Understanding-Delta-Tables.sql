-- Databricks notebook source
create table employee 
(id INT,name STRING, sal DOUBLE);

-- COMMAND ----------

insert into employee
values
(1, "adam", 220),
(2, "padam", 300);

-- COMMAND ----------

describe DETAIL employee;

-- COMMAND ----------

-- MAGIC %fs ls dbfs:/user/hive/warehouse/employee

-- COMMAND ----------

-- MAGIC %fs ls dbfs:/user/hive/warehouse/employee/_delta_log/

-- COMMAND ----------

-- MAGIC %fs ls dbfs:/user/hive/warehouse/employee/_delta_log/00000000000000000000.json

-- COMMAND ----------

describe history employee

-- COMMAND ----------

insert into employee
values
(1, "badam", 220),
(2, "cadam", 300);

-- COMMAND ----------

select * from employee

-- COMMAND ----------

drop table emp

-- COMMAND ----------

drop table em

-- COMMAND ----------

UPDATE employee
SET sal = sal + 100
WHERE name like "a%"

-- COMMAND ----------

select * from employee

-- COMMAND ----------

DESCRIBE DETAIL employee

-- COMMAND ----------

-- MAGIC %fs ls dbfs:/user/hive/warehouse/employee

-- COMMAND ----------

DESCRIBE HISTORY employee


-- COMMAND ----------

-- MAGIC %fs ls dbfs:/user/hive/warehouse/employee/_delta_log

-- COMMAND ----------

-- MAGIC %fs head dbfs:/user/hive/warehouse/employee/_delta_log/00000000000000000003.json

-- COMMAND ----------

