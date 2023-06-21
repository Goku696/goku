-- Databricks notebook source
create table managed_default
  (width INT, length INT, height INT);
  
INSERT into managed_default
values (3,2,1);

-- COMMAND ----------

DESCRIBE EXTENDED managed_default

-- COMMAND ----------

create table external_default
 (width INT, length INT, eight INT)
location 'dbfs:/mnt/demo/external_default';

insert into external_default
values (3,2,1);

-- COMMAND ----------

DESCRIBE DETAIL external_default

-- COMMAND ----------

describe extended external_default

-- COMMAND ----------

DROP TABLE managed_default

-- COMMAND ----------

-- MAGIC %fs ls 'dbfs:/user/hive/warehouse/managed_default'

-- COMMAND ----------

drop table external_default

-- COMMAND ----------

-- MAGIC %fs ls dbfs:/mnt/demo/external_default

-- COMMAND ----------

CREATE SCHEMA new_default;

-- COMMAND ----------

describe database extended new_default;

-- COMMAND ----------

use new_default;
create table managed_default
 (width INT, length INT, eight INT);

insert into managed_default
values (3,2,1);


-- COMMAND ----------

drop table external_default

-- COMMAND ----------

create table external_default
 (width INT, length INT, eight INT)
location 'dbfs:/mnt/demo/external_new_default';

insert into external_default
values (3,2,1);

-- COMMAND ----------

