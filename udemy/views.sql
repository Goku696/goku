-- Databricks notebook source
create table if not exists smartphones
(id INT, name STRING, brand STRING, year INT);

INSERT INTo smartphones
values (1,'iphone14', 'apple', 2022),
        (2, 'iphone13', 'apple', 2021),
        (3, 'galaxy s22', 'samsung', 2016);

-- COMMAND ----------

show tables

-- COMMAND ----------

-- create view onlly for apple phone

create view view_apple_phones
as select *
   from smartphones
   where brand = "apple";

-- COMMAND ----------

select * from view_apple_phones;

-- COMMAND ----------

select * from smartphones;

-- COMMAND ----------

show tables;

-- COMMAND ----------

-- create a temporary view
create TEMPORARY VIEW temp_view_phones_brands
as select DISTINCT brand
from smartphones;

select * from temp_view_phones_brands;

-- COMMAND ----------

show tables;

-- COMMAND ----------

-- global temporary view

create GLOBAL TEMP VIEW global_temp_view_latest_phones
as select * from smartphones
where year >2020
order by year desc;

-- COMMAND ----------

select * from global_temp.global_temp_view_latest_phones

-- COMMAND ----------

show tables
-- global temp is not listed as it is tied to globa_temp cluster db

-- COMMAND ----------

show tables in global_temp;

-- COMMAND ----------

