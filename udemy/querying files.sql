-- Databricks notebook source
-- MAGIC %run ../Includes/Copy-Datasets

-- COMMAND ----------

-- MAGIC %python
-- MAGIC files = dbutils.fs.ls(f"{dataset_bookstore}/customers-json")
-- MAGIC display(files)

-- COMMAND ----------

select * from json.`${dataset.bookstore}/customers-json/export_001.json`

-- COMMAND ----------

select * from json.`${dataset.bookstore}/customers-json/export_*.json`

-- COMMAND ----------

select * from json.`${dataset.bookstore}/customers-json`

-- COMMAND ----------

select count(*) from json.`${dataset.bookstore}/customers-json`

-- COMMAND ----------

select *, input_file_name() as source
from json.`${dataset.bookstore}/customers-json`;

-- COMMAND ----------

select * from text.`${dataset.bookstore}/customers-json`;

-- COMMAND ----------

select * from binaryFile.`${dataset.bookstore}/customers-json`;

-- COMMAND ----------

select * from csv.`${dataset.bookstore}/books-csv`

-- COMMAND ----------

create table books_csv
  (book_id STRING, title STRING, author STRING, catagory STRING, price DOUBLE)
USING CSV
options (header = "true",
   delimiter = ";")
LOCATION "${dataset.bookstore}/books-csv"

-- COMMAND ----------

select * from books_csv

-- COMMAND ----------

DESCRIBE EXTENDED books_csv

-- COMMAND ----------

-- MAGIC %python
-- MAGIC (spark.read
-- MAGIC            .table("books_csv")
-- MAGIC            .write
-- MAGIC               .mode("append")
-- MAGIC            .format("csv")
-- MAGIC             .option('header','true')
-- MAGIC             .option('delimiter', ';')
-- MAGIC             .save(f"{dataset_bookstore}/books-csv")
-- MAGIC )

-- COMMAND ----------

-- MAGIC %python
-- MAGIC files = dbutils.fs.ls(f"{dataset_bookstore}/books-csv")
-- MAGIC display(files)

-- COMMAND ----------

select count(*) from books_csv

-- COMMAND ----------

refresh table books_csv

-- COMMAND ----------

select count(*) from books_csv

-- COMMAND ----------

create table customers as
select * from json.`${dataset.bookstore}/customers-json`;

-- COMMAND ----------

describe extended customers;

-- COMMAND ----------

create table books_unparsed as
select * from csv.`${dataset.bookstore}/books-csv`;

select * from books_unparsed

-- COMMAND ----------

describe extended books_unparsed

-- COMMAND ----------

create temp view books_tmp_vw
 (book_id STRING, title STRING, author STRING, category STRING, prce DOUBLE)
 using csv
 options(
 path = "${dataset.bookstore}/books-csv/export_*.csv",
 header = "true",
 delimiter  ";"
 );
 
 create table books as select * from books_tmp_vw;
 select * from books

-- COMMAND ----------

describe extended books

-- COMMAND ----------

