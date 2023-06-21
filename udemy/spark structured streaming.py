# Databricks notebook source
# MAGIC %run  ../Includes/Copy-Datasets

# COMMAND ----------

(spark.readStream
      .table("books")
      .createOrReplaceTempView("books_streaming_tmp_vw")
)

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from books_streaming_tmp_vw

# COMMAND ----------

# MAGIC %sql
# MAGIC select author, count(book_id) as total_books
# MAGIC from books_streaming_tmp_vw
# MAGIC group by author

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REPLACE temp view author_counts_tmp_vw as (
# MAGIC   select author, count(book_id) as total_books
# MAGIC   from books_streaming_tmp_vw
# MAGIC   group by author
# MAGIC )

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from author_counts_tmp_vw

# COMMAND ----------

(spark.table("author_counts_tmp_vw")
      .writeStream
      .trigger(processingTime='4 seconds')
      .outputMode("complete")
      .option("checkpointLocation","dbfs:/mnt/demo/author_counts_checkpoint")
      .table("author_counts")
)

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from author_counts

# COMMAND ----------

# MAGIC %sql
# MAGIC INSERT INTO books
# MAGIC VALUES ("B19", "123","123","123",123),
# MAGIC        ("B20", "123","124","123",123),
# MAGIC        ("B21", "123","125","123",123);

# COMMAND ----------

(spark.table("author_counts_tmp_vw")
      .writeStream
      .trigger(availableNow=True)
      .outputMode("complete")
      .option("checkpointLocation","dbfs:/mnt/demo/author_counts_checkpoint")
      .table("author_counts")
      . awaitTermination()
)

# COMMAND ----------

# MAGIC %sql
# MAGIC INSERT INTO books
# MAGIC VALUES ("B22", "123","126","123",123),
# MAGIC        ("B23", "123","127","123",123),
# MAGIC        ("B24", "123","128","123",123);

# COMMAND ----------

# MAGIC %sql
# MAGIC INSERT INTO books
# MAGIC VALUES ("B25", "123","129","123",123),
# MAGIC        ("B26", "123","130","123",123),
# MAGIC        ("B27", "123","131","123",123);

# COMMAND ----------

