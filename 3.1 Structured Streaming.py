# Databricks notebook source
(
    spark
    .readStream
    .table("books")
    .createOrReplaceTempView("books_streaming_tmp_vw")
)

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC SELECT * FROM books_streaming_tmp_vw

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC SELECT 
# MAGIC   author,
# MAGIC   count(book_id) AS total_books
# MAGIC FROM books_streaming_tmp_vw
# MAGIC GROUP BY 1;

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC SELECT *
# MAGIC FROM books_streaming_tmp_vw
# MAGIC ORDER BY author

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC CREATE OR REPLACE TEMP VIEW author_counts_tmp_vw AS (
# MAGIC   SELECT
# MAGIC     author,
# MAGIC     count(book_id) AS total_books
# MAGIC     FROM books_streaming_tmp_vw
# MAGIC     GROUP BY 1
# MAGIC )

# COMMAND ----------

(
    spark
    .table("author_counts_tmp_vw")
    .writeStream
    .trigger(processingTime='4 seconds')
    .outputMode("complete")
    .option("checkpointLocation", "dbfs:/mnt/demo/author_counts_checkpoints")
    .table("author_counts")
)

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC SELECT *
# MAGIC FROM author_counts

# COMMAND ----------

# MAGIC %md 
# MAGIC
# MAGIC ### try to add the new rows to books table

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC INSERT INTO
# MAGIC   books
# MAGIC values
# MAGIC   (
# MAGIC     "B16",
# MAGIC     "Hands-On Deep Learning Algorithms with Python",
# MAGIC     "Sudharsan Ravichandiran",
# MAGIC     "Computer Science",
# MAGIC     25
# MAGIC   ),
# MAGIC   (
# MAGIC     "B17",
# MAGIC     "Neural Network Methods in Natural Language Processing",
# MAGIC     "Yoav Goldberg",
# MAGIC     "Computer Science",
# MAGIC     30
# MAGIC   ),
# MAGIC   (
# MAGIC     "B18",
# MAGIC     "Understanding digital signal processing",
# MAGIC     "Richard Lyons",
# MAGIC     "Computer Science",
# MAGIC     35
# MAGIC   )

# COMMAND ----------

(
    spark
    .table("author_counts_tmp_vw")
    .writeStream
    .trigger(availableNow=True)
    .outputMode("complete")
    .option("checkpointLocation", "dbfs:/mnt/demo/author_counts_checkpoints")
    .table("author_counts")
    .awaitTermination()
)

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC SELECT *
# MAGIC FROM author_counts
