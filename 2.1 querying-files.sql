-- Databricks notebook source
-- MAGIC %run ./includes/copy-datasets

-- COMMAND ----------

-- MAGIC %python
-- MAGIC
-- MAGIC files = dbutils.fs.ls(f"{dataset_bookstore}/customers-json")
-- MAGIC
-- MAGIC display(files)

-- COMMAND ----------

SELECT `${dataset.bookstore}`

-- COMMAND ----------

SELECT * FROM JSON.`${dataset.bookstore}/customers-json/*.json`

-- COMMAND ----------

SELECT
  *,
  input_file_name() AS source_file
FROM JSON.`${dataset.bookstore}/customers-json/*.json`

-- COMMAND ----------

SELECT
  *,
  input_file_name() AS source_file
FROM TEXT.`${dataset.bookstore}/customers-json/*.json`

-- COMMAND ----------

SELECT
  *,
  input_file_name() AS source_file
FROM binaryFile.`${dataset.bookstore}/customers-json/*.json`

-- COMMAND ----------

SELECT
  *,
  input_file_name() AS source_file
FROM CSV.`${dataset.bookstore}/books-csv`

-- COMMAND ----------

CREATE TABLE books_csv
USING CSV
OPTIONS (
  header = "true",
  delimiter = ";"
)
LOCATION "${dataset.bookstore}/books-csv";

SELECT * FROM books_csv;

-- COMMAND ----------

DESC EXTENDED books_csv

-- COMMAND ----------

-- MAGIC %python
-- MAGIC
-- MAGIC df = spark.read.format("csv").options(header="true", delimiter=";").load(f"{dataset_bookstore}/books-csv")
-- MAGIC df.limit(5).display()

-- COMMAND ----------

-- MAGIC %python
-- MAGIC
-- MAGIC dataset_bookstore

-- COMMAND ----------

-- MAGIC %python
-- MAGIC
-- MAGIC df.write.format("csv").options(header="true", delimiter=";").mode("append").save(f"{dataset_bookstore}/books-csv")

-- COMMAND ----------

-- MAGIC %python
-- MAGIC
-- MAGIC display(dbutils.fs.ls(path=f"{dataset_bookstore}/books-csv"))

-- COMMAND ----------

SELECT COUNT(*)
FROM books_csv

-- COMMAND ----------

REFRESH TABLE books_csv

-- COMMAND ----------

SELECT COUNT(*)
FROM books_csv

-- COMMAND ----------

DROP TABLE IF EXISTS customers;

CREATE TABLE customers
AS
SELECT *
FROM JSON.`${dataset.bookstore}/customers-json`;

SELECT * FROM customers;

-- COMMAND ----------

DESC EXTENDED customers;

-- COMMAND ----------

CREATE TABLE books_unparsed
AS
SELECT *
FROM CSV.`${dataset.bookstore}/books-csv`;

SELECT * FROM books_unparsed;

-- COMMAND ----------

CREATE TEMP VIEW books_tmp_vw
  (book_id STRING, title STRING, author STRING, category STRING, price DOUBLE)
USING CSV
OPTIONS (
  path "${dataset.bookstore}/books-csv",
  header "true",
  delimiter ";"
);

CREATE TABLE books
AS 
SELECT *
FROM books_tmp_vw;

SELECT * FROM books;

-- COMMAND ----------

DESC EXTENDED books;
