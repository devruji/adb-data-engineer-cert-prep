-- Databricks notebook source
-- MAGIC %fs ls /mnt/demo-datasets/bookstore

-- COMMAND ----------

CREATE TABLE orders
AS 
SELECT *
FROM PARQUET.`/mnt/demo-datasets/bookstore/orders`;

SELECT * FROM orders;

-- COMMAND ----------

DESC EXTENDED orders;

-- COMMAND ----------

CREATE OR REPLACE TABLE orders
AS 
SELECT *
FROM PARQUET.`/mnt/demo-datasets/bookstore/orders`;

SELECT * FROM orders;

-- COMMAND ----------

DESC HISTORY orders;

-- COMMAND ----------

INSERT OVERWRITE orders
SELECT *
FROM PARQUET.`/mnt/demo-datasets/bookstore/orders`;

SELECT * FROM orders;

-- COMMAND ----------

DESC HISTORY orders;

-- COMMAND ----------

INSERT OVERWRITE orders
SELECT *,
  current_timestamp()
FROM PARQUET.`/mnt/demo-datasets/bookstore/orders`;

SELECT * FROM orders;

-- COMMAND ----------

INSERT INTO orders
SELECT *
FROM PARQUET.`/mnt/demo-datasets/bookstore/orders-new`;

SELECT * FROM orders;

-- COMMAND ----------

DESC HISTORY orders;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC
-- MAGIC #### MERGE INTO

-- COMMAND ----------

CREATE OR REPLACE TEMP VIEW customers_updates
AS
SELECT *
FROM JSON.`/mnt/demo-datasets/bookstore/customers-json-new`;

SELECT * FROM customers_updates;

-- COMMAND ----------

MERGE INTO customers c USING customers_updates cu ON c.customer_id = cu.customer_id
WHEN MATCHED
AND c.email IS NOT NULL
AND cu.email IS NOT NULL THEN
UPDATE
SET
  email = cu.email,
  updated = cu.updated
  WHEN NOT MATCHED THEN
INSERT
  *

-- COMMAND ----------

DESC HISTORY customers;

-- COMMAND ----------

MERGE INTO customers c USING customers_updates cu ON c.customer_id = cu.customer_id
WHEN MATCHED
AND c.email IS NOT NULL
AND cu.email IS NOT NULL THEN
UPDATE
SET
  email = cu.email,
  updated = cu.updated
  WHEN NOT MATCHED THEN
INSERT
  *

-- COMMAND ----------

DESC HISTORY customers;

-- COMMAND ----------

CREATE OR REPLACE TEMP VIEW books_updates
  (book_id STRING, title STRING, author STRING, category STRING, price DOUBLE)
USING CSV
OPTIONS
  (
    path = "/mnt/demo-datasets/bookstore/books-csv-new",
    header = "true",
    delimiter = ";"
  );

SELECT * FROM books_updates;

-- COMMAND ----------

MERGE INTO books b
USING books_updates u
ON b.book_id = u.book_id
  AND b.title = u.title
WHEN NOT MATCHED AND u.category = 'Computer Science' THEN INSERT *
