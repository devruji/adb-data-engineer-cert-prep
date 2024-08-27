-- Databricks notebook source
-- MAGIC %fs ls /mnt/demo-datasets/bookstore

-- COMMAND ----------

SELECT * FROM orders LIMIT 5;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC
-- MAGIC ### Higher order functions: filter
-- MAGIC
-- MAGIC - Allow you to work directly with hierarchical data like arrays and map type objects
-- MAGIC - https://docs.databricks.com/en/sql/language-manual/functions/filter.html

-- COMMAND ----------

-- One of the most common higher order functions is the `filter()` function
-- Syntax: filter(expr, func)

SELECT
  order_id,
  books,
  FILTER (books, i -> i.quantity >= 2) AS multiple_copies
FROM orders;

-- COMMAND ----------

SELECT order_id, multiple_copies
FROM (
  SELECT
    order_id,
    books,
    FILTER (books, i -> i.quantity >= 2) AS multiple_copies
  FROM orders
)
WHERE SIZE(multiple_copies) > 0;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC
-- MAGIC ### Higher order function: transform
-- MAGIC - https://docs.databricks.com/en/sql/language-manual/functions/transform.html

-- COMMAND ----------

SELECT
  order_id,
  books,
  TRANSFORM (
    books,
    b -> CAST(b.subtotal * 0.8 AS INT)
  ) AS subtotal_after_discount
FROM orders;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC
-- MAGIC ### UDF - User Defined Function
-- MAGIC - which allow you to register a custom combination of SQL logic as function in a database, making these methos reusable in any SQL query
-- MAGIC - In addition, UDF functions leverage spark SQL directly maintaining all the optimization of Spark when applying your custom logic to large datasets

-- COMMAND ----------

CREATE OR REPLACE FUNCTION get_url(email STRING) RETURNS STRING

RETURN CONCAT("https://www.", SPLIT(email, "@")[1])

-- COMMAND ----------

SELECT *
FROM customers LIMIT 5;

-- COMMAND ----------

SELECT email, get_url(email) AS domain
FROM customers LIMIT 5

-- COMMAND ----------

DESC FUNCTION get_url

-- COMMAND ----------

DESC FUNCTION EXTENDED get_url

-- COMMAND ----------

CREATE FUNCTION site_type(email STRING) RETURNS STRING
RETURN 
  CASE
    WHEN email LIKE '%.com' THEN "Commercial business"
    WHEN email LIKE '%.org' THEN "Non-profit organization"
    WHEN email LIKE '%.edu' THEN "Educational institution"
    ELSE CONCAT("Unknown extension for domain: ", SPLIT(email, "@")[1])
  END
;

-- COMMAND ----------

SELECT email, site_type(email) AS domain_category
FROM customers

-- COMMAND ----------

DESC FUNCTION EXTENDED site_type

-- COMMAND ----------

-- MAGIC %md
-- MAGIC
-- MAGIC ### Summary
-- MAGIC
-- MAGIC - UDF functions are really powerful
-- MAGIC - Remeber, everything is evaluated natively in Spark
-- MAGIC   - And so it's optimized for parallel execution

-- COMMAND ----------

DROP FUNCTION get_url;
DROP FUNCTION site_type;
