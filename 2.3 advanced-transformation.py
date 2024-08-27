# Databricks notebook source
# MAGIC %fs ls /mnt/demo-datasets/bookstore

# COMMAND ----------

dataset_path: str = "/mnt/demo-datasets/bookstore"

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC SELECT * FROM customers LIMIT 5;

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC DESC EXTENDED customers;

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC #### Access the value in JSON string

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC SELECT
# MAGIC   customer_id,
# MAGIC   profile:first_name,
# MAGIC   profile:address:country
# MAGIC FROM customers
# MAGIC LIMIT 5

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC SELECT
# MAGIC   from_json(profile) AS profile_struct
# MAGIC FROM customers
# MAGIC LIMIT 5;
# MAGIC
# MAGIC -- Error because spark require schema for the json object

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC -- we can derive the schema from our current data
# MAGIC SELECT
# MAGIC   profile
# MAGIC FROM customers
# MAGIC LIMIT 1

# COMMAND ----------

# MAGIC
# MAGIC %sql
# MAGIC CREATE OR REPLACE TEMP VIEW parsed_customers
# MAGIC AS
# MAGIC SELECT
# MAGIC   customer_id,
# MAGIC   from_json(profile, schema_of_json('{"first_name":"Dniren","last_name":"Abby","gender":"Female","address":{"street":"768 Mesta Terrace","city":"Annecy","country":"France"}}')) AS profile_struct
# MAGIC FROM customers;
# MAGIC
# MAGIC SELECT * FROM parsed_customers LIMIT 5;

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC DESC parsed_customers

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC SELECT
# MAGIC   customer_id,
# MAGIC   profile_struct.first_name,
# MAGIC   profile_struct.address.country
# MAGIC FROM parsed_customers
# MAGIC LIMIT 5;

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC -- We can use * operation to flatten fields into columns
# MAGIC
# MAGIC CREATE OR REPLACE TEMP VIEW customers_final
# MAGIC AS
# MAGIC SELECT
# MAGIC   customer_id,
# MAGIC   profile_struct.*
# MAGIC FROM parsed_customers;
# MAGIC
# MAGIC SELECT * FROM customers_final LIMIT 5;

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC #### Explode function

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC SELECT
# MAGIC   order_id,
# MAGIC   customer_id,
# MAGIC   books
# MAGIC FROM orders
# MAGIC LIMIT 5;

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC DESC orders

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC SELECT
# MAGIC   order_id,
# MAGIC   customer_id,
# MAGIC   explode(books) AS books
# MAGIC FROM orders
# MAGIC LIMIT 5;

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC #### collect_set()

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC SELECT
# MAGIC   customer_id,
# MAGIC   collect_set(order_id) AS orders_set,
# MAGIC   collect_set(books.book_id) AS books_set
# MAGIC FROM orders
# MAGIC GROUP BY customer_id
# MAGIC LIMIT 10;

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC SELECT
# MAGIC   customer_id,
# MAGIC   collect_set(books.book_id) AS before_flatten,
# MAGIC   array_distinct(flatten(collect_set(books.book_id))) AS after_flatten
# MAGIC FROM orders
# MAGIC GROUP BY customer_id
# MAGIC LIMIT 10;

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC #### Join operations

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REPLACE VIEW orders_enriched
# MAGIC AS
# MAGIC SELECT *
# MAGIC FROM (
# MAGIC   SELECT *, explode(books) AS book FROM orders
# MAGIC ) o
# MAGIC INNER JOIN books b
# MAGIC ON o.book.book_id = b.book_id;
# MAGIC
# MAGIC SELECT * FROM orders_enriched;

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC CREATE OR REPLACE TEMP VIEW orders_updates
# MAGIC AS
# MAGIC SELECT *
# MAGIC FROM parquet.`/mnt/demo-datasets/bookstore/orders-new`;
# MAGIC
# MAGIC SELECT * FROM orders
# MAGIC UNION
# MAGIC SELECT * FROM orders_updates;

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC SELECT * FROM orders
# MAGIC INTERSECT
# MAGIC SELECT * FROM orders_updates;

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC SELECT * FROM orders
# MAGIC MINUS
# MAGIC SELECT * FROM orders_updates;

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC CREATE OR REPLACE TABLE transactions
# MAGIC AS
# MAGIC SELECT *
# MAGIC FROM (
# MAGIC   SELECT 
# MAGIC     customer_id,
# MAGIC     book.book_id AS book_id,
# MAGIC     book.quantity AS quantity
# MAGIC   FROM orders_enriched
# MAGIC ) PIVOT (
# MAGIC   SUM(quantity) FOR book_id IN (
# MAGIC     'B01', 'B02', 'B03'
# MAGIC   )
# MAGIC );
# MAGIC
# MAGIC SELECT * FROM transactions;
