-- Databricks notebook source
-- MAGIC %md
-- MAGIC
-- MAGIC ### Create actual table

-- COMMAND ----------

SELECT current_catalog()

-- COMMAND ----------

SELECT current_database()

-- COMMAND ----------

CREATE TABLE IF NOT EXISTS customers (CustomerName STRING, ContactName STRING, Address STRING, City STRING, PostalCode STRING, Country STRING);

INSERT INTO customers
VALUES
  ('Cardinal', 'Tom B. Erichsen', 'Skagen 21', 'Stavanger', '4006', 'Norway'),
  ('Greasy Burger', 'Per Olsen', 'Gateveien 15', 'Sandnes', '4306', 'Norway'),
  ('Tasty Tee', 'Finn Egan', 'Streetroad 19B', 'Liverpool', 'L1 0AA', 'UK');

SELECT * FROM customers

-- COMMAND ----------

-- MAGIC %md
-- MAGIC
-- MAGIC ### Create View

-- COMMAND ----------

CREATE OR REPLACE VIEW v_country_norway
AS
SELECT *
FROM customers
WHERE country = 'Norway';

SELECT * FROM v_country_norway;

-- COMMAND ----------

SHOW TABLES

-- COMMAND ----------

-- MAGIC %md
-- MAGIC
-- MAGIC ### Create TEMP View

-- COMMAND ----------


CREATE OR REPLACE TEMP VIEW v_country_uk
AS
SELECT *
FROM customers
WHERE country = 'UK';

SELECT * FROM v_country_uk;

-- COMMAND ----------

SHOW TABLES

-- COMMAND ----------

-- MAGIC %md 
-- MAGIC
-- MAGIC ### Create GLOBAL View

-- COMMAND ----------


CREATE OR REPLACE GLOBAL TEMP VIEW v_customers_eiei
AS
SELECT *
FROM customers;

SELECT * FROM global_temp.v_customers_eiei;

-- COMMAND ----------

SHOW TABLES

-- COMMAND ----------

SHOW TABLES

-- COMMAND ----------

SHOW VIEWS IN global_temp;
