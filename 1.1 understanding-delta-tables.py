# Databricks notebook source
# MAGIC %sql
# MAGIC SELECT current_catalog()

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC SELECT current_database()

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC CREATE TABLE IF NOT EXISTS employees (
# MAGIC   id INT,
# MAGIC   name STRING,
# MAGIC   salary DOUBLE
# MAGIC )

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC INSERT INTO employees 
# MAGIC VALUES 
# MAGIC   (1, 'Alice', 100000.0),
# MAGIC   (2, 'Bob', 120000.0),
# MAGIC   (3, 'Charlie', 150000.0),
# MAGIC   (4, 'Dave', 110000.0),
# MAGIC   (5, 'Eve', 130000.0),
# MAGIC   (6, 'Frank', 140000.0)
# MAGIC

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC DESC EXTENDED employees;

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC SELECT * FROM employees;

# COMMAND ----------

# MAGIC %fs ls 'dbfs:/user/hive/warehouse/employees'

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC UPDATE employees
# MAGIC SET salary = salary * 1.1
# MAGIC WHERE id = 1;

# COMMAND ----------

# MAGIC %fs ls 'dbfs:/user/hive/warehouse/employees'

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC DESC EXTENDED employees;

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC DESC HISTORY employees;

# COMMAND ----------

# MAGIC %fs ls 'dbfs:/user/hive/warehouse/employees/_delta_log'
