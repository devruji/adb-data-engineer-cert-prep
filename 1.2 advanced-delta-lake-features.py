# Databricks notebook source
# MAGIC %sql
# MAGIC
# MAGIC DESCRIBE HISTORY employees

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC SELECT *
# MAGIC FROM employees
# MAGIC ORDER BY id

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC SELECT *
# MAGIC FROM employees VERSION AS OF 1
# MAGIC ORDER BY id

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC SELECT *
# MAGIC FROM employees@v1
# MAGIC ORDER BY id

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC DELETE FROM employees;

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC DESCRIBE HISTORY employees

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC RESTORE TABLE employees VERSION AS OF 2

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC DESCRIBE HISTORY employees

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC DESCRIBE DETAIL employees

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC OPTIMIZE employees
# MAGIC ZORDER BY id

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC DESCRIBE DETAIL employees

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC DESCRIBE HISTORY employees

# COMMAND ----------

# MAGIC %fs ls 'dbfs:/user/hive/warehouse/employees'

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC VACUUM employees;

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC VACUUM employees RETAIN 0 HOURS;

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC SET spark.databricks.delta.retentionDurationCheck.enabled = false;
# MAGIC VACUUM employees RETAIN 0 HOURS;

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC SELECT *
# MAGIC FROM employees@v1

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC DROP TABLE IF EXISTS employees;

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC SELECT * FROM employees

# COMMAND ----------

display(dbutils.fs.ls("dbfs:/user/hive/warehouse/employees"))
