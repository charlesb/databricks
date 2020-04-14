-- Databricks notebook source
OPTIMIZE tweets.`bronze`;

-- COMMAND ----------

OPTIMIZE tweets.`siver`;

-- COMMAND ----------

OPTIMIZE tweets.`gold`;

-- COMMAND ----------

-- MAGIC %python
-- MAGIC spark.sql("set spark.databricks.delta.retentionDurationCheck.enabled=false")
-- MAGIC 
-- MAGIC spark.sql("VACUUM tweets.`bronze` RETAIN 0 HOURS")

-- COMMAND ----------

-- VACUUM tweets .`bronze` RETAIN 168 HOURS;

-- VACUUM tweets .`bronze` RETAIN 168 HOURS;

-- VACUUM tweets .`bronze` RETAIN 168 HOURS;
