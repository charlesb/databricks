-- Databricks notebook source
OPTIMIZE tweets.`bronze`;

OPTIMIZE tweets.`siver`;

OPTIMIZE tweets.`gold`;

-- COMMAND ----------

VACUUM tweets .`gold` RETAIN 168 HOURS;