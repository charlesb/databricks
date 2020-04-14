-- Databricks notebook source
OPTIMIZE tweets.`bronze`;

OPTIMIZE tweets.`siver`;

OPTIMIZE tweets.`gold`;

-- COMMAND ----------

-- VACUUM tweets .`bronze` RETAIN 168 HOURS;

-- VACUUM tweets .`bronze` RETAIN 168 HOURS;

-- VACUUM tweets .`bronze` RETAIN 168 HOURS;
