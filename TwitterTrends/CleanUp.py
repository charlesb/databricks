# Databricks notebook source
mntPath = dbutils.widgets.get("mntPath")
bronzeCheckpointPath = dbutils.widgets.get("bronzeCheckpointPath")
silveCheckpointPath = dbutils.widgets.get("silveCheckpointPath")
goldCheckpointPath = dbutils.widgets.get("goldCheckpointPath")

# COMMAND ----------

try:
  mounts = dbutils.fs.ls(mntPath)
  dbutils.fs.rm(bronzeCheckpointPath, True)
  dbutils.fs.rm(silverCheckpointPath, True)
  dbutils.fs.rm(goldCheckpointPath, True)
except:
  dbutils.fs.mkdirs(mntPath)
  dbutils.fs.mount("s3a://{}/tweets/".format(spark.conf.get("internal.s3-bucket")), mntPath)

# COMMAND ----------

# MAGIC %sql drop database if exists tweets cascade

# COMMAND ----------

# MAGIC %sql create database tweets

# COMMAND ----------

# MAGIC %sql
# MAGIC create table tweets.`bronze` (
# MAGIC tweet string
# MAGIC )
# MAGIC using delta;

# COMMAND ----------

# MAGIC %sql
# MAGIC create table tweets.`silver` (
# MAGIC id long,
# MAGIC user string,
# MAGIC hashtag string,
# MAGIC lang string,
# MAGIC text string,
# MAGIC createdAt timestamp
# MAGIC )
# MAGIC using delta;

# COMMAND ----------

# MAGIC %sql
# MAGIC create table tweets.`gold` (
# MAGIC hashtag string,
# MAGIC date timestamp,
# MAGIC hour int,
# MAGIC count long
# MAGIC )
# MAGIC using delta;

# COMMAND ----------

import json

dbutils.notebook.exit(json.dumps({
  "status": "OK"
}))
