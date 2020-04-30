# Databricks notebook source
# MAGIC %md-sandbox
# MAGIC <img src="https://files.training.databricks.com/images/Apache-Spark-Logo_TM_200px.png" style="float: left: margin: 20px"/>
# MAGIC 
# MAGIC # Streaming ETL

# COMMAND ----------

mntPath = "dbfs:/mnt/tweets"

# bronzePath           = mntPath + "/bronze.delta"
bronzeCheckpointPath = mntPath + "/bronze.checkpoint"

# silverPath           = mntPath + "/silver.delta"
silverCheckpointPath = mntPath + "/silver.checkpoint"

# goldPath             = mntPath + "/gold.delta"
goldCheckpointPath   = mntPath + "/gold.checkpoint"

dbutils.widgets.text("mntPath", mntPath)
dbutils.widgets.text("bronzeCheckpointPath", bronzeCheckpointPath)
dbutils.widgets.text("silveCheckpointPath", silverCheckpointPath)
dbutils.widgets.text("goldCheckpointPath", goldCheckpointPath)

bronzeStreamName = "bronze_stream"
silverStreamName = "silver_stream"
goldStreamName = "gold_stream"

# COMMAND ----------

# MAGIC %run ./CleanUp

# COMMAND ----------

import twitter

api = twitter.Api(
  consumer_key = spark.conf.get("spark.consumer-key"),
  consumer_secret = spark.conf.get("spark.consumer-secret"),
  access_token_key = spark.conf.get("spark.access-token"),
  access_token_secret = spark.conf.get("spark.access-secret")
)

# COMMAND ----------

api.GetTrendsWoeid("1062617") # WOEID (Where On Earth IDentifier) for Singapore

# COMMAND ----------

from pyspark.sql.functions import col
spark.conf.set("spark.sql.shuffle.partitions", sc.defaultParallelism)

(spark.readStream
 .format("kafka")
 .option("kafka.bootstrap.servers", spark.conf.get("spark.kafka-server"))
 .option("subscribe", "tweets")
 .option("startingOffsets", "earliest")
 .option("maxOffsetsPerTrigger", 1000)
 .load()
 .withColumn("tweet", col("value").cast("STRING"))
 .select("tweet")
 .writeStream
 .format("delta")
 .option("checkpointLocation", bronzeCheckpointPath)
 .outputMode("append")
 .queryName(bronzeStreamName)
 .table("tweets.`bronze`")
)

# COMMAND ----------

# %sql select * from tweets.`bronze`;

# COMMAND ----------

from pyspark.sql.types import StructField, StructType, ArrayType, StringType, IntegerType, LongType, TimestampType
from pyspark.sql.functions import lower, from_json, explode, dayofmonth, year, month

schema = StructType([
  StructField("id", LongType(), True),
  StructField("user", StringType(), True),
  StructField("hashTags", ArrayType(StringType()), True),
  StructField("lang", StringType(), True),
  StructField("text", StringType(), True),
  StructField("createdAt", LongType(), True)
])

(spark.readStream.table("tweets.`bronze`")
 .withColumn("json", from_json(col("tweet"), schema))
 .filter(col("json.id").isNotNull())
 .withColumn("hashtag", explode("json.hashTags"))
 .withColumn("hashtag", lower(col("hashtag")))
 .withColumn("createdAt", (col("json.createdAt").cast(LongType())/1000).cast(TimestampType()))
 .withColumn("year", year(col("createdAt")))
 .withColumn("month", month(col("createdAt")))
 .withColumn("day", dayofmonth(col("createdAt")))
 .select("json.id", "json.user", "hashtag", "json.lang", "json.text", "createdAt", "year", "month", "day")
 .writeStream
 .format("delta")
 .option("checkpointLocation", silverCheckpointPath)
 .outputMode("append")
 .queryName(silverStreamName)
 .table("tweets.`silver`")
)


# COMMAND ----------

# %sql select * from tweets.`silver` order by createdAt desc limit 10;

# COMMAND ----------

from pyspark.sql.functions import window, hour, from_unixtime

(spark.readStream.table("tweets.`silver`")
 .withWatermark("createdAt", "60 minutes")
 .groupBy(window("createdAt", "60 minutes"), "hashtag")
 .count()
 .withColumn("datetime", from_unixtime(col("window.start").cast('long'), "yyyy-MM-dd HH:mm:ss"))
 .withColumn("hour", hour(col("window.start")))
 .select("hashtag", "datetime", "hour", "count")
 .orderBy("datetime", "hour", "count", ascending=False)
 .writeStream
 .format("delta")
 .option("checkpointLocation", goldCheckpointPath)
 .outputMode("complete")
 .queryName(goldStreamName)
 .table("tweets.`gold`")
)

# COMMAND ----------

#  %sql select * from tweets.`gold` order by datetime desc, hour desc, count desc;

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from (
# MAGIC select datetime, hashtag, count, rank() over (partition by datetime order by count desc) as rank
# MAGIC from tweets.`gold`
# MAGIC )
# MAGIC where rank <= 10
# MAGIC order by datetime desc, rank asc;

# COMMAND ----------

for s in spark.streams.active:
  s.stop()
  print(s.name)