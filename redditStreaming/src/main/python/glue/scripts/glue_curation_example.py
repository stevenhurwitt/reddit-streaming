# from awsglue.transforms import *
# from awsglue.utils import getResolvedOptions
<<<<<<< HEAD
# from awsglue.context import GlueContext
# from awsglue.job import Job
from pyspark.sql.session import SparkSession
from pyspark.context import SparkContext
from pyspark.sql.functions import *
from pyspark.sql.types import *
from delta.table import *
from delta import *
import datetime as dt
import pandas as pd
import numpy as np
=======
from pyspark.context import SparkContext
# from awsglue.context import GlueContext
# from awsglue.job import Job
from pyspark.sql.session import SparkSession
from pyspark.sql.functions import *
from delta import *
# from delta.tables import *
import pprint
>>>>>>> 6b5bef152908145a6445e177678cabdd74c2caf8
import boto3
import json
import time
import sys
import os

<<<<<<< HEAD
start_time = time.time()
args = sys.argv
subreddit = os.environ["subreddit"]
client = boto3.client("s3")
base = os.getcwd()

subreddit = os.environ["subreddit"]
aws_client = os.environ["AWS_ACCESS_KEY_ID"]
aws_secret = os.environ["AWS_SECRET_ACCESS_KEY"]

spark = SparkSession \
  .builder \
  .master("spark-master:7077") \
  .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \
  .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog") \
  .config("spark.delta.logStore.class", "org.apache.spark.sql.delta.storage.S3SingleDriverLogStore") \
  .config("spark.scheduler.mode", "FAIR") \
  .config("spark.scheduler.allocation.file", "file:///opt/workspace/redditStreaming/fairscheduler.xml") \
  .config("spark.executor.memory", "2048m") \
  .config("spark.executor.cores", "2") \
  .config("spark.streaming.concurrentJobs", "4") \
  .config("spark.local.dir", "/opt/workspace/tmp/driver/{}/".format(subreddit)) \
  .config("spark.worker.dir", "/opt/workspace/tmp/executor/{}/".format(subreddit)) \
  .config("spark.eventLog.enabled", "true") \
  .config("spark.eventLog.dir", "file:///opt/workspace/events/{}/".format(subreddit)) \
  .config("spark.sql.debug.maxToStringFields", 1000) \
  .config("spark.jars.packages", "org.apache.spark:spark-sql-kafka-0-10_2.12:3.2.0,org.apache.hadoop:hadoop-common:3.3.1,org.apache.hadoop:hadoop-aws:3.3.1,org.apache.hadoop:hadoop-client:3.3.1,io.delta:delta-core_2.12:1.2.1") \
  .config("spark.hadoop.fs.s3a.access.key", aws_client) \
  .config("spark.hadoop.fs.s3a.secret.key", aws_secret) \
  .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem") \
  .config('spark.hadoop.fs.s3a.aws.credentials.provider', 'org.apache.hadoop.fs.s3a.SimpleAWSCredentialsProvider') \
  .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \
  .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog") \
  .config("spark.delta.logStore.class", "org.apache.spark.sql.delta.storage.S3SingleDriverLogStore") \
  .enableHiveSupport() \
  .getOrCreate()

sc = spark.sparkContext
sc.setLogLevel("WARN")
# aws_key = ""
# aws_secret = ""
# spark = configure_spark_with_delta_pip(builder).getOrCreate()
=======
args = sys.argv
print("args: {}".format(args))
start_time = time.time()

subreddit = os.environ["subreddit"]

spark = builder = SparkSession \
  .builder \
  .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem") \
  .getOrCreate()
  
# spark = configure_spark_with_delta_pip(builder).getOrCreate()
# .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \
# .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog") \
# .config("spark.delta.logStore.class", "org.apache.spark.sql.delta.storage.S3SingleDriverLogStore") \
>>>>>>> 6b5bef152908145a6445e177678cabdd74c2caf8

df = spark.read.format("delta").option("header", True).load("s3a://reddit-stevenhurwitt/" + subreddit)

df = df.withColumn("approved_at_utc", col("approved_at_utc").cast("timestamp")) \
                .withColumn("banned_at_utc", col("banned_at_utc").cast("timestamp")) \
                .withColumn("created_utc", col("created_utc").cast("timestamp")) \
                .withColumn("created", col("created").cast("timestamp")) \
<<<<<<< HEAD
                .withColumn("year", year(col("date"))) \
                .withColumn("month", month(col("date"))) \
                .withColumn("day", dayofmonth(col("date"))) \
                .withColumn("date", to_date(col("created_utc"), "MM-dd-yyyy")) \
                .dropDuplicates(subset = ["title"])
                
filepath = "s3a://reddit-stevenhurwitt/" + subreddit + "_clean/"
df.write.format("delta") \
    .partitionBy("year", "month", "day") \
    .mode("overwrite") \
    .option("overwriteSchema", True) \
    .option("header", True) \
    .save(filepath)
        
# delta table
# deltaTable = DeltaTable.forPath(spark, "s3a://reddit-stevenhurwitt/{}_clean".format(subreddit))
# deltaTable.vacuum(168)
# deltaTable.generate("symlink_format_manifest")
=======
                .withColumn("date", to_date(col("created_utc"), "MM-dd-yyyy")) \
                .withColumn("year", year(col("date"))) \
                .withColumn("month", month(col("date"))) \
                .withColumn("day", dayofmonth(col("date"))) \
                .dropDuplicates(subset = ["title"])
                
filepath = "s3a://reddit-stevenhurwitt/" + subreddit + "_clean/"
df.write.format("delta").partitionBy("year", "month", "day").mode("overwrite").option("mergeSchema", "true").option("overwriteSchema", "true").option("header", True).save(filepath)
        
deltaTable = DeltaTable.forPath(spark, "s3a://reddit-stevenhurwitt/{}_clean".format(subreddit))
deltaTable.vacuum(168)
deltaTable.generate("symlink_format_manifest")
>>>>>>> 6b5bef152908145a6445e177678cabdd74c2caf8

athena = boto3.client('athena')
athena.start_query_execution(
         QueryString = "MSCK REPAIR TABLE reddit.{}".format(subreddit),
         ResultConfiguration = {
             'OutputLocation': "s3://reddit-stevenhurwitt/_athena_results"
         })

<<<<<<< HEAD
# job.commit()
=======
job.commit()
>>>>>>> 6b5bef152908145a6445e177678cabdd74c2caf8
