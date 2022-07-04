# from awsglue.transforms import *
# from awsglue.utils import getResolvedOptions
# from awsglue.context import GlueContext
# from awsglue.job import Job
from pyspark.context import SparkContext
from pyspark.sql.session import SparkSession
from pyspark.sql.functions import *
from glue_secrets import glue_secrets
from glue_curation_example import glue_curation
import datetime as dt
import pandas as pd
import numpy as np
import datetime as dt
import pandas as pd
import numpy as np
import pyarrow
from delta import *
# from delta.tables import *
import boto3
import json
import time
import sys
import os
print("imported modules.")

# args = getResolvedOptions(sys.argv, ["JOB_NAME"])
args = sys.argv["JOB_NAME"]
start = time.time()
start_datetime = dt.datetime.now()
args_cli = {}
args_cli["JOB_NAME"] = os.environ["JOB_NAME"]

if os.environ["JOB_NAME"] is None:
      args_cli["JOB_NAME"] = "glue_curation_example"

print("job name: {}".format(args_cli["JOB_NAME"]))

spark = SparkSession \
          .builder \
          .appName("glue_curation_example") \
          .master("spark-master:7077") \
          .config("spark.scheduler.mode", "FAIR") \
          .config("spark.scheduler.allocation.file", "file:///opt/workspace/redditStreaming/fairscheduler.xml") \
          .config("spark.executor.memory", "2048m") \
          .config("spark.executor.cores", "1") \
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
print("created spark.")

# glueContext = GlueContext(sc)
# spark = glueContext.spark_session

# job = Job(glueContext)
job = ""
job.init(args["JOB_NAME"], args)

if os.environ["subreddit"] is None:
      subreddit = "AsiansGoneWild"

subreddit = os.environ["subreddit"]
print("subreddit: {}".format(subreddit))

# spark = SparkSession \
#   .builder \
#   .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem") \
#   .getOrCreate()
  
# spark = configure_spark_with_delta_pip(builder).getOrCreate()
# .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \
# .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog") \
# .config("spark.delta.logStore.class", "org.apache.spark.sql.delta.storage.S3SingleDriverLogStore") \

df_raw = spark.read.format("delta").option("header", True).load("s3a://reddit-stevenhurwitt/" + subreddit)

df_clean = df_raw.withColumn("approved_at_utc", col("approved_at_utc").cast("timestamp")) \
                .withColumn("banned_at_utc", col("banned_at_utc").cast("timestamp")) \
                .withColumn("created_utc", col("created_utc").cast("timestamp")) \
                .withColumn("created", col("created").cast("timestamp")) \
                .withColumn("post_date", to_date(col("created_utc"), "MM-dd-yyyy")) \
                .withColumn("year", year(col("post_date"))) \
                .withColumn("month", month(col("post_date"))) \
                .withColumn("day", dayofmonth(col("post_date"))) \
                .dropDuplicates(subset = ["title"])
                
filepath = "s3a://reddit-stevenhurwitt/" + subreddit + "_clean/"
raw_filepath = "s3a://reddit-stevenhurwitt/" + subreddit + "_raw/"

df_clean.write.format("delta").partitionBy("year", "month", "day").mode("overwrite").option("mergeSchema", "true").option("overwriteSchema", "true").option("header", "true").save(filepath)
df_raw.write.format("delta").option("header", "true").option("overwriteSchema", "true").save(raw_filepath)

# deltaTable = DeltaTable.forPath(spark, "s3a://reddit-stevenhurwitt/{}_clean".format(subreddit))
deltaTable = df_clean
deltaTable.vacuum(168)
deltaTable.generate("symlink_format_manifest")

      except Exception as e:
            print("deltaTable statements failed.")
            print(e)
            pass

      try:
            athena = boto3.client('athena')
            athena.start_query_execution(
                  QueryString = "MSCK REPAIR TABLE reddit.{}".format(subreddit),
                  ResultConfiguration = {
                        'OutputLocation': "s3://reddit-stevenhurwitt/_athena_results"
                  })

# job.commit()
end = time.time()
end_datetime = dt.datetime.now()
diff = (end - start)
diff_datetime = (end_datetime - start_datetime)
print("finished job in {} seconds".format(diff))
print("datetime diff: {}".format(diff_datetime))
