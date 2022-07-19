# from awsglue.transforms import *
# from awsglue.utils import getResolvedOptions
# from awsglue.context import GlueContext
# from awsglue.job import Job
# from delta.tables import *
from pyspark.context import SparkContext
from pyspark.sql.session import SparkSession
from pyspark.sql.functions import *
from glue_secrets import glue_secrets
from glue_curation_example import glue_curation
import datetime as dt
import pandas as pd
import numpy as np
from delta import *
# from delta.tables import *
import pprint
import boto3
import json
import time
import sys
import os
print("imported modules.")

def glue_curation(event, context):

      # check context
      if context is not None:
            print(context)

      # read secrets
      creds = glue_secrets()
      # {
      #    "AWS_ACCESS_KEY_ID": "...",
      #    "AWS_SECRET_ACCESS_KEY": "...",
      #    "subreddit": "..."
      # }

      # try to set initial variables
      try:
            # args = getResolvedOptions(sys.argv, ["JOB_NAME"])
            args = sys.argv["JOB_NAME"]
            args_cli = {}
            start = time.time()
            start_datetime = dt.datetime.now()
            args_cli["JOB_NAME"] = os.environ["JOB_NAME"]
            subreddit = os.environ["subreddit"]
            aws_client = os.environ["AWS_ACCESS_KEY_ID"]
            aws_secret = os.environ["AWS_SECRET_ACCESS_KEY"]
            extra_jar_list = "org.apache.spark:spark-sql-kafka-0-10_2.12:3.2.0,org.apache.hadoop:hadoop-common:3.3.1,org.apache.hadoop:hadoop-aws:3.3.1,org.apache.hadoop:hadoop-client:3.3.1,io.delta:delta-core_2.12:1.2.1"

      except Exception as e:
            print("failed to set environment variables.")
            print(e)
            pass

      # boto3 secrets manager
      secrets = boto3.client("secretsmanager", region_name="us-east-2")
      my_creds = secrets.get_secret_value(SecretId = "reddit-stevenhurwitt-creds")
      my_id = secrets.get_secret_value(SecretId = "AWS_ACCESS_KEY_ID")
      my_key = secrets.get_secret_value(SecretId = "AWS_SECRET_ACCESS_KEY")
      print(my_creds)
      print(my_id)
      print(my_key)
      print("read aws secrets.")

      # get job name
      if os.environ["JOB_NAME"] is None:
            args_cli["JOB_NAME"] = "glue_curation_example"

      print("job name: {}".format(args_cli["JOB_NAME"]))

      # spark
      spark = SparkSession \
            .builder \
            .appName("glue_curation_example") \
            .master("spark-master:7077") \
            .config("spark.scheduler.mode", "FAIR") \
            .config("spark.scheduler.allocation.file", "~/redditStreaming/fairscheduler.xml") \
            .config("spark.executor.memory", "2048m") \
            .config("spark.executor.cores", "1") \
            .config("spark.streaming.concurrentJobs", "4") \
            .config("spark.local.dir", "~/tmp/driver/{}/".format(subreddit)) \
            .config("spark.worker.dir", "~/tmp/executor/{}/".format(subreddit)) \
            .config("spark.eventLog.enabled", "true") \
            .config("spark.eventLog.dir", "~/events/{}/".format(subreddit)) \
            .config("spark.sql.debug.maxToStringFields", 1000) \
            .config("spark.jars.packages", extra_jar_list) \
            .config("spark.hadoop.fs.s3a.access.key", aws_client) \
            .config("spark.hadoop.fs.s3a.secret.key", aws_secret) \
            .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem") \
            .config('spark.hadoop.fs.s3a.aws.credentials.provider', 'org.apache.hadoop.fs.s3a.SimpleAWSCredentialsProvider') \
            .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \
            .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog") \
            .config("spark.delta.logStore.class", "org.apache.spark.sql.delta.storage.S3SingleDriverLogStore") \
            .enableHiveSupport() \
            .getOrCreate()

      # spark context
      sc = spark.sparkContext
      sc.setLogLevel("WARN")
      print("created spark.")

      # glue context
      # glueContext = GlueContext(sc)
      # job = Job(glueContext)
      # spark = glueContext.spark_session

      # glue job metadata
      job = ""
      job.init(args["JOB_NAME"], args)

      if os.environ["subreddit"] is None:
            subreddit = "AsiansGoneWild"

      subreddit = os.environ["subreddit"]
      print("subreddit: {}".format(subreddit))

      # read df
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

      try:
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

      except Exception as e:
            print("athena statements failed.")
            print(e)
            pass

      # job.commit()
      end = time.time()
      end_datetime = dt.datetime.now()
      diff = (end - start)
      diff_datetime = (end_datetime - start_datetime)
      print("finished job in {} seconds".format(diff))
      print("datetime diff: {}".format(diff_datetime))

if __name__ == "__main__":
      print("starting glue curation...")
      with open("glue.json", "r") as f:
            glue_args = json.load(f)
            f.close()

      glue_curation(glue_args, None)
