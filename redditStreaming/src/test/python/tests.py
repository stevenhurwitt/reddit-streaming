from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import *
import datetime as dt
import boto3
import pytest
import json
import time
import sys
import os

def test():
    base = os.getcwd()
    print("working directory: {}".format("base"))
    start = time.time()
    start_dt = dt.datetime.now()

    print("adding to sys path...")
    sys.path.append(base + "/src/test/python")
    sys.path.append(base + "/src/test/scala")
    sys.path.append(base + "/src/test/python/test")
    sys.path.append(base + "/src/test/scala/test/")
    sys.path.append(base + "/src/test/python/test/test_reddit_streaming")
    sys.path.append(base + "/src/test/scala/test/test_reddit_streaming")

    print("imported modules.")

    os.environ["subreddit"] = "technology"
    subreddit = os.environ["subreddit"]

    secrets = boto3.client("secretmanager", region_name="us-east-2")
    os.environ["AWS_ACCESS_KEY_ID"] = secrets.get_secret_value(SecretId = aws_client)
    os.environ["AWS_SECRET_ACCESS_KEY"] = secrets.get_secret_value(SecretId = aws_secret)
    aws_client = os.environ["AWS_ACCESS_KEY_ID"]
    aws_secret = os.environ["AWS_SECRET_ACCESS_KEY"]

    print("subreddit: {}".format(os.environ["subreddit"]))

    with open("creds.json", "r") as f:
        creds = json.load(f)
        f.close()
        print("creds: {}".format(creds))

    spark_host = creds["spark_host"]
    kafka_host = creds["kafka_host"]
    extra_jar_list = ["org.apache.spark:spark-sql-kafka-0-10_2.12:3.2.0,org.apache.hadoop:hadoop-common:3.3.1,org.apache.hadoop:hadoop-aws:3.3.1,org.apache.hadoop:hadoop-client:3.3.1,io.delta:delta-core_2.12:1.2.1"]

    print("attempting spark session...")
    spark = SparkSession \
            .builder \
            .appName("tests") \
            .master("spark://{}:7077".format(spark_host)) \
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
            .config("spark.jars.packages", extra_jar_list[0]) \
            .config("spark.hadoop.fs.s3a.access.key", aws_client) \
            .config("spark.hadoop.fs.s3a.secret.key", aws_secret) \
            .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem") \
            .config('spark.hadoop.fs.s3a.aws.credentials.provider', 'org.apache.hadoop.fs.s3a.SimpleAWSCredentialsProvider') \
            .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \
            .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog") \
            .config("spark.delta.logStore.class", "org.apache.spark.sql.delta.storage.S3SingleDriverLogStore") \
            .enableHiveSupport() \
            .getOrCreate()

    print("spark session created.")

    # read df
    bucket = "reddit-stevenhurwitt"
    folder = subreddit
    clean_folder = subreddit + "_clean"
    filepath = "s3a://{}/{}/".format(bucket, folder)
    clean_filepath = "s3a://{}/{}/".format(bucket, clean_folder)
    df = spark.read.format("delta").option("header", "true").load(filepath)
    df.show()

    # write df to s3 delta tables
    df.write.format("delta").mode("overwrite").option("header", "true").option("overwriteSchema", "true").save(clean_filepath)
    print("wrote df's to s3 delta tables.")

    # write df locally
    local_filepath = "file://opt/workspace/redditStreaming/data/raw/{}/".format(subreddit)
    local_clean_filepath = "file://opt/workspace/redditStreaming/data/clean/{}/".format(subreddit)
    df.write.format("delta").mode("overwrite").option("header", "true").option("overwriteSchema", "true").save(local_filepath)
    df.write.format("delta").mode("overwrite").option("header", "true").option("overwriteSchema", "true").save(local_clean_filepath)
    print("wrote df's to local delta tables.")

    # write df console
    print("writing df to console: ")
    df.write.format("console").mode("append").option("truncate", "true").option("header", "true").start()

    # other??

    # timer
    end = time.time()
    end_dt = dt.datetime.now()

    diff = (end - start)
    diff_dt = (end_dt - start_dt)
    print("time: {}".format(diff))
    print("datetime diff: {}".format(diff_dt))


if __name__ == "__main__":
    print("running tests...")
    test()
    print("tests completed.")



