from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import *
import datetime as dt
import pandas as pd
import numpy as np
import pyarrow
import boto3
import json
import time
import sys
import os

def main():
    # initialize time modules.
    start = time.time()
    start_datetime = dt.datetime.now()
    base = os.getcwd()
    print("working in directory: {}".format(base))

    # aws boto3 clients
    try:
        print("begin aws authentication.")
        secrets = boto3.client("secretsmanager", region_name = "us-east-2")
        s3 = boto3.client("s3", region_name = "us-east-2")
        athena = boto3.client("athena", region_name = "us-east-2")
        glue = boto3.client("glue", region_name = "us-east-2")
        ec2 = boto3.client("ec2", region_name = "us-east-2")
        rds = boto3.client("rds", region_name = "us-east-2")
        print("initialized boto3 clients.")

    except Exception as e:
        secrets = None
        s3 = None
        athena = None
        print(e)
        pass

    if secrets is not None:
        # aws secret manager
        print("begin aws secret manager.")
        # my_subreddit = secrets.get_secret_value(SecretId = "subreddit")
        my_client = secrets.get_secret_value(SecretId = "AWS_ACCESS_KEY_ID")
        my_secret = secrets.get_secret_value(SecretId = "AWS_SECRET_ACCESS_KEY")
        # subreddit = my_subreddit["SecretString"]
        aws_client = my_client["SecretString"]
        aws_secret = my_secret["SecretString"]
        subreddit = os.environ["subreddit"]
        print("subreddit: {}".format(subreddit))
        print("aws_client: {}".format(aws_client))
        print("secret key length (n) = {}.".format(len(aws_secret)))
    
    else:
        raise TypeError("secrets is {}".format(type(secrets)))
        pass

    # s3 buckets
    print("begin s3 buckets.")
    my_buckets = s3.list_buckets()
    print("my s3 buckets: {}".format(my_buckets))

    # aws clients
    print("athena aws client \n {}".format(athena))
    print("glue aws client \n {}".format(glue))
    print("ec2 aws client \n {}".format(ec2))
    print("rds aws client: \n {}".format(rds))

    # creds.json
    print("begin creds.json.")
    with open("creds.json", "r") as f:
        creds = json.load(f)
        f.close()
        print(creds)

    # spark session
    spark = SparkSession \
                .builder \
                .master("{}:7077".format(creds["host"])) \
                .appName("redditStreaming") \
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

    # target = "redditStreaming/src/data/"
    # print(os.listdir(target))

    local_raw = "file://redditStreaming/src/data/raw/{}".format(subreddit)
    local_clean = "file://redditStreaming/src/data/clean/{}".format(subreddit)

    read_raw_file = "s3://reddit-stevenhurwitt/{}/".format(subreddit)
    read_clean_file = "s3://reddit-stevenhurwitt/{}_clean/".format(subreddit)

    raw = spark.read.format("delta").option("header", "true").load(read_raw_file)
    clean = spark.read.format("delta").option("header", "true").load(read_clean_file)

    raw.write.format("delta").mode("overwrite").save(local_raw)
    clean.write.format("delta").mode("overwrite").save(local_clean)
    print("wrote df to spark delta tables.")

    # quit
    sys.exit()

if __name__ == "__main__":
    print("starting main...")
    main()
    print("done.")
    sys.exit()