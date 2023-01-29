from pyspark.sql import SparkSession
from pyspark.sql.types import *
from pyspark.sql.functions import *
import datetime as dt
from delta import *
import boto3
import pprint
import yaml
import time
import json
import sys
import os

################### database upload ###################
##                                                   ##
##    upload 8 clean tables to postgres db           ##
##                                                   ##
#######################################################


def main():

    pp = pprint.PrettyPrinter(indent = 1)
    print("imported modules.")

    creds_path = os.path.join("/opt", "workspace", "redditStreaming", "creds.json")

    with open(creds_path, "r") as f:
        creds = json.load(f)
        print("read creds.json.")
        f.close()

    # to-do: start spark for all subreddits
    spark_host = "spark-master"
    # spark_host = "spark-master"
    aws_client = creds["aws_client"]
    aws_secret = creds["aws_secret"]
    index = 0
    subreddit = "technology"

    # initialize spark session
    try:
        spark = SparkSession.builder.appName("reddit_{}".format(subreddit)) \
                    .master("spark://{}:7077".format(spark_host)) \
                    .config("spark.scheduler.mode", "FAIR") \
                    .config("spark.scheduler.allocation.file", "file:///opt/workspace/redditStreaming/fairscheduler.xml") \
                    .config("spark.executor.memory", "4096m") \
                    .config("spark.executor.cores", "4") \
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
                    .config('spark.hadoop.fs.s3a.buffer.dir', '/opt/workspace/tmp/blocks') \
                    .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \
                    .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog") \
                    .config("spark.delta.logStore.class", "org.apache.spark.sql.delta.storage.S3SingleDriverLogStore") \
                    .enableHiveSupport() \
                    .getOrCreate()

        sc = spark.sparkContext
        # .config('spark.hadoop.fs.s3a.fast.upload.buffer', 'bytebuffer') \

        sc.setLogLevel('WARN')
        sc.setLocalProperty("spark.scheduler.pool", "pool{}".format(str(index)))
        # sc._jsc.hadoopConfiguration().set("fs.s3a.awsAccessKeyId", aws_client)
        # sc._jsc.hadoopConfiguration().set("fs.s3a.awsSecretAccessKey", aws_secret)
        # sc._jsc.hadoopConfiguration().set("fs.s3a.endpoint", "s3.us-east-2.amazonaws.com")
        print("created spark successfully")

    except Exception as e:
        print(e)

    try:
        df = spark.read.format("delta").option("header", True).load("s3a://reddit-streaming-stevenhurwitt/" + subreddit + "_clean")
        df.show()

    except Exception as e:
        print(e)

    try:
        # jdbc_url = ""
        # jdbc_user = "postgres"
        # jdbc_password = creds["jdbc_password"]
        # df.write.format("jdbc")....
        # print("wrote table to postgres db.")

if __name__ == "__main__":
    main()