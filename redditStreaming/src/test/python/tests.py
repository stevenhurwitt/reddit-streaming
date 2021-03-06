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

def test_aws_creds():

    # set environment variables
    os.environ["subreddit"] = "technology"
    os.environ["AWS_ACCESS_KEY_ID"] = "AKIA6BTEPFALMZ7ZHAYU"
    os.environ["AWS_SECRET_ACCESS_KEY"] = "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"

    # set local vars
    # subreddit = os.environ["subreddit"]
    # aws_client = os.environ["AWS_ACCESS_KEY_ID"]
    # aws_secret = os.environ["AWS_SECRET_ACCESS_KEY"]
    subreddit = "technology"
    aws_client = "AKIA6BTEPFALMZ7ZHAYU"
    aws_secret = "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"

    # aws
    secrets = boto3.client("secretsmanager", region_name = "us-east-2")
    # client = secrets.getSecretValue("AWS_ACCESS_KEY_ID")
    # secret = secrets.getSecretValue("AWS_SECRET_ACCESS_KEY")
    # print("client: {}, secret: {}.".format(client, secret))
    assert True == True


def test_spark_session():
    base = os.getcwd()

    # add to path
    sys.path.append(base + "/src/test/python")
    sys.path.append(base + "/src/test/python/test/test_reddit_streaming")

    # set environment variables
    os.environ["subreddit"] = "technology"
    aws_client = "AKIA6BTEPFALMZ7ZHAYU"
    aws_secret = "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"
 
    # set local vars
    subreddit = os.environ["subreddit"]
    print("subreddit: {}".format(os.environ["subreddit"]))

    spark_host = "spark-master"
    kafka_host = "kafka"
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
    assert str(type(spark)) == "<class 'pyspark.sql.session.SparkSession'>"
    # return(spark, subreddit)


def read_raw_s3(spark, subreddit):
    # set s3 filepaths
    bucket = "reddit-stevenhurwitt"
    folder = subreddit
    filepath = "s3a://{}/{}/".format(bucket, folder)

    # read raw df
    df = spark.read.format("delta").option("header", "true").load(filepath)
    # df.show()

    assert df.count() > 0
    # return(df)

def read_clean_s3(spark, subreddit):
    # set s3 filepaths
    bucket = "reddit-stevenhurwitt"
    clean_folder = subreddit + "_clean"
    clean_filepath = "s3a://{}/{}/".format(bucket, clean_folder)

    # read clean df
    df_clean = spark.read.format("delta").option("header", "true").load(clean_filepath)
    # df_clean.show()

    assert df_clean.count() > 0
    # return(df_clean)


def write_console(df):
    # write df to console
    df.show()
    assert True == True

def main():

    print("running tests...")

    test_aws_creds()

    spark, subreddit = test_spark_session()

    df_raw = read_raw_s3(spark, subreddit)

    df_clean = read_clean_s3(spark, subreddit)

    write_console(df_raw)

    write_console(df_clean)

    print("tests completed.")


if __name__ == "__main__":

    main()