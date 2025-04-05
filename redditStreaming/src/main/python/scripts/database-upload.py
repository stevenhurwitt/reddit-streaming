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
import ast
import os

################### database upload ###################
##                                                   ##
##    upload 8 clean tables to postgres db           ##
##                                                   ##
#######################################################


def main(subreddit):

    pp = pprint.PrettyPrinter(indent = 1)
    secretmanager_client = boto3.client("secretsmanager", region_name = "us-east-2")

    aws_client = ast.literal_eval(secretmanager_client.get_secret_value(SecretId="AWS_ACCESS_KEY_ID")["SecretString"])["AWS_ACCESS_KEY_ID"]
    aws_secret = ast.literal_eval(secretmanager_client.get_secret_value(SecretId="AWS_SECRET_ACCESS_KEY")["SecretString"])["AWS_SECRET_ACCESS_KEY"]
    
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
    extra_jar_list = creds["extra_jar_list"]
    index = 0
    # subreddit = "technology"

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
                    .config("spark.jars.packages", extra_jar_list) \
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
        print("created spark successfully")

    except Exception as e:
        print(e)

    try:
        df = spark.read.format("delta").option("header", True).load("s3a://reddit-streaming-stevenhurwitt-new/" + subreddit + "_clean")
        df.show()

    except Exception as e:
        print(e)

    try:
        db_creds = ast.literal_eval(secretmanager_client.get_secret_value(SecretId="dev/reddit/postgres")["SecretString"])
        connect_str = "jdbc:postgresql://{}:{}/{}".format(db_creds["host"], db_creds["port"], db_creds["dbname"])
        pp.pprint(db_creds)

        try:
            df.write.format("jdbc") \
                .mode("overwrite") \
                .option("url", connect_str) \
                .option("dbtable", "reddit.{}".format(subreddit)) \
                .option("user", db_creds["username"]) \
                .option("password", db_creds["password"]) \
                .option("driver", "org.postgresql.Driver") \
                .save()

            print("wrote df to postgresql table.")

        except Exception as e:
            print(e)
        # jdbc_url = ""
        # jdbc_user = "postgres"
        # jdbc_password = creds["jdbc_password"]
        # df.write.format("jdbc")....
        # print("wrote table to postgres db.")

    except Exception as e:
        print(e)

if __name__ == "__main__":

    pp = pprint.PrettyPrinter(indent = 1)

    with open("./../config.yaml", "r") as h:
        config = yaml.safe_load(h)
        h.close()

        for s in config["subreddit"]:
            print(s)
            main(s)