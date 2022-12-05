from pyspark.sql import SparkSession
from pyspark.sql.types import *
from pyspark.sql.functions import *
import yaml
import json
import sys
import os

def read_files():
    """
    initializes spark session using config.yaml and creds.json files.
    """

    base = os.getcwd()
    creds_path_container = os.path.join(base, "creds.json")

    creds_dir = "/".join(base.split("/")[:-3])
    creds_path = os.path.join(base, "creds.json")

    try:
        with open(creds_path, "r") as f:
            creds = json.load(f)
            print("read creds.json.")
            f.close()

    except FileNotFoundError:
        # print("couldn't find: {}.".format(creds_path))
        try:
            with open(creds_path_container, "r") as f:
                creds = json.load(f)
                f.close()

        except FileNotFoundError:
            with open("/opt/workspace//redditStreaming/creds.json", "r") as f:
                creds = json.load(f)
                f.close()

    except:
        print("failed to find creds.json.")
        sys.exit()

    try:
        with open("config.yaml", "r") as f:
            config = yaml.safe_load(f)
            # print("read config file.")
            f.close()

    except:
        print("failed to find config.yaml, exiting now.")
        sys.exit()

    return(creds, config)

def init_spark(subreddit, index):
    """
    initialize spark given config and credential's files

    returns: spark, sparkContext (sc)
    """
    creds, config = read_files()
    spark_host = config["spark_host"]
    # spark_host = "spark-master"
    aws_client = creds["aws_client"]
    aws_secret = creds["aws_secret"]
    index = 0

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

    return(spark, sc)
    
def read_kafka_stream(spark, sc, subreddit):
    """
    reads streaming data from kafka producer

    params: spark, sc
    returns: df
    """
    creds, config = read_files()
    kafka_host = config["kafka_host"]
    spark_host = config["spark_host"]
    aws_client = creds["aws_client"]
    aws_secret = creds["aws_secret"]

    # define schema for payload data
    payload_schema = StructType([
        StructField("approved_at_utc", FloatType(), True),
        StructField("subreddit", StringType(), False),
        StructField("selftext", StringType(), False),
        StructField("author_fullname", StringType(), False),
        StructField("saved", BooleanType(), False),
        StructField("mod_reason_title", StringType(), True),
        StructField("gilded", IntegerType(), False),
        StructField("clicked", BooleanType(), False),
        StructField("title", StringType(), False),
        StructField("subreddit_name_prefixed", StringType(), False),
        StructField("hidden", BooleanType(), False),
        StructField("pwls", IntegerType(), False),
        StructField("link_flair_css_class", StringType(), False),
        StructField("downs", IntegerType(), False),
        StructField("thumbnail_height", IntegerType(), True),
        StructField("top_awarded_type", StringType(), True),
        StructField("hide_score", BooleanType(), False),
        StructField("name", StringType(), False),
        StructField("quarantine", BooleanType(), False),
        StructField("link_flair_text_color", StringType(), True),
        StructField("upvote_ratio", FloatType(), False),
        StructField("author_flair_background_color", StringType(), True),
        StructField("ups", IntegerType(), False),
        StructField("total_awards_received", IntegerType(), False),
        StructField("thumbnail_width", IntegerType(), True),
        StructField("author_flair_template_id", StringType(), True),
        StructField("is_original_content", BooleanType(), False),
        StructField("secure_media", StringType(), True),
        StructField("is_reddit_media_domain", BooleanType(), False),
        StructField("is_meta", BooleanType(), False),
        StructField("category", StringType(), True),
        StructField("link_flair_text", StringType(), True),
        StructField("can_mod_post", BooleanType(), False),
        StructField("score", IntegerType(), False),
        StructField("approved_by", StringType(), True),
        StructField("is_created_from_ads_ui", BooleanType(), False),
        StructField("author_premium", BooleanType(), False),
        StructField("thumbnail", StringType(), True),
        StructField("edited", BooleanType(), False),
        StructField("author_flair_css_class", StringType(), True),
        StructField("post_hint", StringType(), False),
        StructField("content_categories", StringType(), True),
        StructField("is_self", BooleanType(), False),
        StructField("subreddit_type", StringType(), False),
        StructField("created", FloatType(), False),
        StructField("link_flair_type", StringType(), True),
        StructField("wls", IntegerType(), False),
        StructField("removed_by_category", StringType(), True),
        StructField("banned_by", StringType(), True),
        StructField("author_flair_type", StringType(), True),
        StructField("domain", StringType(), True),
        StructField("allow_live_comments", BooleanType(), False),
        StructField("selftext_html", StringType(), True),
        StructField("likes", IntegerType(), True),
        StructField("suggested_sort", StringType(), True),
        StructField("banned_at_utc", FloatType(), True),
        StructField("url_overridden_by_dest", StringType(), True),
        StructField("view_count", IntegerType(), True),
        StructField("archived", BooleanType(), False),
        StructField("no_follow", BooleanType(), False),
        StructField("is_crosspostable", BooleanType(), False),
        StructField("pinned", BooleanType(), False),
        StructField("over_18", BooleanType(), False),
        StructField("media_only", BooleanType(), False),
        StructField("link_flair_template_id", StringType(), True),
        StructField("can_gild", BooleanType(), False),
        StructField("spoiler", BooleanType(), False),
        StructField("locked", BooleanType(), False),
        StructField("author_flair_text", StringType(), True),
        StructField("visited", BooleanType(), False),
        StructField("removed_by", StringType(), True),
        StructField("mod_note", StringType(), True),
        StructField("distinguished", StringType(), True),
        StructField("subreddit_id", StringType(), False),
        StructField("author_is_blocked", BooleanType(), False),
        StructField("mod_reason_by", StringType(), True),
        StructField("num_reports", IntegerType(), True),
        StructField("removal_reason", StringType(), True),
        StructField("link_flair_background_color", StringType(), True),
        StructField("id", StringType(), False),
        StructField("is_robot_indexable", BooleanType(), False),
        StructField("report_reasons", StringType(), True),
        StructField("author", StringType(), False),
        StructField("discussion_type", StringType(), True),
        StructField("num_comments", IntegerType(), False),
        StructField("send_replies", BooleanType(), False),
        StructField("whitelist_status", StringType(), False),
        StructField("contest_mode", BooleanType(), False),
        StructField("author_patreon_flair", BooleanType(), False),
        StructField("author_flair_text_color", StringType(), True),
        StructField("permalink", StringType(), False),
        StructField("parent_whitelist_status", StringType(), False),
        StructField("stickied", BooleanType(), False),
        StructField("url", StringType(), False),
        StructField("subreddit_subscribers", IntegerType(), False),
        StructField("created_utc", FloatType(), False),
        StructField("num_crossposts", IntegerType(), False),
        StructField("media", StringType(), True),
        StructField("is_video", BooleanType(), False),
    ])

    # read json from kafka and select all columns
    df = spark \
            .readStream \
                .format("kafka") \
                .option("kafka.bootstrap.servers", "{}:9092".format(kafka_host)) \
                .option("subscribe", "reddit_" + subreddit) \
                .option("startingOffsets", "latest") \
                .option("failOnDataLoss", "false") \
                .load() \
                .selectExpr("CAST(value AS STRING) as json") \
                .select(from_json(col("json"), payload_schema).alias("data")) \
                .select("data.*") 

    return(df)

def write_stream(df, subreddit):
    """
    writes streaming data to s3 data lake

    params: df
    """

    # write subset of df to console
    df.withColumn("created_utc", col("created_utc").cast("timestamp")) \
        .select("subreddit", "title", "score", "created_utc") \
        .writeStream \
        .trigger(processingTime='180 seconds') \
        .option("truncate", "true") \
        .option("checkpointLocation", "file:///opt/workspace/checkpoints/{}_console".format(subreddit)) \
        .outputMode("update") \
        .format("console") \
        .queryName(subreddit + "_console") \
        .start()

    # write to s3 delta
    df.writeStream \
        .trigger(processingTime="180 seconds") \
        .format("delta") \
        .option("path", "s3a://reddit-streaming-stevenhurwitt/{}".format(subreddit)) \
        .option("checkpointLocation", "file:///opt/workspace/checkpoints/{}".format(subreddit)) \
        .option("header", True) \
        .outputMode("append") \
        .queryName(subreddit + "_delta") \
        .start()

def main():
    """
    initialize spark, read stream from kafka, write stream to s3 parquet
    """
    creds, config = read_files()
    # subreddit_list = []
    subreddit_list = config["subreddit"]
    for i, s in enumerate(subreddit_list):
        spark, sc = init_spark(s, i)

        stage_df = read_kafka_stream(spark, sc, s)

        try:
            write_stream(stage_df, s)
        
        except KeyboardInterrupt:
            spark.stop
            sys.exit()

    spark.streams.awaitAnyTermination()

    

if __name__ == "__main__":

    try:
        print("starting spark streaming...")
        main()

    except Exception as e:
        print(e)
        sys.exit()