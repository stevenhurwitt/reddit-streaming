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

    creds_path_container = os.path.join("/opt", "workspace", "redditStreaming", "creds.json")

    base = os.getcwd()
    # creds_dir = "/".join(base.split("/")[:-3])
    creds_dir = os.path.join("/Users", "stevenhurwitt", "Documents", "reddit-streaming", "redditStreaming")
    creds_path = os.path.join(creds_dir, "creds.json")

    try:
        with open(creds_path, "r") as f:
            creds = json.load(f)
            f.close()

    except FileNotFoundError:
        # print("couldn't find: {}.".format(creds_path))
        try:
            with open(creds_path_container, "r") as f:
                creds = json.load(f)
                f.close()

        except FileNotFoundError:
            with open("/home/pi/Documents/reddit-streaming/redditStreaming/creds.json", "r") as f:
                creds = json.load(f)
                f.close()

    except:
        print("failed to find creds.json.")
        sys.exit()

    try:
        with open("config.yaml", "r") as f:
            config = yaml.safe_load(f)

    except:
        print("failed to find config.yaml")
        sys.exit()

    return(creds, config)

def init_spark():
    """
    initialize spark given config and credential's files

    returns: spark, sparkContext (sc)
    """
    creds, config = read_files()
    subreddit = config["subreddit"]
    kafka_host = config["kafka_host"]
    spark_host = config["spark_host"]
    aws_client = creds["aws-client"]
    aws_secret = creds["aws-secret"]

    # initialize spark session
    try:
        spark = SparkSession.builder.appName("reddit_" + subreddit) \
                    .master("spark://{}:7077".format(spark_host)) \
                    .config("spark.eventLog.enabled", "true") \
                    .config("spark.eventLog.dir", "file:///opt/workspace/events") \
                    .config("spark.sql.debug.maxToStringFields", 1000) \
                    .config("spark.jars.packages", "org.apache.spark:spark-sql-kafka-0-10_2.12:3.1.0,org.apache.hadoop:hadoop-common:3.3.1,org.apache.hadoop:hadoop-aws:3.3.1,org.apache.hadoop:hadoop-client:3.3.1,io.delta:delta-core_2.12:1.2.1") \
                    .config("spark.hadoop.fs.s3a.access.key", aws_client) \
                    .config("spark.hadoop.fs.s3a.secret.key", aws_secret) \
                    .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem") \
                    .config('spark.hadoop.fs.s3a.aws.credentials.provider', 'org.apache.hadoop.fs.s3a.SimpleAWSCredentialsProvider') \
                    .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \
                    .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog") \
                    .enableHiveSupport() \
                    .getOrCreate()
        sc = spark.sparkContext

        sc.setLogLevel('WARN')
        # sc._jsc.hadoopConfiguration().set("fs.s3a.awsAccessKeyId", aws_client)
        # sc._jsc.hadoopConfiguration().set("fs.s3a.awsSecretAccessKey", aws_secret)
        # sc._jsc.hadoopConfiguration().set("fs.s3a.endpoint", "s3.us-east-2.amazonaws.com")
        print("created spark successfully")

    except Exception as e:
        print(e)

    return(spark, sc)
    
def read_kafka_stream(spark, sc):
    """
    reads streaming data from kafka producer

    params: spark, sc
    returns: df
    """
    creds, config = read_files()
    subreddit = config["subreddit"]
    kafka_host = config["kafka_host"]
    spark_host = config["spark_host"]
    aws_client = creds["aws-client"]
    aws_secret = creds["aws-secret"]

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
                .load() \
                .selectExpr("CAST(value AS STRING) as json") \
                .select(from_json(col("json"), payload_schema).alias("data")) \
                .select("data.*") 

    return(df)

def write_stream(df):
    """
    writes streaming data to s3 data lake

    params: df
    """
    creds, config = read_files()
    subreddit = config["subreddit"]
    kafka_host = config["kafka_host"]
    spark_host = config["spark_host"]
    aws_client = creds["aws-client"]
    aws_secret = creds["aws-secret"]

    # write to console
    # df.writeStream \
    #     .trigger(processingTime='30 seconds') \
    #     .outputMode("update") \
    #     .format("console") \
    #     .option("truncate", "true") \
    #     .start() \
    #     .awaitTermination()   

    # write to s3 delta
    df.writeStream \
        .trigger(processingTime="30 seconds") \
        .format("delta") \
        .option("path", "s3a://reddit-stevenhurwitt/" + subreddit) \
        .option("checkpointLocation", "file:///opt/workspace/checkpoints") \
        .option("header", True) \
        .outputMode("append") \
        .start() \
        .awaitTermination()

    # test writing to csv
    # df.writeStream \
    #     .trigger(processingTime="30 seconds") \
    #     .format("csv") \
    #     .option("path", "s3a://reddit-stevenhurwitt/test_technology") \
    #     .option("checkpointLocation", "file:///opt/workspace/checkpoints") \
    #     .option("header", True) \
    #     .outputMode("append") \
    #     .start() \
    #     .awaitTermination()

def main():
    """
    initialize spark, read stream from kafka, write stream to s3 parquet
    """

    spark, sc = init_spark()

    stage_df = read_kafka_stream(spark, sc)

    try:
        write_stream(stage_df)
    
    except KeyboardInterrupt:
        spark.stop
        sys.exit()

if __name__ == "__main__":

    print("starting spark streaming...")
    main()