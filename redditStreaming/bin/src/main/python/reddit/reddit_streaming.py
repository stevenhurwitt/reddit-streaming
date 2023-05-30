import os
import sys
import ast
import yaml
import json

from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql.types import *

args = getResolvedOptions(sys.argv, ["JOB_NAME"])
sc = SparkContext()
glueContext = GlueContext(sc)
sc.setLogLevel('INFO')
logger = glueContext.get_logger()
logger = logging.getLogger('reddit_streaming')
# spark = glueContext.spark_session
job = Job(glueContext)
job.init(args["JOB_NAME"], args)

secretmanager_client = boto3.client("secretsmanager")

spark_host = "spark-master" 
kafka_host = "kafka" 
subreddit = "cosmos"
spark_version = "3.4.0"
hadoop_version = "3.3.4"
delta_version = "1.2.1"
postgres_version = "52.4.0"
aws_client = ast.literal_eval(secretmanager_client.get_secret_value(SecretId="AWS_ACCESS_KEY_ID")["SecretString"])["AWS_ACCESS_KEY_ID"]
aws_secret = ast.literal_eval(secretmanager_client.get_secret_value(SecretId="AWS_SECRET_ACCESS_KEY")["SecretString"])["AWS_SECRET_ACCESS_KEY"]
extra_jar_list = f"org.apache.spark:spark-sql-kafka-0-10_2.12:{spark_version},org.apache.hadoop:hadoop-common:{hadoop_version},org.apache.hadoop:hadoop-aws:{hadoop_version},org.apache.hadoop:hadoop-client:{hadoop_version},io.delta:delta-core_2.12:{delta_version},org.postgresql:postgresql:{postgres_version}"
bucket = "reddit-streaming-stevenhurwitt-2"

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
            logger.info("read credentials file from {}".format(creds_path))
            f.close()

    except FileNotFoundError as e:
        # print("couldn't find: {}.".format(creds_path))
        try:
            with open(creds_path_container, "r") as f:
                creds = json.load(f)
                logger.warn(e)
                f.close()

        except FileNotFoundError as e:
            with open("/opt/workspace//redditStreaming/creds.json", "r") as f:
                creds = json.load(f)
                logger.warn(e)
                f.close()

    except:
        logger.warn("failed to find creds.json.")
        sys.exit()

    try:
        with open("config.yaml", "r") as f:
            config = yaml.safe_load(f)
            # print("read config file.")\
            logger.info("read config.yaml file.")
            f.close()

    except:
        logger.warn("failed to find config.yaml, exiting now.")
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
    extra_jar_list = config["extra_jar_list"]
    index = 0

    # initialize spark session
    try:
        spark = SparkSession.builder.appName("reddit_{}".format(subreddit)) \
                    .master("spark://{}:7077".format(spark_host)) \
                    .config("spark.scheduler.mode", "FAIR") \
                    .config("spark.scheduler.allocation.file", "file:///opt/workspace/redditStreaming/fairscheduler.xml") \
                    .config("spark.executor.memory", "2048m") \
                    .config("spark.executor.cores", "2") \
                    .config("spark.streaming.concurrentJobs", "8") \
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
        # sc._jsc.hadoopConfiguration().set("fs.s3a.awsAccessKeyId", aws_client)
        # sc._jsc.hadoopConfiguration().set("fs.s3a.awsSecretAccessKey", aws_secret)
        # sc._jsc.hadoopConfiguration().set("fs.s3a.endpoint", "s3.us-east-2.amazonaws.com")
        logger.info("created spark successfully")

    except Exception as e:
        logger.warn(e)

    return(spark, sc)
    
def read_kafka_stream(spark, sc, subreddit):
    """
    reads streaming data from kafka producer

    params: spark, sc
    returns: df
    """
    creds, config = read_files()
    kafka_host = config["kafka_host"]

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
        StructField("link_fla3.2ir_text_color", StringType(), True),
        StructField("upvote_ratio", FloatType(), False),
        StructField("author_flair_background_color", StringType(), True),
        StructField("ups", IntegerType(), False),
        StructField("total_awards_received", IntegerType(), False),
        StructField("thumbnail_width", IntegerType(), True),
        StructField("author_flair_template_id", StringType(), True),
        StructField("is_original_content", BooleanType(), False),
        StructField("secure_media", StringType(), True),
        StructField("is_3.2reddit_media_domain", BooleanType(), False),
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

    logger.info("read spark df.")
    return(df)

def write_stream(df, subreddit):
    """
    writes streaming data to s3 data lake

    params: df
    """

    creds, config = read_files()

    bucket = config["bucket"]
    logger.info("bucket: {}".format(bucket))
    logger.info("subreddit: {}".format(subreddit))
    write_path = f"s3a://{bucket}/{subreddit}"
    logger.info("write path: {}".format(write_path))

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

    logger.info("wrote console df.")

    # write to s3 delta
    df.writeStream \
        .trigger(processingTime="180 seconds") \
        .format("delta") \
        .option("path", write_path) \
        .option("checkpointLocation", "file:///opt/workspace/checkpoints/{}".format(subreddit)) \
        .option("header", True) \
        .outputMode("append") \
        .queryName(subreddit + "_delta") \
        .start()

    logger.info("wrote delta df.")

def main():
    """
    initialize spark, read stream from kafka, write stream to s3 parquet
    """
    creds, config = read_files()
    logger.info("read credentials & config files.")
    # subreddit_list = []
    subreddit_list = config["subreddit"]
    logger.info("got subreddit list.")
    for i, s in enumerate(subreddit_list):
        logger.info("creating spark...")
        spark, sc = init_spark(s, i)

        stage_df = read_kafka_stream(spark, sc, s)
        logger.info("read staging df...")

        try:
            write_stream(stage_df, s)
            logger.info("wrote streaming delta table...")
        
        except KeyboardInterrupt:
            spark.stop
            logger.info("stopping spark gracefully....")
            sys.exit()

    spark.streams.awaitAnyTermination()
    logger.info("all streams stopped successfully.")


if __name__ == "__main__":

    try:
        logger.info("starting spark streaming...")
        main()

    except Exception as e:
        logger.warn(e)
        sys.exit()