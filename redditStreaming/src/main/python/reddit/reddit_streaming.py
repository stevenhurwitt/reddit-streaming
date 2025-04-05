import json
import os
import sys
import ast
import yaml
import json
import logging

import yaml
import datetime as dt
import findspark
findspark.init()
import pyspark
from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import *
from delta import *

# sc = SparkContext()
# sc.setLogLevel('INFO')

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s [%(levelname)s] %(message)s',
    handlers=[
        logging.StreamHandler()
    ]
)

logger = logging.getLogger('reddit_streaming')

logger = logging.getLogger('spark_streaming')

spark_host = "spark-master" 
kafka_host = "kafka" 
subreddit = "aws"
spark_version = "3.5.5"
hadoop_version = "3.3.4"
delta_version = "3.3.0"
postgres_version = "9.4.1212"
# aws_client = ast.literal_eval(secretmanager_client.get_secret_value(SecretId="AWS_ACCESS_KEY_ID")["SecretString"])["AWS_ACCESS_KEY_ID"]
# aws_secret = ast.literal_eval(secretmanager_client.get_secret_value(SecretId="AWS_SECRET_ACCESS_KEY")["SecretString"])["AWS_SECRET_ACCESS_KEY"]
# Update jar list with proper versions
extra_jar_list = ",".join([
    f"org.apache.spark:spark-sql-kafka-0-10_2.12:{spark_version}",
    f"org.apache.kafka:kafka-clients:3.3.1",
    f"io.delta:delta-core_2.12:{delta_version}",
    f"org.apache.hadoop:hadoop-aws:{hadoop_version}"
])
bucket = "reddit-streaming-stevenhurwitt-2"

# Set Java environment variables
os.environ['SPARK_HOME'] = os.path.dirname(pyspark.__file__)
os.environ['JAVA_HOME'] = '/usr/local/openjdk-11/'
os.environ['PYSPARK_PYTHON'] = sys.executable
os.environ['PYSPARK_DRIVER_PYTHON'] = sys.executable
os.environ['SPARK_LOCAL_IP'] = 'localhost'
os.environ['SPARK_LOCAL_DIRS'] = '/opt/workspace/tmp/spark'
os.environ['SPARK_LOG_DIR'] = '/opt/workspace/events'
# os.environ['PYSPARK_SUBMIT_ARGS'] = "--master local[2] pyspark-shell"



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
    raises: Exception if Spark session creation fails
    """
    creds, config = read_files()
    spark_host = config["spark_host"]
    extra_jar_list = config["extra_jar_list"]

    # Get list of local JARs
    # jar_dir = "/opt/workspace/jars"
    # local_jars = ",".join([os.path.join(jar_dir, f) for f in os.listdir(jar_dir) if f.endswith('.jar')])
    
    # Set Java specific configurations
    # os.environ['PYSPARK_PYTHON'] = sys.executable
    # os.environ['PYSPARK_DRIVER_PYTHON'] = sys.executable
    
    
    # initialize spark session
    try:
        spark = SparkSession.builder.appName(f"reddit_{subreddit}") \
                    .master("local[*]") \
                    .config("spark.driver.host", "localhost") \
                    .config("spark.driver.bindAddress", "0.0.0.0") \
                    .config("spark.scheduler.mode", "FAIR") \
                    .config("spark.scheduler.allocation.file", "file:///opt/workspace/redditStreaming/fairscheduler.xml") \
                    .config("spark.driver.memory", "4g") \
                    .config("spark.executor.memory", "4g") \
                    .config("spark.executor.cores", "2") \
                    .config("spark.streaming.concurrentJobs", "8") \
                    .config("spark.local.dir", "/opt/workspace/tmp/spark") \
                    .config("spark.worker.dir", "/opt/workspace/tmp/executor/{}/".format(subreddit)) \
                    .config("spark.eventLog.enabled", "true") \
                    .config("spark.eventLog.dir", "file:///opt/workspace/events") \
                    .config("spark.sql.debug.maxToStringFields", 1000) \
                    .config("spark.jars.packages", extra_jar_list) \
                    .config("spark.hadoop.fs.s3a.access.key", creds["aws_client"]) \
                    .config("spark.hadoop.fs.s3a.secret.key", creds["aws_secret"]) \
                    .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem") \
                    .config("spark.hadoop.fs.s3a.endpoint", "s3.amazonaws.com") \
                    .config("spark.hadoop.fs.s3a.path.style.access", "true") \
                    .config("spark.jars.packages", 
                            "org.apache.hadoop:hadoop-aws:3.3.4," + 
                            "org.apache.hadoop:hadoop-common:3.3.4," +
                            "org.apache.hadoop:hadoop-aws:3.3.4," + 
                            "com.amazonaws:aws-java-sdk-bundle:1.12.261," +
                            "org.apache.logging.log4j:log4j-slf4j-impl:2.17.2," +
                            "org.apache.logging.log4j:log4j-api:2.17.2," +
                            "org.apache.logging.log4j:log4j-core:2.17.2," + 
                            "org.apache.hadoop:hadoop-client:3.3.4," + 
                            "io.delta:delta-core_2.12:3.3.0," + 
                            "org.postgresql:postgresql:42.2.18") \
                    .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \
                    .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog") \
                    .config("spark.delta.logStore.class", "org.apache.spark.sql.delta.storage.S3SingleDriverLogStore") \
                    .enableHiveSupport() \
                    .getOrCreate()

                    # .config("spark.jars.packages", extra_jar_list) \

        sc = spark.sparkContext
        sc.setLogLevel('WARN')
        sc.setLocalProperty("spark.scheduler.pool", "pool{}".format(str(index)))
        
        print("Created Spark session successfully")
        return spark, sc

    except Exception as e:
        print(f"Failed to create Spark session: {str(e)}")
        raise
    
def test_kafka_connection(spark, kafka_host):
    """Test Kafka connection before starting stream"""
    try:
        df = spark.read \
            .format("kafka") \
            .option("kafka.bootstrap.servers", f"{kafka_host}:9092") \
            .option("subscribe", "test_topic") \
            .load()
        return True

    except Exception as e:
        print(f"Failed to connect to Kafka: {str(e)}")
        return False

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

    # Add error handling
    df.writeStream \
        .foreachBatch(lambda df, epoch_id: print(f"Processing batch {epoch_id}")) \
        .start()

    return(df)

def write_stream(df, subreddit):
    """
    writes streaming data to s3 data lake
    
    params: df
    returns: list of streaming queries
    """
    creds, config = read_files()
    bucket = config["bucket"]
    logger.info("subreddit: {}".format(subreddit))
    write_path = f"s3a://{bucket}/{subreddit}"
    logger.info("write path: {}".format(write_path))

    # Store queries in a list
    queries = []

    # write subset of df to console
    console_query = df.withColumn("created_utc", col("created_utc").cast("timestamp")) \
        .select("subreddit", "title", "score", "created_utc") \
        .writeStream \
        .trigger(processingTime='180 seconds') \
        .option("truncate", "true") \
        .option("checkpointLocation", "file:///opt/workspace/checkpoints/{}_console".format(subreddit)) \
        .outputMode("update") \
        .format("console") \
        .queryName(subreddit + "_console") \
        .start()
    
    queries.append(console_query)

    # write to s3 delta
    delta_query = df.writeStream \
        .trigger(processingTime="180 seconds") \
        .format("delta") \
        .option("path", write_path) \
        .option("checkpointLocation", "file:///opt/workspace/checkpoints/{}".format(subreddit)) \
        .option("header", True) \
        .outputMode("append") \
        .queryName(subreddit + "_delta") \
        .start(f"/opt/workspace/data/{subreddit}")
    
    queries.append(delta_query)
    
    return queries


def test_kafka_connection(kafka_host):
    """Test Kafka connectivity"""
    try:
        # Try reading from Kafka to verify connection
        test_df = spark.read \
            .format("kafka") \
            .option("kafka.bootstrap.servers", f"{kafka_host}:9092") \
            .option("subscribe", "test") \
            .option("failOnDataLoss", "false") \
            .load()
        return True

    except Exception as e:
        print(f"Failed to connect to Kafka: {str(e)}")
        return False

def main():
    """
    initialize spark, read stream from kafka, write stream to s3 parquet
    """
    spark = None
    streams = []

    try:
        creds, config = read_files()
        subreddit_list = config["subreddit"]
        extra_jar_list = config["extra_jar_list"]

        # Initialize Delta Lake (add this before creating SparkSession)
        builder = SparkSession.builder \
            .appName("reddit_streaming") \
            .config("spark.jars.packages", extra_jar_list) \
            .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \
            .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")

         # Validate configuration
        if not subreddit_list or not isinstance(subreddit_list, list):
            raise ValueError("Invalid subreddit configuration")
        
        for i, s in enumerate(subreddit_list):
            try:
                print(f"Initializing Spark for subreddit: {s}")
                spark, sc = init_spark(s, i)

                # Test connection before proceeding
                if not test_kafka_connection(kafka_host):
                    raise Exception("Failed to connect to Kafka")
                    
                stage_df = read_kafka_stream(spark, sc, s)
                streams.extend(write_stream(stage_df, s))

            except Exception as e:
                print(f"Error processing subreddit {s}: {str(e)}")
                continue

        if not streams:
            raise Exception("No streams were successfully created")

        print("All streams initialized, awaiting termination...")
        spark.streams.awaitAnyTermination()

        # Only await termination if at least one stream was created
        if 'spark' in locals():
            spark.streams.awaitAnyTermination()
        else:
            print("No streams were successfully created")
            sys.exit(1)
            
    except KeyboardInterrupt:
        print("\nShutting down gracefully...")
        for stream in streams:
            stream.stop()
        if 'spark' in locals():
            spark.stop()
        sys.exit(0)

    except Exception as e:
        print(f"Fatal error: {str(e)}")
        if 'spark' in locals():
            spark.stop()
        sys.exit(1)

    finally:
        for stream in streams:
            try:
                stream.stop()
            except:
                pass
        if spark:
            try:
                spark.stop()
            except:
                pass

if __name__ == "__main__":

    try:
        print("starting spark streaming...")
        main()

    except Exception as e:
        print(e)
        sys.exit()