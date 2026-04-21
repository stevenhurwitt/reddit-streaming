import json
import logging
import os
import sys

import boto3
from awsglue.context import GlueContext
from awsglue.job import Job
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from delta import *
from delta.tables import *
from pyspark.context import SparkContext
from pyspark.sql.functions import *
from pyspark.sql.session import SparkSession

# Configure logging
logger = logging.getLogger()
logger.setLevel(logging.INFO)

args = getResolvedOptions(sys.argv, ["JOB_NAME"])
sc = SparkContext()
glueContext = GlueContext(sc)
job = Job(glueContext)
job.init(args["JOB_NAME"], args)

subreddit = "technology"
bucket = os.environ.get("BUCKET_NAME", "reddit-streaming-stevenhurwitt-2")

# Get AWS credentials from Secrets Manager
secretmanager_client = boto3.client("secretsmanager", region_name="us-east-1")

try:
    aws_client = json.loads(
        secretmanager_client.get_secret_value(SecretId="AWS_ACCESS_KEY_ID")["SecretString"]
    )["AWS_ACCESS_KEY_ID"]
    aws_secret = json.loads(
        secretmanager_client.get_secret_value(SecretId="AWS_SECRET_ACCESS_KEY")["SecretString"]
    )["AWS_SECRET_ACCESS_KEY"]
    logger.info("Successfully retrieved AWS credentials from Secrets Manager")
except Exception as e:
    logger.error(f"Failed to retrieve credentials from Secrets Manager: {str(e)}")
    raise

# Spark JAR packages with updated versions
extra_jar_list = "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.3,org.apache.hadoop:hadoop-common:3.3.4,org.apache.hadoop:hadoop-aws:3.3.4,org.apache.hadoop:hadoop-client:3.3.4,io.delta:delta-core_2.12:3.2.0,org.postgresql:postgresql:42.6.0"

spark = SparkSession.builder \
    .config("spark.jars.packages", extra_jar_list) \
    .config("spark.hadoop.fs.s3a.access.key", aws_client) \
    .config("spark.hadoop.fs.s3a.secret.key", aws_secret) \
    .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem") \
    .config("spark.hadoop.fs.s3a.aws.credentials.provider", "org.apache.hadoop.fs.s3a.SimpleAWSCredentialsProvider") \
    .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \
    .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog") \
    .enableHiveSupport() \
    .getOrCreate()

logger.info("Spark session created successfully")

try:
    # Read Delta table from S3
    logger.info(f"Reading Delta table from s3a://{bucket}/{subreddit}")
    df = spark.read.format("delta").load(f"s3a://{bucket}/{subreddit}")
    logger.info(f"Successfully read {df.count()} records from {subreddit}")
    
    # Transform and clean data
    logger.info("Transforming data...")
    df = df.withColumn("approved_at_utc", col("approved_at_utc").cast("timestamp")) \
            .withColumn("banned_at_utc", col("banned_at_utc").cast("timestamp")) \
            .withColumn("created_utc", col("created_utc").cast("timestamp")) \
            .withColumn("created", col("created").cast("timestamp")) \
            .withColumn("date", to_date(col("created_utc"), "MM-dd-yyyy")) \
            .withColumn("year", year(col("date"))) \
            .withColumn("month", month(col("date"))) \
            .withColumn("day", dayofmonth(col("date"))) \
            .dropDuplicates(subset=["title"])
    
    # Write cleaned data to Delta
    filepath = f"s3a://{bucket}/{subreddit}_clean/"
    logger.info(f"Writing cleaned data to {filepath}")
    df.write.format("delta") \
        .partitionBy("year", "month", "day") \
        .mode("overwrite") \
        .option("mergeSchema", "true") \
        .option("overwriteSchema", "true") \
        .save(filepath)
    logger.info(f"Successfully wrote cleaned data to {filepath}")
    
    # Vacuum and generate manifests
    logger.info("Running vacuum and generating symlink manifests...")
    deltaTable = DeltaTable.forPath(spark, f"s3a://{bucket}/{subreddit}_clean")
    deltaTable.vacuum(168)
    deltaTable.generate("symlink_format_manifest")
    logger.info("Vacuum and manifest generation complete")
    
    # Run MSCK REPAIR TABLE in Athena
    logger.info(f"Running MSCK REPAIR TABLE for reddit.{subreddit}")
    athena = boto3.client("athena", region_name="us-east-1")
    athena.start_query_execution(
        QueryString=f"MSCK REPAIR TABLE reddit.{subreddit}",
        ResultConfiguration={
            "OutputLocation": f"s3://{bucket}/_athena_results"
        }
    )
    logger.info("MSCK REPAIR TABLE query submitted to Athena")
    
except Exception as e:
    logger.error(f"Error processing {subreddit}: {str(e)}", exc_info=True)
    raise
finally:
    job.commit()
