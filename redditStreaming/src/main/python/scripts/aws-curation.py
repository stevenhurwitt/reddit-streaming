import ast
import json
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

args = getResolvedOptions(sys.argv, ["JOB_NAME"])
sc = SparkContext()
glueContext = GlueContext(sc)
sc.setLogLevel('INFO')
logger = glueContext.get_logger()
# spark = glueContext.spark_session
job = Job(glueContext)
job.init(args["JOB_NAME"], args)

subreddit = "aws"

secretmanager_client = boto3.client("secretsmanager")

delta_version = "2.0.2"
spark_version = "3.3.2"
hadoop_version = "3.3.4"
postgres_version = "42.5.0"
aws_client = ast.literal_eval(secretmanager_client.get_secret_value(SecretId="AWS_ACCESS_KEY_ID")["SecretString"])["AWS_ACCESS_KEY_ID"]
aws_secret = ast.literal_eval(secretmanager_client.get_secret_value(SecretId="AWS_SECRET_ACCESS_KEY")["SecretString"])["AWS_SECRET_ACCESS_KEY"]
extra_jar_list = f"org.apache.spark:spark-sql-kafka-0-10_2.12:{spark_version},org.apache.hadoop:hadoop-common:{hadoop_version},org.apache.hadoop:hadoop-aws:{hadoop_version},org.apache.hadoop:hadoop-client:{hadoop_version},io.delta:delta-core_2.12:{delta_version},org.postgresql:postgresql:{postgres_version}"
bucket = "reddit-streaming-stevenhurwitt-2"

spark = SparkSession \
  .builder \
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

  
logger.info("created spark session.")
# spark = configure_spark_with_delta_pip(builder).getOrCreate()
# .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \
# .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog") \
# .config("spark.delta.logStore.class", "org.apache.spark.sql.delta.storage.S3SingleDriverLogStore") \

df = spark.read.format("delta").option("header", True).load("s3a://" + bucket + "/" + subreddit)

df = df.withColumn("approved_at_utc", col("approved_at_utc").cast("timestamp")) \
                .withColumn("banned_at_utc", col("banned_at_utc").cast("timestamp")) \
                .withColumn("created_utc", col("created_utc").cast("timestamp")) \
                .withColumn("created", col("created").cast("timestamp")) \
                .withColumn("date", to_date(col("created_utc"), "MM-dd-yyyy")) \
                .withColumn("year", year(col("date"))) \
                .withColumn("month", month(col("date"))) \
                .withColumn("day", dayofmonth(col("date"))) \
                .dropDuplicates(subset = ["title"])
                
filepath = "s3a://" + bucket + "/" + subreddit + "_clean/"
df.write.format("delta").partitionBy("year", "month", "day").mode("overwrite").option("overwriteSchema", "true").option("header", True).save(filepath)
        
deltaTable = DeltaTable.forPath(spark, f"s3a://{bucket}/{subreddit}_clean")
deltaTable.vacuum(168)
deltaTable.generate("symlink_format_manifest")

logger.info("wrote clean df to delta, vacuumed df.")

db_creds = ast.literal_eval(secretmanager_client.get_secret_value(SecretId="dev/reddit/postgres")["SecretString"])
host = db_creds['host']
port = db_creds['port']
dbname = db_creds['dbname']
user = db_creds["username"]
password = db_creds["password"]

connect_str = f"jdbc:postgresql://{host}:{port}/{dbname}"

try:
    df.write.format("jdbc") \
        .mode("overwrite") \
        .option("url", connect_str) \
        .option("dbtable", f"reddit.{subreddit}") \
        .option("user", user) \
        .option("password", password) \
        .option("driver", "org.postgresql.Driver") \
        .save()

    logger.info("wrote df to postgresql table.")

except Exception as e:
    logger.info("Exception: {}".format(e))

athena = boto3.client('athena')
athena.start_query_execution(
         QueryString = f"MSCK REPAIR TABLE `reddit`.`{subreddit}`",
         ResultConfiguration = {
             'OutputLocation': f"s3://{bucket}/_athena_results"
         })

logger.info("ran msck repair for athena.")

job.commit()
