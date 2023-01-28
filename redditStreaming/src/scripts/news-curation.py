import os
import sys
import json
import pprint
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
from pyspark.sql.session import SparkSession
from pyspark.sql.functions import *
from delta import *
from delta.tables import *
import boto3
import ast

args = getResolvedOptions(sys.argv, ["JOB_NAME"])
sc = SparkContext()
glueContext = GlueContext(sc)
# spark = glueContext.spark_session
job = Job(glueContext)
job.init(args["JOB_NAME"], args)

subreddit = "news"
bucket = "reddit-streaming-stevenhurwitt"

secretmanager_client = boto3.client("secretsmanager")

aws_client = ast.literal_eval(secretmanager_client.get_secret_value(SecretId="AWS_ACCESS_KEY_ID")["SecretString"])["AWS_ACCESS_KEY_ID"]
aws_secret = ast.literal_eval(secretmanager_client.get_secret_value(SecretId="AWS_SECRET_ACCESS_KEY")["SecretString"])["AWS_SECRET_ACCESS_KEY"]
extra_jar_list = "org.apache.spark:spark-sql-kafka-0-10_2.12:3.2.0,org.apache.hadoop:hadoop-common:3.3.1,org.apache.hadoop:hadoop-aws:3.3.1,org.apache.hadoop:hadoop-client:3.3.1,io.delta:delta-core_2.12:1.2.1,org.postgresql:postgresql:42.5.0"

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

print("created spark session.")
  
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
df.write.format("delta").partitionBy("year", "month", "day").mode("overwrite").option("mergeSchema", "true").option("overwriteSchema", "true").option("header", True).save(filepath)
        
deltaTable = DeltaTable.forPath(spark, "s3a://" + bucket + "/{}_clean".format(subreddit))
deltaTable.vacuum(168)
deltaTable.generate("symlink_format_manifest")

print("wrote df to delta.")

db_creds = ast.literal_eval(secretmanager_client.get_secret_value(SecretId="dev/reddit/postgres")["SecretString"])
connect_str = "jdbc:postgresql://{}:{}/{}".format(db_creds["host"], db_creds["port"], db_creds["dbname"])

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

athena = boto3.client('athena')
athena.start_query_execution(
         QueryString = "MSCK REPAIR TABLE reddit.{}".format(subreddit),
         ResultConfiguration = {
             'OutputLocation': "s3://" + bucket + "/_athena_results"
         })

print("ran msck repair for athena.")

job.commit()
