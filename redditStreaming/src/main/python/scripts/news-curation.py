import sys
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

args = getResolvedOptions(sys.argv, ["JOB_NAME"])
sc = SparkContext()
glueContext = GlueContext(sc)
# spark = glueContext.spark_session
job = Job(glueContext)
job.init(args["JOB_NAME"], args)

subreddit = "news"

spark = builder = SparkSession \
  .builder \
  .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem") \
  .getOrCreate()
  
# spark = configure_spark_with_delta_pip(builder).getOrCreate()
# .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \
# .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog") \
# .config("spark.delta.logStore.class", "org.apache.spark.sql.delta.storage.S3SingleDriverLogStore") \

df = spark.read.format("delta").option("header", True).load("s3a://reddit-streaming-stevenhurwitt/" + subreddit)

df = df.withColumn("approved_at_utc", col("approved_at_utc").cast("timestamp")) \
                .withColumn("banned_at_utc", col("banned_at_utc").cast("timestamp")) \
                .withColumn("created_utc", col("created_utc").cast("timestamp")) \
                .withColumn("created", col("created").cast("timestamp")) \
                .withColumn("date", to_date(col("created_utc"), "MM-dd-yyyy")) \
                .withColumn("year", year(col("date"))) \
                .withColumn("month", month(col("date"))) \
                .withColumn("day", dayofmonth(col("date"))) \
                .dropDuplicates(subset = ["title"])
                
filepath = "s3a://reddit-streaming-stevenhurwitt/" + subreddit + "_clean/"
df.write.format("delta").partitionBy("year", "month", "day").mode("overwrite").option("mergeSchema", "true").option("overwriteSchema", "true").option("header", True).save(filepath)
        
deltaTable = DeltaTable.forPath(spark, "s3a://reddit-streaming-stevenhurwitt/{}_clean".format(subreddit))
deltaTable.vacuum(168)
deltaTable.generate("symlink_format_manifest")

athena = boto3.client('athena')
athena.start_query_execution(
         QueryString = "MSCK REPAIR TABLE reddit.{}".format(subreddit),
         ResultConfiguration = {
             'OutputLocation': "s3://reddit-streaming-stevenhurwitt/_athena_results"
         })

job.commit()