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

args = getResolvedOptions(sys.argv, ["JOB_NAME"])
sc = SparkContext()
glueContext = GlueContext(sc)
# spark = glueContext.spark_session
job = Job(glueContext)
job.init(args["JOB_NAME"], args)

subreddit = "technology"

spark = SparkSession \
  .builder \
  .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem") \
  .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \
  .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog") \
  .config("spark.delta.logStore.class", "org.apache.spark.sql.delta.storage.S3SingleDriverLogStore") \
  .getOrCreate()

df = spark.read.format("delta").option("header", True).load("s3a://reddit-stevenhurwitt/" + subreddit)

df = df.withColumn("approved_at_utc", col("approved_at_utc").cast("timestamp")) \
                .withColumn("banned_at_utc", col("banned_at_utc").cast("timestamp")) \
                .withColumn("created_utc", col("created_utc").cast("timestamp")) \
                .withColumn("created", col("created").cast("timestamp")) \
                .withColumn("post_date", to_date(col("created_utc"), "MM-dd-yyyy"))
                
filepath = "s3a://reddit-stevenhurwitt/" + subreddit + "_clean/"
df.write.format("delta").partitionBy("post_date").mode("overwrite").option("mergeSchema", True).option("header", True).save(filepath)
        
# deltaTable = DeltaTable.forPath(spark, "s3a://reddit-stevenhurwitt/{}_clean".format(subreddit))
# deltaTable.generate("symlink_format_manifest")

job.commit()
