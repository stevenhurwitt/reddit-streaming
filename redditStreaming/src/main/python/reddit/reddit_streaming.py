from pyspark.sql import SparkSession
from pyspark.sql.types import *
from pyspark.sql.functions import *
import yaml
import sys

def main(subreddit):

    try:
        spark = SparkSession.builder.appName("reddit_" + subreddit) \
                .master("spark://spark-master:7077") \
                .config("spark.eventLog.enabled", "true") \
                .config("spark.eventLog.dir", "file:///opt/workspace/events") \
                .config("spark.jars.packages", "org.apache.spark:spark-sql-kafka-0-10_2.12:3.2.0") \
                .getOrCreate()
                # org.apache.hadoop:hadoop-aws:3.2.0

        spark.sparkContext.setLogLevel('WARN')
        print("created spark successfully")

    except Exception as e:
        print(e)

    test_df = spark \
                .readStream \
                .format("kafka") \
                .option("kafka.bootstrap.servers", "kafka:9092") \
                .option("subscribe", "reddit_" + subreddit) \
                .option("startingOffsets", "earliest") \
                .load() \
                .selectExpr("CAST(value AS STRING)")

    test_df.writeStream \
        .trigger(processingTime='5 seconds') \
        .outputMode("update") \
        .format("console") \
        .option("truncate", "true") \
        .start() \
        .awaitTermination()

if __name__ == "__main__":

    try:
        with open("config.yaml", "r") as f:
            config = yaml.safe_load(f)
            subreddit = config["subreddit"]
    
    except:
        print("failed to find config.yaml")
        sys.exit()

    print("starting spark streaming...")
    main(subreddit)