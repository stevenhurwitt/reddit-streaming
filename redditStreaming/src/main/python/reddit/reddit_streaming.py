from pyspark.sql import SparkSession
from pyspark.sql.types import *
from pyspark.sql.functions import *
import yaml
import sys

def main(subreddit, kafka_host, spark_host):

    try:
        spark = SparkSession.builder.appName("reddit_" + subreddit) \
                .master("spark://{}:7077".format(spark_host)) \
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
                .option("kafka.bootstrap.servers", "{}:9092".format(kafka_host)) \
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
        # base = os.getcwd()
        # config_path = "/".join(base.split("/")[:-1])
        # config_file = os.path.join(config_path, "config.yaml")
        
        with open("config.yaml", "r") as f:
            config = yaml.safe_load(f)
            subreddit = config["subreddit"]
            post_type = config["post_type"]
            kafka_host = config["kafka_host"]
            spark_host = config["spark_host"]
    
    except:
        print("failed to find config.yaml")
        sys.exit()

    print("starting spark streaming...")
    main(subreddit, kafka_host, spark_host)