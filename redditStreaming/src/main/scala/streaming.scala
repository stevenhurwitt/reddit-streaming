package com.steven.redditStreaming

import org.apache.spark.sql._

object streaming {

def main(args:Array[String]):Unit= {

val spark: SparkSession = SparkSession.builder()
      // .master("spark://xanaxprincess.asuscomm.com:7077")
      .master("spark://spark-master:7077")
      // .master("spark://192.168.50.7:7077")
      .appName("kafkaProducer")
      .getOrCreate()

import spark.implicits._

val kafka_df = spark.readStream
        .format("kafka")
        .option("kafka.bootstrap.servers", "192.168.50.7:9092, 192.168.50.19:9092, 192.168.50.31:9092")
        .option("subscribe", "reddit")
        .option("includeHeaders", "true")
        .load()

    //   val json_schema =
    //     new ArrayType(
    //       new StructType()
    //         .add("topic", StringType)
    //         .add("subject", StringType)
    //         .add("eventType", StringType)
    //         .add("eventTime", TimestampType)
    //         .add("id", StringType)
    //         .add("data", new StructType()
    //           .add("api", StringType)
    //           .add("clientRequestId", StringType)
    //           .add("requestId", StringType)
    //           .add("eTag", StringType)
    //           .add("contentType", StringType)
    //           .add("contentLength", IntegerType)
    //           .add("blobType", StringType)
    //           .add("url", StringType)
    //           .add("sequencer", StringType)
    //           .add("storageDiagnostics", new StructType()
    //             .add("batchId", StringType)
    //             , true)
    //           , true)
    //         .add("dataVersion", StringType)
    //         .add("metadataVersion", StringType)
    //       , true)


      // val payload = kafka_df.selectExpr("CAST(body AS STRING) as json", "enqueuedTime", "properties").select(from_json($"json", json_schema).as("data"), col("enqueuedTime"), col("properties"))

      // var eventDF = payload.select(explode(payload("data")).alias("d"))


    //   var df = eventDF.select("d.data.url").withColumn("url", regexp_replace($"url", "https://zneudl1p33lakesstor.blob.core.windows.net/udl-container/", "abfss://udl-container@zneudl1p33lakesstor.dfs.core.windows.net/")).withColumn("region", split($"url", "/").getItem(5)).withColumn("WellName", split($"url", "/").getItem(6)).withColumn("WellBore", split($"url", "/").getItem(7)).withColumn("LogName", split($"url", "/").getItem(8)).withColumn("logID", split($"url", "/").getItem(10)).toDF()

    }
}
