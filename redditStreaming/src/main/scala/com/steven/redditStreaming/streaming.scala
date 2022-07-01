package com.steven.twitter

import org.apache.spark._
import org.apache.spark.sql._
import org.apache.spark.sql.functions._ 
import org.apache.spark.sql.types._ 

object streaming {

def streaming(args:Array[String]):Unit= {

val index = 0
val spark: SparkSession = SparkSession.builder()
      .master("spark://xanaxprincess.asuscomm.com:7077")
      // .master("spark://spark-master:7077")
      // .master("spark://192.168.50.7:7077")
      .appName("streaming")
      .config("spark.scheduler.mode", "FAIR")
      .config("spark.scheduler.allocation.file", "file:///opt/workspace/twitter-ingestion/fairscheduler.xml")
      .config("spark.executor.memory", "2048m")
      .config("spark.executor.cores", "1")
      .config("spark.streaming.concurrentJobs", "4")
  sc.setLocalProperty("spark.scheduler.pool", "pool" + index.toString)

import spark.implicits._
println("started spark.")

val kafka_df = spark.readStream
        .format("kafka")
        .option("kafka.bootstrap.servers", "xanaxprincess.asuscomm.com:9092")
        .option("subscribe", "reddit")
        .option("includeHeaders", "true")
        .load()

val json_schema = 
  new ArrayType(
    new StructType()
      .add("topic", StringType)
      .add("subject", StringType)
  )

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

    // val json_schema = 
    //     new ArrayType(
    //       new StructType()
    //         .add("", StringType)
    //         .add("", StringType)
    //         //   .add("struct", new StructType()
    //         //     .add("batchId", StringType)
    //         //     , true)
    //         // , true)
    //         // .add("metadataVersion", StringType)
    //     )


  val payload = kafka_df.selectExpr("CAST(body AS STRING) as json", "enqueuedTime", "properties").select(from_json($"json", json_schema).as("data"), col("enqueuedTime"), col("properties"))

  var eventDF = payload.select(explode(payload("data")).alias("d"))

  eventDF.writeStream.format("console").queryName("twitter_console").start()

  eventDf.write.format("delta").path("s3://twitter-stevenhurwitt/tweets/jars")

      // eventDf.write.format("delta").option("header", "true").partitionBy("").save("s3://twitter-stevenhurwitt/tweets/data/raw/twitter")

      // spark.streaming.awaitAnyTermination
    }
}
