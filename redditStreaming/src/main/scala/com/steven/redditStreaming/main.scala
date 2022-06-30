package com.steven.twitter

import org.apache.spark._
import org.apache.spark.sql._
import org.apache.spark.sql.functions._ 
import org.apache.spark.sql.types._ 

object twitter {

def main(args:Array[String]):Unit= {

val spark: SparkSession = SparkSession.builder()
      .master("spark://xanaxprincess.asuscomm.com:7077")
      // .master("spark://spark-master:7077")
      // .master("spark://192.168.50.7:7077")
      .appName("kafkaProducer")
      .getOrCreate()

import spark.implicits._
// println("started spark.")

val kafka_df = spark.readStream
        .format("kafka")
        .option("kafka.bootstrap.servers", "xanaxprincess.asuscomm.com:9092")
        .option("subscribe", "twitter")
        .option("includeHeaders", "true")
        .load()

// println("read kafka.")

// val json_schema = 
//   new ArrayType(
//     new StructType()
//       .add("topic", StringType)
//       .add("body", StringType)
//   )

val json_schema = new ArrayType(new StructType().add("topic", StringType))

// println("created schema.")

val df = kafka_df.selectExpr("CAST(body AS STRING) as json")
            .select(from_json(col("json"), json_schema).alias("data"))
            .select("data.*")

// println("created df.")
df.show()

// df.write.format("console").option("header", "true").option("truncate", "true").load()
df.writeStream.format("console").queryName("twitter-console").start()

val target_dir = "s3:/twitter-stevenhurwitt/tweets/data/raw/twitter/"
// df.write.format("delta").queryName("twitter-delta").option("header", "true").save(target_dir)
print("wrote df to delta table.")

// spark.streaming.awaitAnyTermination
print("streaming...")

// return(df)

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


      // val payload = kafka_df.selectExpr("CAST(body AS STRING) as json", "enqueuedTime", "properties").select(from_json($"json", json_schema).as("data"), col("enqueuedTime"), col("properties"))

      // var eventDF = payload.select(explode(payload("data")).alias("d"))
      
      // eventDF = eventDF.withColumn("dt_now", )

      // eventDf.writeStream.format("console").queryName("twitter").start()

      // eventDf.write.format("delta").option("header", "true").partitionBy("").save("s3://twitter-stevenhurwitt/tweets/data/raw/twitter")

      // spark.streaming.awaitAnyTermination
    }
}
