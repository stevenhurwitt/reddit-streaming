package com.steven.reddit

import org.apache.spark._
import org.apache.spark.sql._
import org.apache.spark.sql.functions._ 
import org.apache.spark.sql.types._ 

object streaming {

def streaming(args:Array[String]):Unit= {

  // val aws_client = ""
  // val aws_secret = "abc"

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
      .config("spark.local.dir", "/opt/workspace/tmp/driver/twitter/")
      .config("spark.worker.dir", "/opt/workspace/tmp/executor/twitter/")
      .config("spark.eventLog.enabled", "true")
      .config("spark.eventLog.dir", "file:///opt/workspace/events/twitter/")
      .config("spark.sql.debug.maxToStringFields", 1000)
      .config("spark.jars.packages", "org.apache.spark:spark-sql-kafka-0-10_2.12:3.2.0,org.apache.hadoop:hadoop-common:3.3.1,org.apache.hadoop:hadoop-aws:3.3.1,org.apache.hadoop:hadoop-client:3.3.1,io.delta:delta-core_2.12:1.2.1")
      .config("spark.hadoop.fs.s3a.access.key", aws_client)
      .config("spark.hadoop.fs.s3a.secret.key", aws_secret)
      .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
      .config("spark.hadoop.fs.s3a.aws.credentials.provider", "org.apache.hadoop.fs.s3a.SimpleAWSCredentialsProvider")
      .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
      .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
      .config("spark.delta.logStore.class", "org.apache.spark.sql.delta.storage.S3SingleDriverLogStore")
      .enableHiveSupport()
      .getOrCreate()

  val sc = spark.sparkContext
  // val sqlContext = SQLContext(sc)

  val index = 0
  sc.setLogLevel("WARN")
  sc.setLocalProperty("spark.scheduler.pool", "pool" + index.toString)

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

// val json_schema = new ArrayType(new StructType().add("topic", StringType))

// println("created schema.")

// val json_schema = new ArrayType( new StructType().add("json", StringType))

val df = kafka_df.selectExpr("CAST(body AS STRING) as json")
// df.show()

// val df = kafka_df.selectExpr("CAST(body AS STRING) as json")
//             .select(from_json(col("json"), json_schema)
//             .alias("data"))
//             .select("data.*")

// val df = kafka_df.selectExpr("CAST(body AS STRING) as json")
//             .select(from_json(col("json"), json_schema).alias("data"))
//             .select("data.*")

// println("created df.")
// df.show()

// write to console.

// df.write.format("console").option("header", "true").option("truncate", "true").load()
// df.writeStream.format("console").queryName("twitter-console").start()

// write to spark delta table.

val target_dir = "s3:/twitter-stevenhurwitt/tweets/data/raw/twitter/"
df.write.format("delta").option("header", "true").save(target_dir)
print("wrote df to delta table.")

// write to jdbc (postgresql)

val database = "twitter"
val src_table = "dbo.twitter"
val user = "postgres"
val password  = "Secret!12345"

val connection_str = ""
val jdbcUrl = f"jdbc:postgresql://xanaxprincess.asuscomm.com:1433;databaseName={database}"
val jdbcDriver = "com.microsoft.sqlserver.jdbc.SQLServerDriver"

var jdbcDF = spark.read.format("jdbc")
    .option("url", jdbcUrl)
    .option("dbtable", "twitter")
    .option("user", user)
    .option("password", password)
    .option("driver", jdbcDriver)
    .load()

jdbcDF.show()
val path = "s3:/twitter-stevenhurwitt/tweets/data/raw/twitter/"
jdbcDF.write.format("delta").option("header", "true").partitionBy("").save(path)

jdbcDF.select("*").write.format("jdbc")
  .mode("overwrite")
  .option("url", jdbcUrl)
  .option("dbtable", "dbo.twitter_clean")
  .option("user", user)
  .option("password", password)
  .save()

// df.write.format("jdbc").option("header", "true)").

// allow continuous parallel streaming applications...

// spark.streaming.awaitAnyTermination

// print
println("streaming...")

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
