package com.steven.redditStreaming

import org.apache.spark.sql._

import org.apache.spark

object producer {

    def main(args:Array[String]):Unit= {

        // aws secrets...
        import scala.io.Source

        val filename = "aws_access_key.txt"
        val filename2 = "aws_secret.txt"
        for (line <- Source.fromFile(filename).getLines) {
            aws_client = line[0]
            println("aws client: " + aws_client)

        for (line <- Source.fromFile(filename2).getLines) {
            // println(line)
            aws_secret = line[0]
            println("aws secret: ***")
}
        // val aws_client = ""
        // val aws_secret = ""

        if aws_client or aws_secret is None:
            print("aws client or secret is None")
        
        val spark = SparkSession.builder
                                .master("spark://spark-master:7077")
                                .appName("producer")
                                .config("spark.scheduler.mode", "FAIR")
                                .config("spark.scheduler.allocation.file", "file:///opt/workspace/redditStreaming/fairscheduler.xml")
                                .config("spark.executor.memory", "4096m")
                                .config("spark.executor.cores", "1")
                                .config("spark.streaming.concurrentJobs", "8")
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

        sc = spark.SparkContext
        println("imported spark.")

        return(None)
    }

}