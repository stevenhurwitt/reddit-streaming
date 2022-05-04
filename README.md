# reddit-streaming
An attempt to stream reddit data using Kafka, process with Spark, and store in Postgres.

## Build dockerfiles

Go to docker directory and run build script.

`cd docker && ./build.sh`

`docker-compose up -d`

## Create Kafka topic

Attach a shell of `wurstmeister/kafka` container in VSCode.

```
docker exec -it wurstmeister/kafkas bash
```

```
cd $KAFKA_HOME 

./kafka-topics.sh --create --topic window-example --bootstrap-server localhost:9092
```

## Run Spark streaming query

Attach VSCode or shell of `jupyterlab` container.

```
docker exec -it jupyterlab bash
```

Run spark-submit script using `kafka-example.py`.
```
cd /opt/workspace/notebooks/jobs

./spark-submit.sh kafka-example.py
```

## Generate data with Kafka Producer

Back in `wurstmeister/kafka` shell:

```
./bin/kafka-console-producer.sh --topic window-example --bootstrap-server localhost:9092 
```

### Enter data

```
1,3
1,4
2,2
2,1
2,3
2,1
3,4
3,5
3,6
1,10
```

### View streaming output

Data is processed in microbatches by `kafka-example.py`.

```
-------------------------------------------
Batch: 0
-------------------------------------------
+--------+---------------+
|test_key|sum(test_value)|
+--------+---------------+
+--------+---------------+

-------------------------------------------
Batch: 1
-------------------------------------------
+--------+---------------+
|test_key|sum(test_value)|
+--------+---------------+
|1       |3.0            |
+--------+---------------+

-------------------------------------------
Batch: 2
-------------------------------------------
+--------+---------------+
|test_key|sum(test_value)|
+--------+---------------+
|1       |7.0            |
+--------+---------------+

-------------------------------------------
Batch: 3
-------------------------------------------
+--------+---------------+
|test_key|sum(test_value)|
+--------+---------------+
|1       |7.0            |
|2       |2.0            |
+--------+---------------+

-------------------------------------------
Batch: 4
-------------------------------------------
+--------+---------------+
|test_key|sum(test_value)|
+--------+---------------+
|1       |7.0            |
|2       |3.0            |
+--------+---------------+

-------------------------------------------
Batch: 5
-------------------------------------------
+--------+---------------+
|test_key|sum(test_value)|
+--------+---------------+
|1       |7.0            |
|2       |6.0            |
+--------+---------------+

-------------------------------------------
Batch: 6
-------------------------------------------
+--------+---------------+
|test_key|sum(test_value)|
+--------+---------------+
|3       |4.0            |
|1       |7.0            |
|2       |6.0            |
+--------+---------------+

-------------------------------------------
Batch: 7
-------------------------------------------
+--------+---------------+
|test_key|sum(test_value)|
+--------+---------------+
|3       |10.0           |
|1       |7.0            |
|2       |6.0            |
+--------+---------------+

-------------------------------------------
Batch: 8
-------------------------------------------
+--------+---------------+
|test_key|sum(test_value)|
+--------+---------------+
|3       |10.0           |
|1       |17.0           |
|2       |6.0            |
+--------+---------------+
```

### Python wheel

`python setup.py bdist_wheel --universal`

## Kafka Producer

### Python wheel

`python3 setup.py bdist_wheel --universal`

## Kafka Producer

`python3 ./src/main/python/main.py`

## Build jar

`cd redditStreaming && mvn clean package`

## Put into hdfs

`hdfs dfs -put -f ./target/uber-redditStreaming-1.0-SNAPSHOT.jar hdfs:///user/ubuntu/jars/`

## Restart cluster

`cd ~/ && ./cluster-stop.sh`
`cd ~/ && ./cluster-start.sh`

## Hadoop & Yarn clusters

`stop-dfs.sh && stop-yarn.sh`
`start-dfs.sh && start-yarn.sh`

## Spark Shell

`spark-shell`

`pyspark`

## Spark Submit

```
spark-submit --deploy-mode client \
--class com.steven.redditStreaming.writeJDBC \
hdfs:///user/ubuntu/jars/uber-redditStreaming-1.0-SNAPSHOT.jar
```

### spark session test

```
spark-submit --deploy-mode client \
--class com.steven.redditStreaming.SparkSessionTest \
hdfs:///user/ubuntu/jars/uber-redditStreaming-1.0-SNAPSHOT.jar
```

### dataframe from csv

```
spark-submit --deploy-mode client \
--class com.steven.redditStreaming.dataFrameFromCSVFile \
hdfs:///user/ubuntu/jars/uber-redditStreaming-1.0-SNAPSHOT.jar
```

### spark kafka

```
spark-submit --deploy-mode cluster \
--packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.2.0 \
--class com.steven.redditStreaming.sparkKafka \
hdfs:///user/ubuntu/jars/uber-redditStreaming-1.0-SNAPSHOT.jar
```

## spark-submit python

`spark-submit --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.2.0 ./src/main/python/kafka-example.py`

## Build docker

`cd reddit-streaming/redditStreaming`
`docker build -t stevenhurwitt/reddit-streaming:latest .`