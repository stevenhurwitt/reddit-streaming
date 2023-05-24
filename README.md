# reddit-streaming
An attempt to stream data from the reddit api using kafka, process with spark, and store in s3 data lake.

## credentials file (json)

```json
{        
    "client-id": "tR3-Rx38Bt5_1H3Cy0fP5Q",          
    "secret-id": "nKuZqRowUMP_neaHvbsKeDbqqdtr7A",          
    "user": "SJH823",          
    "password": "graycie123",          
    "subreddit": "technology",          
    "aws_client": "AKIAR6OXHD6L4X3XTNVE",          
    "aws_secret": "XXX9HgP+GopSJvRj270xGiuhSilo4lGQErww7epX7Z"
}
```

XXX=?

## config file (yaml)

```yaml
subreddit: #limit to 4 b/c there are 4 cores on raspberry pi for spark to share
  - technology
  - ProgrammerHumor
  - news
  - worldnews
  - BikiniBottomTwitter
  - BlackPeopleTwitter
  - WhitePeopleTwitter
  - aws
post_type: new
bucket: reddit-streaming-stevenhurwitt-2
kafka_host: kafka
spark_host: spark-master
postgres_host: xanaxprincess.asuscomm.com
postgres_user: steven
postgres_password: XXXXXX!1234
extra_jar_list: "org.apache.spark:spark-sql-kafka-0-10_2.12:3.3.2,org.apache.hadoop:hadoop-common:3.3.4,org.apache.hadoop:hadoop-aws:3.3.4,org.apache.hadoop:hadoop-client:3.3.4,io.delta:delta-core_2.12:2.2.0,org.postgresql:postgresql:42.5.0"
debug: True
```

XXXXXX=?

# Build dockerfiles

Go to docker directory and run build script.

`git clone https://github.com/stevenhurwitt/reddit-streaming

`cd /home/steven/reddit-streaming/`

`./build.sh`

`docker-compose up -d --no-recreate`

# Start streaming data

`cd redditStreaming/src/main/python/reddit`

`python3 -m reddit_streaming.py`

# Start kafka producer

`cd redditStreaming/src/main/python/reddit`

`python3 -m reddit_producer.py`

## Remove untagged docker images

`docker rmi $(docker images | grep "^<none>" | awk "{print $3}")`

## Note on versions

When changing version of spark, hadoop, jupyterlab, etc, versions must be updated in `build.sh`, respective `*.Dockerfile`, `pom.xml`, `requirements.txt` and `reddit_streaming.py`.

## pyspark write stream to s3 error

Likely caused by guava jar mismatch, follow steps here: https://kontext.tech/article/689/pyspark-read-file-in-google-cloud-storage

- **fixed** in spark update 3.3.3/3.3.4

## Kafka/zookeeper broker id mismatch

https://github.com/wurstmeister/kafka-docker/issues/409

If there are kafka errors: 

run `docker-compose down`, 

delete `cluster_config/kafka/logs` 

&

 `cluster_config/zookeeper/data/version-2`.

run `docker-compose up -d`.


## S3

`s3://XXX/scripts/`

XXX=?

hello...

# enhancements

- lambda function to backup s3 to local daily (aws s3 sync...)

- glue function for s3 to docker postgres (aws is $6 a day??? could use a smaller instance?)

- airflow to gracefully restart streaming and producer jobs as needed

- could move from docker-compose local streaming app to cloud based

- kubernetes cluster w/ raspberry pis and local pc (arm)