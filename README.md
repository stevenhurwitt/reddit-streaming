# reddit-streaming
An attempt to stream data from the reddit api using kafka, process with spark, and store in s3 data lake.

## Build dockerfiles

Go to docker directory and run build script.

`./build.sh`

`docker-compose up -d --no-recreate`

## Start streaming data

`docker exec -it jupyterlab`

`cd redditStreaming/src/main/python/reddit`

`python3 -m reddit_streaming.py`

## Start kafka producer

`docker exec -it jupyterlab`

`python3 -m reddit_producer.py`

### Remove untagged docker images

`docker rmi $(docker images | grep "^<none>" | awk "{print $3}")`

### Note on versions

When changing version of spark, hadoop, jupyterlab, etc, versions must be updated in `build.sh`, respective `*.Dockerfile`, `pom.xml`, `requirements.txt` and `reddit_streaming.py`.

### pyspark write stream to s3 error

Likely caused by guava jar mismatch, follow steps here: https://kontext.tech/article/689/pyspark-read-file-in-google-cloud-storage

### Kafka/zookeeper broker id mismatch

https://github.com/wurstmeister/kafka-docker/issues/409

If there are kafka errors, run `docker-compose down`, delete `cluster_config/kafka/logs` and `cluster_config/zookeeper/data/version-2` directories, run `docker-compose up -d`.


## Glue

Glue scripts to transform epoch columns to timestamps and partition by month/day/year in `s3://reddit-streaming-stevenhurwitt-2/{subreddit}_clean/`

`s3://reddit-streaming-stevenhurwitt-2/scripts/`

# enhancements

- lambda function to backup s3 to local daily (aws s3 sync...)
- glue function for s3 to docker postgres (aws is $6 a day??? could use a smaller instance?)
- airflow to gracefully restart streaming and producer jobs as needed
- could move from docker-compose local streaming app to cloud based
- kubernetes cluster w/ raspberry pis and local pc