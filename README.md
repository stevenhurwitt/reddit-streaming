# reddit-streaming
An attempt to stream data from the reddit api using kafka, process with spark, and store in s3 data lake.

## Build dockerfiles

Go to docker directory and run build script.

`./build.sh`

`docker-compose up -d --no-recreate`

## Start streaming data

`cd redditStreaming/src/main/python/reddit`

`python3 -m reddit_streaming.py`

## Start kafka producer

`python3 -m reddit_producer.py`

## Etc.

### Remove untagged docker images

`docker rmi $(docker images | grep "^<none>" | awk "{print $3}")`

### Note on versions

When changing version of spark, hadoop, jupyterlab, etc, versions must be updated in `build.sh`, respective `*.Dockerfile`, `pom.xml`, `requirements.txt` and `reddit_streaming.py`.

### Kafka/zookeeper broker id mismatch

https://github.com/wurstmeister/kafka-docker/issues/409

If there are kafka errors, run `docker-compose down`, delete `cluster_config/kafka/logs` and `cluster_config/zookeeper/data/version-2` directories, run `docker-compose up -d`.