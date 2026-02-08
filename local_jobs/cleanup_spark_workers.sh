#!/bin/bash
# cleanup_spark_workers.sh - Clean up Spark worker temporary files

docker exec reddit-spark-worker-1 bash -c 'cd /opt/workspace/tmp && rm -rf spark-* driver/* blocks/*'
docker exec reddit-spark-worker-2 bash -c 'cd /opt/workspace/tmp && rm -rf spark-* driver/* blocks/*'
