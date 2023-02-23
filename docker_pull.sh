#!/bin/sh

docker login

echo "pulling docker files..."
docker pull stevenhurwitt/maven:latest
docker pull stevenhurwitt/glue_history:latest 
docker pull stevenhurwitt/jupyterlab:latest 
docker pull stevenhurwitt/cluster-base:latest
docker pull stevenhurwitt/spark-base:latest
docker pull stevenhurwitt/spark-master:latest 
docker pull stevenhurwitt/spark-worker:latest 