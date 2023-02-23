#!/bin/sh

docker login

echo "pushing docker files..."
docker push stevenhurwitt/maven:latest
docker push stevenhurwitt/glue_history:latest 
docker push stevenhurwitt/jupyterlab:latest 
docker push stevenhurwitt/spark-master:latest 
docker push stevenhurwitt/spark-worker:latest 