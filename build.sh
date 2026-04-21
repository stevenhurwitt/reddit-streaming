SPARK_VERSION="3.5.3"
HADOOP_VERSION="3"
JUPYTERLAB_VERSION="4.3.4"

# -- Building the Images using docker buildx

docker buildx build \
  --file cluster-base.Dockerfile \
  -t cluster-base \
  --load .

docker buildx build \
  --build-arg spark_version="${SPARK_VERSION}" \
  --build-arg hadoop_version="${HADOOP_VERSION}" \
  --file spark-base.Dockerfile \
  -t spark-base \
  --load .

docker buildx build \
  --file spark-master.Dockerfile \
  -t spark-master \
  --load .

docker buildx build \
  --file spark-worker.Dockerfile \
  -t spark-worker \
  --load .

docker buildx build \
  --build-arg spark_version="${SPARK_VERSION}" \
  --build-arg jupyterlab_version="${JUPYTERLAB_VERSION}" \
  --file jupyterlab.Dockerfile \
  -t jupyterlab \
  --load .
