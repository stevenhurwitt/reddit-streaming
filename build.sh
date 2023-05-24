SPARK_VERSION="3.4.0"
HADOOP_VERSION="3"
JUPYTERLAB_VERSION="3.5.2"

# -- Building the Images

# docker build -f cluster-base.Dockerfile -t cluster-base .

# docker build --build-arg spark_version="${SPARK_VERSION}" --build-arg hadoop_version="${HADOOP_VERSION}" -f spark-base.Dockerfile -t spark-base .

# docker build -f spark-master.Dockerfile -t spark-master .

# docker build -f spark-worker.Dockerfile -t spark-worker .

# ,docker build --build-arg spark_version="${SPARK_VERSION}" --build-arg jupyterlab_version="${JUPYTERLAB_VERSION}" -f jupyterlab.Dockerfile -t jupyterlab .

# docker build \
#   -f maven.Dockerfile \
#   -t stevenhurwitt/maven:latest .

# docker build \
#   -f glue_history.Dockerfile \
#   -t stevenhurwitt/glue_history:latest .

docker build \
  -f cluster-base.Dockerfile \
  -t stevenhurwitt/cluster-base:latest .

docker build \
  --build-arg spark_version="${SPARK_VERSION}" \
  --build-arg hadoop_version="${HADOOP_VERSION}" \
  -f spark-base.Dockerfile \
  -t stevenhurwitt/spark-base:latest .

docker build \
  -f spark-master.Dockerfile \
  -t stevenhurwitt/spark-master:latest .

docker build \
  -f spark-worker.Dockerfile \
  -t stevenhurwitt/spark-worker:latest .

docker build \
  --build-arg spark_version="${SPARK_VERSION}" \
  --build-arg jupyterlab_version="${JUPYTERLAB_VERSION}" \
  -f jupyterlab.Dockerfile \
  -t stevenhurwitt/jupyterlab:latest .
