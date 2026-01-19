SPARK_VERSION="3.5.3"
HADOOP_VERSION="3"
JUPYTERLAB_VERSION="4.3.4"

# -- Building the Images

# docker build -f cluster-base.Dockerfile -t cluster-base .

# docker build --build-arg spark_version="${SPARK_VERSION}" --build-arg hadoop_version="${HADOOP_VERSION}" -f spark-base.Dockerfile -t spark-base .

# docker build -f spark-master.Dockerfile -t spark-master .

# docker build -f spark-worker.Dockerfile -t spark-worker .

# ,docker build --build-arg spark_version="${SPARK_VERSION}" --build-arg jupyterlab_version="${JUPYTERLAB_VERSION}" -f jupyterlab.Dockerfile -t jupyterlab .

docker build \
  -f cluster-base.Dockerfile \
  -t cluster-base .

docker build \
  --build-arg spark_version="${SPARK_VERSION}" \
  --build-arg hadoop_version="${HADOOP_VERSION}" \
  -f spark-base.Dockerfile \
  -t spark-base .

docker build \
  -f spark-master.Dockerfile \
  -t spark-master .

docker build \
  -f spark-worker.Dockerfile \
  -t spark-worker .

docker build \
  --build-arg spark_version="${SPARK_VERSION}" \
  --build-arg jupyterlab_version="${JUPYTERLAB_VERSION}" \
  -f jupyterlab.Dockerfile \
  -t jupyterlab .
