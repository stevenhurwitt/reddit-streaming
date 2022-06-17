FROM spark-base

# -- Runtime
ARG spark_master_web_ui=8080

EXPOSE ${spark_master_web_ui} ${SPARK_MASTER_PORT}

COPY ./spark-defaults.conf conf/spark-defaults.conf

#Create the workspace/events shared dir and start Spark Master
CMD bash -c "mkdir -p /opt/workspace/events && mkdir -p /opt/workspace/tmp/driver && mkdir -p /opt/workspace/tmp/executor && bin/spark-class org.apache.spark.deploy.master.Master >> logs/spark-master.out"

