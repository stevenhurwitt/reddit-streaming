FROM stevenhurwitt/spark-base

# -- Runtime
ARG spark_master_web_ui=8080

EXPOSE ${spark_master_web_ui} ${SPARK_MASTER_PORT}

#Create the workspace/events shared dir and start Spark Master
CMD bash -c "mkdir -p /opt/workspace/events && bin/spark-class org.apache.spark.deploy.master.Master >> logs/spark-master.out"

