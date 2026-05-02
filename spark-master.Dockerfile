FROM spark-base

# -- Runtime
ARG spark_master_web_ui=8008

EXPOSE ${spark_master_web_ui} ${SPARK_MASTER_PORT}

COPY ./spark-defaults.conf conf/spark-defaults.conf

COPY ./spark-env.conf conf/spark-env.conf

# -- Copy local_jobs directory and install dependencies
COPY ./local_jobs /opt/workspace/local_jobs

RUN cd /opt/workspace/local_jobs && \
    python3 -m pip install --upgrade pip && \
    python3 -m pip install boto3 pyspark==3.5.3 delta-spark==3.1.0

#Create the workspace/events shared dir and start Spark Master

CMD bash -c "mkdir -p /opt/workspace/events/technology && mkdir -p /opt/workspace/events/news && mkdir -p /opt/workspace/events/worldnews && mkdir -p /opt/workspace/events/ProgrammerHumor && mkdir -p /opt/workspace/tmp/driver/technology && mkdir -p /opt/workspace/tmp/executor/technology && bin/spark-class org.apache.spark.deploy.master.Master >> logs/spark-master.out"


