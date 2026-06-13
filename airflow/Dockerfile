FROM apache/airflow:2.7.2-python3.9

USER root
RUN apt-get update && apt-get install -y default-jdk curl && \
    curl -o /tmp/spark.tgz https://archive.apache.org/dist/spark/spark-3.0.0/spark-3.0.0-bin-hadoop3.2.tgz && \
    tar -xzf /tmp/spark.tgz -C /opt/ && \
    rm /tmp/spark.tgz

ENV SPARK_HOME=/opt/spark-3.0.0-bin-hadoop3.2
ENV PATH=$PATH:$SPARK_HOME/bin

USER airflow
RUN pip install apache-airflow-providers-apache-spark==4.1.3