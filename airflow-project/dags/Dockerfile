FROM apache/airflow:2.10.2

USER root

RUN apt-get update && \
    apt-get install -y openjdk-17-jdk && \
    apt-get clean

USER airflow

RUN pip install --no-cache-dir --default-timeout=1000 pyspark