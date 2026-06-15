FROM bde2020/spark-worker:3.0.0-hadoop3.2

RUN apk add --no-cache python3 python3-dev && \
    ln -sf /usr/bin/python3 /usr/bin/python3.9

ENV PYSPARK_PYTHON=python3