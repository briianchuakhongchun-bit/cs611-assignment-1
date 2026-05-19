# CS611 Assignment 1 - data pipeline runtime
# Mirrors the Lab 2 image: Python 3.12 + Java 17 (for PySpark) + JupyterLab.
FROM python:3.12-slim

ENV DEBIAN_FRONTEND=noninteractive

# Java is required by Spark. procps + bash satisfy Spark's spark-submit scripts.
RUN apt-get update && \
    apt-get install -y --no-install-recommends default-jdk-headless procps bash && \
    rm -rf /var/lib/apt/lists/* && \
    ln -sf /bin/bash /bin/sh && \
    mkdir -p /usr/lib/jvm/java-17-openjdk-amd64/bin && \
    ln -s "$(which java)" /usr/lib/jvm/java-17-openjdk-amd64/bin/java

ENV JAVA_HOME=/usr/lib/jvm/java-17-openjdk-amd64
ENV PATH=$PATH:$JAVA_HOME/bin

WORKDIR /app

COPY requirements.txt ./
RUN pip install --no-cache-dir -r requirements.txt

EXPOSE 8888
VOLUME /app
ENV JUPYTER_ENABLE_LAB=yes

CMD ["jupyter", "lab", "--ip=0.0.0.0", "--port=8888", "--no-browser", "--allow-root", "--notebook-dir=/app"]
