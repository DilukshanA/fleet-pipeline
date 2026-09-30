FROM python:3.11-slim-bookworm

# Spark 3.5 officially supports Java 17 (not yet 21) -- pin to bookworm
# explicitly since newer slim tags moved to trixie, which dropped the
# openjdk-17 package.
# Spark needs a JRE.
RUN apt-get update \
    && apt-get install -y --no-install-recommends openjdk-17-jre-headless curl procps \
    && rm -rf /var/lib/apt/lists/*

ENV JAVA_HOME=/usr/lib/jvm/java-17-openjdk-amd64
WORKDIR /app

COPY streaming/requirements.txt /app/streaming/requirements.txt
RUN pip install --no-cache-dir -r /app/streaming/requirements.txt

COPY common /app/common
COPY streaming /app/streaming

ENV PYTHONPATH=/app
ENV PYTHONUNBUFFERED=1

# The Kafka connector + its dependencies are resolved from Maven via
# spark-submit --packages and cached in the spark_ivy_cache volume (mounted
# at /root/.ivy2), so only the first start needs internet access.
CMD ["spark-submit", \
     "--packages", "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.1,org.apache.commons:commons-pool2:2.11.1", \
     "streaming/spark_stream_job.py"]
