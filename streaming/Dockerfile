# syntax=docker/dockerfile:1
# Spark 3.5 + Delta + Kafka + S3A on the supported Python 3.11 / Java 17 pair.
# Build context is the repository root (see infra/docker-compose.yml).
FROM eclipse-temurin:17-jre-jammy AS java
FROM python:3.15-slim-bookworm

WORKDIR /app
COPY --from=java /opt/java/openjdk /opt/java/openjdk
ENV JAVA_HOME=/opt/java/openjdk
ENV PATH="/opt/java/openjdk/bin:${PATH}"
RUN apt-get update && apt-get install -y --no-install-recommends ca-certificates libstdc++6 \
    && rm -rf /var/lib/apt/lists/*

# Python deps: ML scoring (pandas_udf needs pandas + pyarrow; sklearn to load the
# artifact) and Faker for the model-training job. Great Expectations is optional
# (the silver report falls back to native metrics if it is absent).
COPY ingestion/requirements.txt /tmp/ingestion-requirements.txt
COPY streaming/requirements.txt /tmp/streaming-requirements.txt
RUN --mount=type=secret,id=proxy_ca \
    if [ -f /run/secrets/proxy_ca ]; then export PIP_CERT=/run/secrets/proxy_ca; fi; \
    pip install --no-cache-dir -r /tmp/ingestion-requirements.txt \
      -r /tmp/streaming-requirements.txt \
      "scikit-learn>=1.9.1,<2" "pandas>=2.0,<3" "numpy>=1.24" "pyarrow>=26,<27" \
      "boto3>=1.42,<2"
RUN ln -s /usr/local/lib/python3.11/site-packages/pyspark /opt/spark \
    && groupadd --gid 185 spark \
    && useradd --uid 185 --gid spark --create-home --shell /bin/bash spark

# Application code: bronze streaming + lakehouse batch jobs (silver/gold) +
# ingestion (used only by the model-training job, not by silver/gold at runtime).
COPY ingestion/ ./ingestion/
COPY streaming/ ./streaming/
COPY lakehouse/ ./lakehouse/
COPY infra/init_bucket.py ./infra/init_bucket.py

# Writable Ivy cache for --packages resolution; owned by the spark user (uid 185).
RUN mkdir -p /tmp/.ivy2 /reports /app/models \
    && chown -R spark:spark /app /tmp/.ivy2 /reports

ENV PYTHONPATH=/app
ENV SPARK_HOME=/opt/spark
ENV PYSPARK_PYTHON=/usr/local/bin/python3
ENV PYSPARK_DRIVER_PYTHON=/usr/local/bin/python3

USER spark

# Jars that must be on the classpath before the session starts (resolved by Ivy,
# cached in the mounted volume on first run). Versions are pinned to match the
# Spark 3.5.9 / Hadoop 3.3.4 runtime.
ENTRYPOINT ["/opt/spark/bin/spark-submit", \
  "--packages", "io.delta:delta-spark_2.12:3.2.1,org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.9,org.apache.hadoop:hadoop-aws:3.3.4,com.amazonaws:aws-java-sdk-bundle:1.12.262", \
  "--conf", "spark.jars.ivy=/tmp/.ivy2", \
  "/app/streaming/bronze_job.py"]
