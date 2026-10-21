# syntax=docker/dockerfile:1
# Bronze streaming job image — Spark 3.5 + Delta + Kafka + S3A (hadoop-aws).
# Build context is the repository root (see infra/docker-compose.yml).
FROM apache/spark:3.5.9

USER root
WORKDIR /app

# Python deps: ML scoring (pandas_udf needs pandas + pyarrow; sklearn to load the
# artifact) and Faker for the model-training job. Great Expectations is optional
# (the silver report falls back to native metrics if it is absent).
COPY ingestion/requirements.txt /tmp/ingestion-requirements.txt
RUN --mount=type=secret,id=proxy_ca \
    if [ -f /run/secrets/proxy_ca ]; then export PIP_CERT=/run/secrets/proxy_ca; fi; \
    pip install --no-cache-dir -r /tmp/ingestion-requirements.txt \
      "scikit-learn>=1.9.1,<2" "pandas>=2.0,<3" "numpy>=1.24" "pyarrow>=26,<27"

# Application code: bronze streaming + lakehouse batch jobs (silver/gold) +
# ingestion (used only by the model-training job, not by silver/gold at runtime).
COPY ingestion/ ./ingestion/
COPY streaming/ ./streaming/
COPY lakehouse/ ./lakehouse/

# Writable Ivy cache for --packages resolution; owned by the spark user (uid 185).
RUN mkdir -p /tmp/.ivy2 && chown -R spark:spark /app /tmp/.ivy2

ENV PYTHONPATH=/app

USER spark

# Jars that must be on the classpath before the session starts (resolved by Ivy,
# cached in the mounted volume on first run). Versions are pinned to match the
# Spark 3.5.9 / Hadoop 3.3.4 runtime.
ENTRYPOINT ["/opt/spark/bin/spark-submit", \
  "--packages", "io.delta:delta-spark_2.12:3.2.1,org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.9,org.apache.hadoop:hadoop-aws:3.3.4,com.amazonaws:aws-java-sdk-bundle:1.12.262", \
  "--conf", "spark.jars.ivy=/tmp/.ivy2", \
  "/app/streaming/bronze_job.py"]
