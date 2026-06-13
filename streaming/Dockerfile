# Bronze streaming job image — Spark 3.5 + Delta + Kafka + S3A (hadoop-aws).
# Build context is the repository root (see infra/docker-compose.yml).
FROM apache/spark:3.5.1

USER root
WORKDIR /app

# Application code (the streaming package only; bronze has no other deps).
COPY streaming/ ./streaming/

# Writable Ivy cache for --packages resolution; owned by the spark user (uid 185).
RUN mkdir -p /tmp/.ivy2 && chown -R spark:spark /app /tmp/.ivy2

ENV PYTHONPATH=/app

USER spark

# Jars that must be on the classpath before the session starts (resolved by Ivy,
# cached in the mounted volume on first run). Versions are pinned to match the
# Spark 3.5.1 / Hadoop 3.3.4 runtime.
ENTRYPOINT ["/opt/spark/bin/spark-submit", \
  "--packages", "io.delta:delta-spark_2.12:3.2.0,org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.1,org.apache.hadoop:hadoop-aws:3.3.4,com.amazonaws:aws-java-sdk-bundle:1.12.262", \
  "--conf", "spark.jars.ivy=/tmp/.ivy2", \
  "/app/streaming/bronze_job.py"]
