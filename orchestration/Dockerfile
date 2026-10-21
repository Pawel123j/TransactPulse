# syntax=docker/dockerfile:1
FROM apache/airflow:3.3.2-python3.12
COPY orchestration/requirements.txt /tmp/tp-requirements.txt
COPY orchestration/constraints-3.12.txt /tmp/tp-constraints.txt
RUN --mount=type=secret,id=proxy_ca \
    if [ -f /run/secrets/proxy_ca ]; then export PIP_CERT=/run/secrets/proxy_ca; fi; \
    pip install --no-cache-dir -r /tmp/tp-requirements.txt -c /tmp/tp-constraints.txt
COPY lakehouse/ /opt/transactpulse/lakehouse/
ENV PYTHONPATH=/opt/transactpulse
