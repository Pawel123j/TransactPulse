# syntax=docker/dockerfile:1
# Streamlit dashboard image.
FROM python:3.11-slim

WORKDIR /app

COPY dashboard/requirements.txt ./dashboard/requirements.txt
RUN --mount=type=secret,id=proxy_ca \
    if [ -f /run/secrets/proxy_ca ]; then export PIP_CERT=/run/secrets/proxy_ca; fi; \
    pip install --no-cache-dir -r dashboard/requirements.txt

# The dashboard reads gold via DuckDB (analytics package) — copy both.
COPY analytics/ ./analytics/
COPY dashboard/ ./dashboard/
COPY streaming/ ./streaming/

ENV PYTHONPATH=/app
EXPOSE 8501

HEALTHCHECK --interval=30s --timeout=10s --retries=5 --start-period=20s \
  CMD python -c "import urllib.request; urllib.request.urlopen('http://localhost:8501/_stcore/health')" || exit 1

ENTRYPOINT ["streamlit", "run", "dashboard/app.py", \
  "--server.port=8501", "--server.address=0.0.0.0", "--server.headless=true"]
