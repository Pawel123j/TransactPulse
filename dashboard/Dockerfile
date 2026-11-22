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

# Match the jobs image so a reports volume initialized by either container is
# writable by the Spark report writer. The dashboard only reads those reports.
RUN groupadd --gid 185 spark \
    && useradd --uid 185 --gid spark --create-home spark \
    && mkdir /reports && chown spark:spark /reports

ENV PYTHONPATH=/app
# TP-04: proces Streamlit bez uprawnień roota (ten sam nieuprzywilejowany
# użytkownik co w obrazie jobs, żeby wolumen raportów był wspólny).
USER 185:185
EXPOSE 8501

HEALTHCHECK --interval=30s --timeout=10s --retries=5 --start-period=20s \
  CMD python -c "import urllib.request; urllib.request.urlopen('http://localhost:8501/_stcore/health')" || exit 1

ENTRYPOINT ["streamlit", "run", "dashboard/app.py", \
  "--server.port=8501", "--server.address=0.0.0.0", "--server.headless=true"]
