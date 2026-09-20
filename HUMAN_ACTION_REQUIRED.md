# Human action required

Everything in this file needs a machine with a **running Docker daemon** and a
browser. It could not be done in the environment where the rest of the v1.0.1
work was carried out (Docker client present, daemon unavailable), so it is
written down precisely rather than skipped or faked.

Nothing here blocks CI — lint, the full 121-test suite (including the 12 real
Spark tests), compose validation and the security gates all pass without it.

---

## 1. Run the pipeline end to end

**Why:** the automated tests cover every transformation in isolation, but a
single live run is what proves the whole chain holds together — and it is what
produces the data the screenshots need.

```bash
git clone https://github.com/Pawel123j/TransactPulse.git
cd TransactPulse
cp infra/.env.example infra/.env        # dev-only credentials, safe as-is

# 1. Infrastructure: Kafka (KRaft) + MinIO
docker compose -f infra/docker-compose.yml up -d
./infra/smoke-test.sh                   # waits for Kafka and MinIO to be ready

# 2. Create the topic and start producing synthetic transactions
./infra/create-topics.sh
docker compose up -d generator

# 3. Streaming: Kafka -> bronze (Delta on MinIO)
docker compose run --rm jobs python -m streaming.bronze_job

# 4. Batch: bronze -> silver -> gold (DQ gate, quarantine, ML scoring)
docker compose run --rm jobs python -m lakehouse.silver_job
docker compose run --rm jobs python -m lakehouse.gold_job

# 5. Orchestration: the same batch path driven by Airflow
docker compose -f orchestration/docker-compose.yml up -d
# open http://localhost:8088  (admin/admin) and trigger `medallion_pipeline`

# 6. Serving
docker compose up -d dashboard
# open http://localhost:8501
```

**Expected result:** the DAG completes green, the MinIO bucket contains
`bronze/`, `silver/` and `gold/` prefixes, and the Streamlit dashboard renders
non-empty charts.

**If a step fails,** capture the console output — that is a real bug worth an
issue, not something to work around.

---

## 2. Capture the screenshots

With the stack from step 1 still running, capture the five images listed in
[`docs/screenshots/README.md`](docs/screenshots/README.md) and save them into
that directory under exactly the filenames given there.

Then uncomment the image block in the **Screenshots** section of the root
`README.md` — it is already written and pointing at those paths, commented out
so the published README never shows broken images.

---

## 3. Promote the Trivy scan to blocking

The `security` job in `.github/workflows/ci.yml` installs Trivy from a pinned
release tarball, but the asset URL could not be verified offline, so both Trivy
steps carry `continue-on-error: true`.

After the first CI run on `main`:

1. Open the `security` job log and check the **Install Trivy (pinned)** step.
2. If it printed a version, delete the two `continue-on-error: true` lines from
   the Trivy steps and update the table in `SECURITY.md` to mark Trivy blocking.
3. If it 404'd, bump `TRIVY_VERSION` to a release that exists and repeat.

---

## 4. Optional: publish the dashboard

The Streamlit dashboard reads the gold layer over DuckDB and needs the lakehouse
to be reachable, so it cannot be deployed as a static site. If a public demo is
wanted, Streamlit Community Cloud with a small seeded DuckDB file is the
cheapest route — that is a product decision, not a gap in the code.
