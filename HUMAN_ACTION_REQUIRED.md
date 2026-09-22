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

## 3. Add the Trivy scan (optional, ~5 minutes)

`pip-audit` already blocks on Python dependency CVEs. Trivy would add a second,
independent vulnerability database plus container-image coverage. It is not
wired up because this workflow was authored in an environment whose egress
policy blocks GitHub Releases for third-party repositories, so no version or
asset name could be verified — and two guesses both failed. See `SECURITY.md`.

From a machine with open network access:

```bash
# 1. Find a real release and its Linux asset name
curl -s https://api.github.com/repos/aquasecurity/trivy/releases/latest \
  | grep -E '"(tag_name|name)":' | grep -iE 'tag_name|linux'
```

Then add to the `security` job in `.github/workflows/ci.yml`, after the
pip-audit step, substituting the version and asset name you just confirmed:

```yaml
      - name: Install Trivy (pinned)
        run: |
          curl -sSfL -o /tmp/trivy.tar.gz \
            "https://github.com/aquasecurity/trivy/releases/download/v${TRIVY_VERSION}/<CONFIRMED_ASSET_NAME>"
          tar -xzf /tmp/trivy.tar.gz -C /tmp trivy
          /tmp/trivy --version
        env:
          TRIVY_VERSION: <CONFIRMED_VERSION>
      - name: Trivy (dependency CVEs) — blocking
        run: |
          /tmp/trivy fs . --scanners vuln --severity HIGH,CRITICAL \
            --ignore-unfixed --exit-code 1 --no-progress
```

Do **not** add it with `continue-on-error`. A scanner that reports success
without scanning is worse than no scanner.

---

## 3b. Upgrade the DAG check to a full Airflow DagBag load (optional)

`tests/test_retraining_loop_integrity.py` validates the drift → retraining loop
at run time and cross-checks — by parsing the DAG with `ast`, no Airflow needed —
that the branch gate can only route to task ids that actually exist in
`medallion_pipeline`. That runs in the ordinary `pytest` job and catches the
class of failure where a task is renamed on one side of the glue but not the
other.

What it does **not** do is load the DAGs through a real Airflow `DagBag`, which
would additionally catch import errors, cycles and operator-arg mistakes. That
step was left out of CI on purpose: `apache-airflow==2.9.3` (the version pinned
by `orchestration/docker-compose.yml`) could not be installed in the environment
where this work was done — `unicodecsv`/`python-nvd3` fail to build on the
sandbox's Python 3.11 — so a DagBag CI job could not be verified here, and an
unverified gate is not added.

On a runner where Airflow installs cleanly (GitHub's `ubuntu-latest` does), add:

```yaml
  dag-validation:
    name: Airflow DAG integrity
    runs-on: ubuntu-latest
    steps:
      - uses: actions/checkout@v4
      - uses: actions/setup-python@v5
        with:
          python-version: "3.11"
      - name: Install Airflow (matching the compose image) + docker provider
        run: |
          pip install "apache-airflow==2.9.3" apache-airflow-providers-docker             --constraint "https://raw.githubusercontent.com/apache/airflow/constraints-2.9.3/constraints-3.11.txt"
      - name: DagBag import check
        env:
          AIRFLOW_HOME: /tmp/airflow
          AIRFLOW__CORE__LOAD_EXAMPLES: "False"
          PYTHONPATH: ${{ github.workspace }}
        run: |
          python - <<'EOF'
          from airflow.models import DagBag
          bag = DagBag(dag_folder="orchestration/dags", include_examples=False)
          assert not bag.import_errors, bag.import_errors
          assert {"medallion_pipeline", "train_fraud_model"} <= set(bag.dag_ids)
          print("DAGs loaded:", sorted(bag.dag_ids))
          EOF
```

---

## 4. Optional: publish the dashboard

The Streamlit dashboard reads the gold layer over DuckDB and needs the lakehouse
to be reachable, so it cannot be deployed as a static site. If a public demo is
wanted, Streamlit Community Cloud with a small seeded DuckDB file is the
cheapest route — that is a product decision, not a gap in the code.
