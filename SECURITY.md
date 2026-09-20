# Security

TransactPulse is a **portfolio / learning project**. It runs a full lakehouse on
one machine via `docker compose`, which forces trade-offs that would be
unacceptable in production. This document states those trade-offs explicitly,
describes what the repository *does* enforce, and explains how to report a
problem.

> **All data in this project is synthetic.** The generator fabricates every
> transaction with Faker; there is no real banking data, no PII, and no
> production credentials anywhere in the repository or its history.

---

## Automated checks in CI

Run on every push and pull request
([`.github/workflows/ci.yml`](.github/workflows/ci.yml)):

| Tool | Scope | Blocking? |
| ---- | ----- | --------- |
| **Bandit** | Python SAST over `analytics/`, `dashboard/`, `ingestion/`, `lakehouse/`, `orchestration/`, `streaming/` | **Yes** — the job fails on any finding |
| **Gitleaks** | Secret scanning across the working tree and the full git history | **Yes** |
| **ruff** | Lint + format, including rules that catch unsafe patterns | **Yes** |
| **pytest** | 109 fast tests + 12 Spark tests | **Yes** |

Bandit runs against the config in `pyproject.toml`. Every remaining finding is
either fixed or annotated inline with `# nosec <TEST-ID>` and a comment stating
why it is safe in this context:

| Finding | Location | Why it is accepted |
| ------- | -------- | ------------------ |
| `B311` non-cryptographic RNG | `ingestion/generator.py` | Synthetic data generation; reproducibility matters, cryptographic strength does not. No security decision derives from these values. |
| `B403`/`B301` pickle | `lakehouse/ml/model.py` | The artifact is produced by this project's own training job and read from a local path or a Docker volume. See *Model artifacts* below. |
| `B108` hardcoded `/tmp` path | `orchestration/dags/medallion_pipeline.py` | Container-internal Ivy cache path backed by a named Docker volume, not host `/tmp`. |
| `B608` SQL built from a string | `analytics/duckdb_query.py` | `CREATE VIEW` cannot take bind parameters; the view name comes from a module constant and the bucket is validated against a strict allowlist regex before interpolation. |

### Dependency CVE scanning

Dependency CVEs **are** gated in CI as of v1.0.1:

| Tool | Scope | Blocking? |
| ---- | ----- | --------- |
| `pip-audit` | every `requirements*.txt` in the repo, resolved against the Python advisory database | **Yes** |
| Trivy `fs` | filesystem vulnerabilities and IaC misconfiguration | No — advisory only, see below |
| Dependabot | weekly PRs for pip, GitHub Actions and Docker base images | n/a |

`pip-audit` is the blocking gate because it installs from PyPI: no third-party
action has to resolve at job set-up time, which is what broke the earlier Trivy
attempt. All five requirements files are currently clean.

The advisory feed moves daily, so an advisory with no released fix can appear
against an untouched commit. The answer is **not** to weaken the gate: add
`--ignore-vuln <ID>` to the step with a one-line justification, which keeps the
exception explicit and reviewable in the diff. The Trivy scan runs with
`--ignore-unfixed` because, unlike pip-audit, it supports that flag natively.

Trivy is installed from a **pinned release tarball** (the same pattern gitleaks
uses) rather than from an action. It is currently non-blocking: the pinned asset
URL could not be verified from the environment the workflow was authored in.
Once a CI run shows the install step succeeding, drop the `continue-on-error`
flags on both Trivy steps to make it blocking.

Still outstanding for a later release: **SBOM generation** and **signed, pinned
container base images**.

---

## Known limitations of the development stack

The defaults exist so the demo starts with one command. **Do not expose this
stack to an untrusted network.**

### Credentials

- MinIO uses `minioadmin` / `minioadmin123`; Airflow uses `admin` / `admin`.
  These are published in this repository, so they are public knowledge.
- Every credential is read from an environment variable with a dev default
  (`S3_ACCESS_KEY`, `S3_SECRET_KEY`, `MINIO_ROOT_USER`, `MINIO_ROOT_PASSWORD`,
  …), so nothing has to be edited in code to change them. Copy
  `infra/.env.example` to `infra/.env` (git-ignored) and override them.
- Production would instead pull credentials from a secret manager (Vault, AWS
  Secrets Manager, Kubernetes secrets) with rotation and a distinct identity per
  service, and would never bake them into an image or a compose file.

### Kafka

- The broker listens with `PLAINTEXT` and no authentication; it is only
  reachable on the internal `transactpulse` Docker network.
- Production would require TLS in transit, SASL or mTLS authentication,
  per-topic ACLs (the bronze consumer needs read on one topic and nothing else)
  and client quotas.

### Object store (MinIO / S3)

- A single `lakehouse` bucket is created with root credentials, and every job
  uses those same credentials for all prefixes.
- Production would issue least-privilege IAM policies per job — the bronze
  writer must not be able to read `gold/`, the dashboard must be read-only —
  plus server-side encryption, bucket versioning, object-lock/retention where
  regulation requires it, and access logging.

### Containers

- The Spark jobs image runs as the unprivileged `spark` user, but the Airflow
  services run as root (`user: "0:0"`) so they can write to bind-mounted local
  volumes, and the Airflow scheduler mounts the host Docker socket in order to
  launch `DockerOperator` tasks. **Mounting the Docker socket is equivalent to
  granting root on the host** — acceptable for a local demo, not for a shared
  machine.
- Production would run non-root everywhere, drop capabilities, set
  `no-new-privileges` and read-only root filesystems, pin images by digest, set
  CPU/memory limits, and use a Kubernetes executor instead of the Docker socket.

### Network exposure

- The dashboard (8501), Airflow (8088), Kafka UI (8080) and the MinIO console
  (9001) are published on `localhost` with dev logins and no TLS.
- Neither Streamlit nor Airflow should be reachable without authentication.
  Production would put SSO/OIDC in front of both, keep the data plane on a
  private network, terminate TLS at an ingress, and log access.

### Model artifacts

- `lakehouse/ml/model.py` loads the scorer with `pickle`. **Unpickling executes
  arbitrary code**, so the loader must only ever be pointed
  (via `GOLD_MODEL_PATH`) at an artifact this project produced — the
  `train_fraud_model` job writing to the `tp-models` volume, or a local
  `models/fraud_model.pkl`.
- `.pkl` files are git-ignored: model artifacts are build outputs, not source.
- Production would fetch signed artifacts from a model registry and verify
  integrity before loading, or use a non-executable serialization format (ONNX).

### Data protection

- Synthetic data means no PCI-DSS or GDPR obligations apply here. Real
  transaction data would require encryption in transit and at rest, tokenized
  or masked account identifiers, field-level access control, retention limits,
  and lineage/audit trails across the medallion layers.

---

## Reporting a vulnerability

This is a personal portfolio repository with no production deployment and no
users at risk. If you spot something wrong, please open a GitHub issue — or, if
you would rather not disclose it publicly, contact the maintainer through the
profile linked on the repository. There is no formal SLA.
