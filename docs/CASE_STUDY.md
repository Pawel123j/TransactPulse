# TransactPulse — case study

A walk through the engineering decisions behind a streaming fraud-detection
lakehouse, written to be discussed rather than skimmed.

---

## Problem

Card fraud is detected in two places, and they pull in opposite directions.

**In the stream**, where a decision must be made in the seconds before a payment
is authorised. There is no time to join against history, and the cost of a wrong
answer is asymmetric: a blocked legitimate payment annoys a customer, a missed
fraudulent one costs money.

**In the batch**, where analysts reconstruct what happened, tune thresholds and
retrain models. Here completeness matters more than latency, and the same
transaction may need to be corrected days later when a chargeback arrives.

A system that only does the first has no memory. One that only does the second
is always too late. TransactPulse exists to hold both paths over one storage
layer without maintaining two contradictory definitions of "a transaction".

---

## Constraints

Self-imposed, and they shaped almost every decision below:

| Constraint | Consequence |
|---|---|
| **No paid services** | MinIO instead of S3, local Kafka instead of managed, DuckDB instead of a warehouse |
| **Runs on one laptop** | Spark in local mode, sample sizes bounded, no cluster assumptions |
| **Reproducible from scratch** | Synthetic generator rather than a downloaded dataset; `docker compose up` is the whole setup |
| **Honest about what is verified** | Anything not actually executed is labelled as such, including in this document |

The "one laptop" constraint is the interesting one. It rules out the architecture
most tutorials reach for, and forces decisions that happen to be the same ones a
small team would make before they have platform engineers.

---

## Architecture

```
generator → Kafka → Spark Structured Streaming → Bronze (Delta)
                                                     ↓
                                    Silver: cleanse · dedup · PLN · DQ
                                                     ↓
                                          data-quality gate (blocking)
                                                     ↓
                    ┌────────────────────────────────┼────────────────┐
                    ↓                                ↓                ↓
             gold aggregates                  fraud scoring        DuckDB
                                                     ↓                ↓
                                              PSI / KS drift     Streamlit
                                                     ↓
                                             retraining gate
                                                     ↓
                                          train_fraud_model DAG
```

Medallion layering (bronze → silver → gold) is not decoration here. It answers a
specific question: *where does a correction go?* Bronze is append-only and never
edited, so raw history is always recoverable. Silver holds the cleansed,
deduplicated, currency-normalised view and is the only layer that gets rewritten.
Gold is derived and therefore disposable — anything in it can be rebuilt.

---

## Engineering decisions

### Delta Lake over plain Parquet

Late-arriving events are not an edge case in payments; they are the normal case.
A transaction can be amended, reversed or chargebacked days after it settles.
With plain Parquet, applying a correction means rewriting partitions by hand and
hoping no reader is mid-scan. Delta gives `MERGE` and snapshot isolation, which
turns a correction into an ordinary operation instead of a maintenance window.

The cost is a dependency on a specific Spark/Delta version pairing, which is
pinned and occasionally painful to move.

### A blocking data-quality gate, not a warning

The gate between silver and gold **fails the pipeline** rather than logging a
warning. This is deliberate and it is the decision most likely to be questioned.

The reasoning: a fraud model trained on silently corrupted data does not fail
loudly. It produces plausible scores that are wrong, and the failure surfaces
weeks later as unexplained drift. A red DAG at 03:00 is cheap; a quarter of
mis-scored transactions is not.

Bad records go to a quarantine path with `dq_errors` attached, so the gate
failing does not mean the data is lost.

### Scoring as a `pandas_udf`, not a service call

Fraud scoring runs inside Spark as a vectorised UDF rather than calling out to a
model server. For batch scoring over millions of rows, a per-row network hop
dominates everything else. The trade-off is that the model artifact must be
distributed to executors — handled here by a shared volume.

The model itself is deliberately simple (scikit-learn, with a heuristic
fallback). The interesting engineering is in the pipeline around it, not in
squeezing another point of AUC out of the estimator.

### Closing the drift loop

The pipeline computed PSI and KS, wrote a report, and stopped. Retraining ran on
a weekly schedule regardless of what the drift said. That is monitoring without
a feedback path — the measurement existed but nothing consumed it.

The gate now reads the drift report and triggers retraining when PSI crosses the
`significant` band. Two guards turned out to matter more than the threshold
itself, and both came from reading the PSI implementation rather than from
theory:

**A missing reference is not "stable".** `population_stability_index` returns
`0.0` when either distribution is empty, and `classify_psi(0.0)` is `"stable"`.
A model that never received a training reference therefore produces a drift
report **byte-for-byte indistinguishable** from a perfectly calibrated one.
Folding that into "stable" would hide a broken monitoring setup behind a green
signal. The policy reports it as *not comparable*.

**Too few rows is not evidence.** PSI computed over a dozen live values against
a 50 000-row reference is arithmetic, not a signal.

The policy is a pure function over the report — no Spark, no Airflow, no
filesystem — because the failure mode that matters is not a crash but a *wrong
decision*, and wrong decisions are only testable in isolation.

---

## Challenges

### Airflow cannot see what the jobs write

The drift report lands on the `tp-reports` Docker volume, which is mounted into
the job containers but **not** into the Airflow containers. A
`BranchPythonOperator` running in the scheduler has nothing to read.

Three options: mount the volume into Airflow (couples the two compose stacks and
needs an `external: true` declaration that is easy to get subtly wrong), fail the
task to signal "no retrain" (semantically wrong — a healthy pipeline would page
someone), or move the decision to where the data is.

The third won. The gate runs in the jobs image and returns its verdict as the
last line of stdout, which `do_xcom_push` publishes as an XCom value. The branch
operator then reads a string rather than a file. The contract — exactly `RETRAIN`
or `NO_RETRAIN`, nothing after it — is documented in the module and covered by a
test that asserts the verdict is the final stdout line.

### Spark in CI without a cluster

Spark tests need a JVM, matching Delta jars and several minutes of warm-up. Two
test suites exist: pure-Python tests that run everywhere in under two seconds,
and Spark transformation tests that skip cleanly when PySpark is absent. CI runs
both in separate jobs so a Spark problem is distinguishable from a logic problem.

---

## Bugs found during validation

Listed because they are the honest part of the story, and because each one
survived a green test suite.

| Bug | How it surfaced | Why tests missed it |
|---|---|---|
| **Drift measured but never acted on** | Reading the DAG end-to-end while closing the loop | Nothing was broken — the feature simply did not exist |
| **"Stable" indistinguishable from "no reference"** | Reading `population_stability_index` before writing the policy | No test asserted what an empty reference *should* mean |
| **Regression test passed on unfixed code** (in a sibling project, same lesson) | Deliberately reverting the fix to check the test failed | The test's comment described a bug the assertions did not exercise |

The third is the one worth keeping. A test that passes on broken code is worse
than no test: it converts an open question into false confidence. The habit that
came out of it — **revert the fix, watch the test fail, restore the fix** — is
now applied to every regression test here, and mutation checks accompany the
retraining policy in the commit history.

---

## Testing strategy

Five layers, because "tests pass" and "the system works" are different claims:

| Layer | What it covers | Where |
|---|---|---|
| **Unit** | Transformations, DQ rules, scoring, drift maths, retraining policy | `pytest`, ~140 tests, runs in seconds |
| **Spark transformation** | Bronze/silver/gold logic against a real Spark session | separate CI job, skips without PySpark |
| **Build validation** | `ruff check` and `ruff format --check` | CI lint job |
| **Security** | `bandit` (SAST) and `gitleaks` (secrets), both blocking | CI security job |
| **Runtime** | `docker compose up`, generator → dashboard | **manual — not automated** |

The last row is stated plainly because it is the honest boundary. CI proves the
transformations are correct; it does not prove the stack comes up.

Mutation checks are used where the logic is a policy rather than a computation.
For the retraining policy: removing the empty-reference guard fails 4 tests,
removing the sample-size threshold fails 1, and widening the trigger bands fails
1. A test suite that survives those mutations would not be worth running.

---

## Security considerations

| Concern | Approach |
|---|---|
| **Secrets** | None in the repository; `gitleaks` blocks on every push |
| **SAST** | `bandit` over all Python packages, blocking |
| **Dependency CVEs** | `pip-audit` gate |
| **Synthetic data only** | The generator produces fake transactions — no real payment data ever enters the system, by construction |
| **Credentials in compose** | Default MinIO credentials are development values and documented as such; the stack is not intended to be exposed |

The synthetic-data decision is worth stating as a security property rather than a
convenience. A portfolio project handling real transaction data would be a
liability; one that cannot, structurally, is not.

---

## Lessons learned

**Green CI is not a working system.** Every bug in the table above passed a full
test suite. The tests were correct about what they tested and silent about the
rest. The lesson is not "write more tests" but "know which claim each test
supports" — which is why the testing table above separates *validated in CI*
from *validated manually*.

**Ambiguous defaults hide failures.** `classify_psi(0.0) == "stable"` is
defensible in isolation and dangerous in a pipeline, because it makes "I measured
nothing" and "I measured and it was fine" produce identical output. Any function
whose neutral return value doubles as a healthy one deserves a second look.

**Measurement without a consumer is decoration.** The drift metrics were correct
and nobody read them. Adding a consumer was 100 lines; deciding *what the
consumer should do when the signal is unreliable* was the actual work.

**Constraints improve architecture.** "No paid services, one laptop" forced
choices that a cloud budget would have papered over, and the result is a system
that can be run end-to-end by anyone who clones it.

---

## Future work

Ordered by value per unit of effort, not by novelty:

1. **Automated runtime validation** — bring the stack up in CI and assert the
   dashboard serves data. Closes the last gap in the testing table.
2. **Run the retraining loop end-to-end** on a machine with Docker. The policy
   and gate are tested; the DAG wiring is not.
3. **Prometheus + Grafana** for pipeline metrics — currently there is no
   operational view beyond Airflow's own UI.
4. **MLflow / model registry** — the model is a pickle on a volume. Versioning
   and lineage would make the retraining loop auditable.
5. **Feature store** — only worth it once more than one consumer exists.

Deliberately **not** planned: Kubernetes, cloud migration, a service mesh. None
of them solve a problem this system currently has, and each would obscure the
parts worth reading.
