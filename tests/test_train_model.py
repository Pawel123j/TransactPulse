"""Held-out evaluation recorded by the trainer: PR-AUC vs a simple predictor.

Fraud is ~2 % of the synthetic population, so ROC-AUC alone flatters. The trainer
must also record PR-AUC on the held-out split, next to the PR-AUC of the heuristic
fallback and the no-skill level (the positive rate). This is what tells the
retraining loop (see tests/test_retraining.py) whether a new artifact beats the
fallback it would replace.
"""

from __future__ import annotations

import pytest

pytest.importorskip("sklearn")

from sklearn.metrics import average_precision_score  # noqa: E402
from sklearn.model_selection import train_test_split  # noqa: E402

from lakehouse.ml.features import features_frame  # noqa: E402
from lakehouse.ml.model import HeuristicFraudModel, load_model  # noqa: E402
from lakehouse.ml.train_model import synthetic_silver_frame, train  # noqa: E402

SAMPLES, SEED = 6000, 3


@pytest.fixture(scope="module")
def trained(tmp_path_factory):
    path = tmp_path_factory.mktemp("model") / "fraud_model.pkl"
    return train(SAMPLES, SEED, str(path)), path


def _holdout():
    """Rebuild the trainer's held-out split independently of ``train``."""
    df = synthetic_silver_frame(SAMPLES, SEED)
    labels = df["is_fraud_label"].astype(int)
    _, x_test, _, y_test = train_test_split(
        features_frame(df), labels, test_size=0.25, random_state=SEED, stratify=labels
    )
    return x_test, y_test


def test_meta_records_pr_auc_against_heuristic_and_no_skill(trained) -> None:
    meta, path = trained
    x_test, y_test = _holdout()

    assert meta["holdout_n"] == len(y_test) == SAMPLES // 4
    assert meta["holdout_positive_rate"] == round(float(y_test.mean()), 4)
    model_ap = average_precision_score(y_test, load_model(path).score(x_test))
    heuristic_ap = average_precision_score(y_test, HeuristicFraudModel().score(x_test))
    assert meta["pr_auc"] == round(model_ap, 4)
    assert meta["heuristic_pr_auc"] == round(heuristic_ap, 4)


def test_both_predictors_beat_no_skill_on_holdout(trained) -> None:
    meta, _ = trained
    no_skill = meta["holdout_positive_rate"]
    assert 0.0 < no_skill < 0.1
    # Each must clearly beat a constant score; PR-AUC's floor is the positive rate.
    assert meta["pr_auc"] > 2 * no_skill
    assert meta["heuristic_pr_auc"] > 2 * no_skill
