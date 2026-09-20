"""Apply the fraud model to silver data as a vectorized Spark ``pandas_udf``.

The model is loaded once per executor (module-level cache) and scores Arrow
batches (ADR 0006). A pure-pandas :func:`score_pandas` is provided for tests that
run without Spark.
"""

from __future__ import annotations

from typing import TYPE_CHECKING

from lakehouse.ml.features import features_frame
from lakehouse.ml.model import FraudModel, load_or_default

if TYPE_CHECKING:  # pragma: no cover
    import pandas as pd
    from pyspark.sql import DataFrame

#: Silver columns the scorer needs as UDF inputs.
SCORING_INPUT_COLUMNS: tuple[str, ...] = (
    "event_time",
    "amount_pln",
    "channel",
    "merchant_category",
    "country",
)

# Per-executor model cache, keyed by artifact path.
_MODEL_CACHE: dict[str, FraudModel] = {}


def _get_model(model_path: str) -> FraudModel:
    model = _MODEL_CACHE.get(model_path)
    if model is None:
        model = load_or_default(model_path)
        _MODEL_CACHE[model_path] = model
    return model


def score_pandas(pdf: pd.DataFrame, model: FraudModel) -> pd.Series:
    """Score a pandas batch of silver rows -> Series of fraud probabilities."""
    import pandas as pd

    if len(pdf) == 0:
        return pd.Series([], dtype="float64")
    features = features_frame(pdf)
    scores = model.score(features)
    return pd.Series(scores, index=pdf.index, dtype="float64")


def add_fraud_scores(
    silver: DataFrame, model_path: str, threshold: float | None = None
) -> DataFrame:
    """Add ``fraud_score`` and ``fraud_flag`` columns to a silver DataFrame.

    Args:
        silver: Silver transactions DataFrame.
        model_path: Path to the pickled model artifact (falls back to heuristic).
        threshold: Flag threshold; defaults to the model's own threshold.
    """
    from pyspark.sql import functions as F
    from pyspark.sql.functions import pandas_udf
    from pyspark.sql.types import DoubleType

    flag_threshold = threshold if threshold is not None else _get_model(model_path).threshold

    @pandas_udf(DoubleType())
    def _score_udf(batch: pd.DataFrame) -> pd.Series:  # struct -> Series
        return score_pandas(batch, _get_model(model_path))

    inputs = F.struct(*[F.col(c) for c in SCORING_INPUT_COLUMNS])
    return silver.withColumn("fraud_score", _score_udf(inputs)).withColumn(
        "fraud_flag", F.col("fraud_score") >= F.lit(flag_threshold)
    )
