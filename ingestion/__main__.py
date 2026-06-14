"""Command-line entrypoint for the transaction generator.

Examples:
    # Stream ~50 tx/s to Kafka for 60s
    python -m ingestion --rate 50 --duration 60

    # Burst mode, deterministic, to Kafka
    python -m ingestion --rate 30 --burst --seed 42

    # No Kafka required: print JSON records to stdout
    python -m ingestion --rate 5 --duration 3 --dry-run
"""

from __future__ import annotations

import argparse
import json
import logging
import sys

from ingestion.config import GeneratorConfig, KafkaConfig
from ingestion.emitter import run_emitter
from ingestion.producer import KafkaTransactionProducer, Sink, StdoutSink


def _build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        prog="python -m ingestion",
        description="Generate synthetic financial transactions and publish them to Kafka.",
    )
    parser.add_argument("--rate", type=float, help="Target transactions per second.")
    parser.add_argument(
        "--duration",
        type=int,
        dest="duration_seconds",
        help="Run duration in seconds (0 = run until interrupted).",
    )
    parser.add_argument(
        "--fraud-rate",
        type=float,
        dest="fraud_rate",
        help="Fraction of fraudulent transactions (e.g. 0.003).",
    )
    parser.add_argument("--seed", type=int, help="RNG seed for reproducible output.")
    parser.add_argument("--burst", action="store_true", help="Enable periodic traffic bursts.")
    parser.add_argument(
        "--no-seasonality", action="store_true", help="Disable hourly rate seasonality."
    )
    parser.add_argument(
        "--bootstrap-servers",
        dest="bootstrap_servers",
        help="Kafka bootstrap servers (host:port,...).",
    )
    parser.add_argument("--topic", help="Destination Kafka topic.")
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Print records to stdout instead of producing to Kafka.",
    )
    parser.add_argument(
        "--max-records", type=int, dest="max_records", help="Stop after N records (smoke tests)."
    )
    parser.add_argument("-v", "--verbose", action="store_true", help="Verbose logging.")
    return parser


def _generator_config(args: argparse.Namespace) -> GeneratorConfig:
    """Merge CLI overrides on top of environment-derived defaults."""
    cfg = GeneratorConfig.from_env()
    if args.rate is not None:
        cfg.rate = args.rate
    if args.duration_seconds is not None:
        cfg.duration_seconds = args.duration_seconds
    if args.fraud_rate is not None:
        cfg.fraud_rate = args.fraud_rate
    if args.seed is not None:
        cfg.seed = args.seed
    if args.burst:
        cfg.burst_enabled = True
    if args.no_seasonality:
        cfg.apply_seasonality = False
    cfg.__post_init__()  # re-validate after overrides
    return cfg


def _kafka_config(args: argparse.Namespace) -> KafkaConfig:
    cfg = KafkaConfig.from_env()
    if args.bootstrap_servers:
        cfg.bootstrap_servers = args.bootstrap_servers
    if args.topic:
        cfg.topic = args.topic
    return cfg


def main(argv: list[str] | None = None) -> int:
    """CLI entrypoint. Returns a process exit code."""
    args = _build_parser().parse_args(argv)
    logging.basicConfig(
        level=logging.DEBUG if args.verbose else logging.INFO,
        format="%(asctime)s %(levelname)s %(name)s: %(message)s",
        stream=sys.stderr,
    )
    log = logging.getLogger("ingestion")

    gen_cfg = _generator_config(args)

    if args.dry_run:
        sink: Sink = StdoutSink()
        log.info("dry-run: writing JSON to stdout (no Kafka)")
        stats = run_emitter(gen_cfg, sink, stop_after=args.max_records)
        failed = sink.failed
    else:
        kafka_cfg = _kafka_config(args)
        log.info("producing to topic=%s on %s", kafka_cfg.topic, kafka_cfg.bootstrap_servers)
        with KafkaTransactionProducer(kafka_cfg) as producer:
            stats = run_emitter(gen_cfg, producer, stop_after=args.max_records)
        failed = producer.failed
        if failed:
            log.error("%d delivery failure(s)", failed)

    log.info("run summary: %s", json.dumps(stats.as_dict()))
    return 1 if failed else 0


if __name__ == "__main__":  # pragma: no cover
    raise SystemExit(main())
