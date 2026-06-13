# 0003. Wire serialization format: JSON now, Avro-ready

- **Status:** accepted
- **Date:** 2026-06-13
- **Deciders:** Paweł Jankowicz (data engineering)

## Context and problem statement

The generator (Stage 2) publishes transaction events to the Kafka topic
`transactions.raw`, which Spark Structured Streaming later consumes into the
bronze layer (Stage 4). We must choose how records are **serialized on the wire**.
The choice trades off human readability and zero-infrastructure simplicity against
payload size, throughput, and schema enforcement.

Which serialization format should the producer use: **JSON**, **Avro (with a Schema
Registry)**, or something in between?

## Decision drivers

- **Demo ergonomics** — a reviewer should be able to open Kafka UI and *read* a
  message without tooling.
- **Minimal infrastructure** — every extra service (e.g. a Schema Registry) adds
  weight to the local `docker compose` stack.
- **Schema governance** — we still want an explicit, validated schema for records.
- **Throughput & size** — relevant at high transaction rates.
- **Portability / future-proofing** — easy to switch formats without rewriting the
  generator.

## Considered options

- **Option A — JSON (UTF-8)**: self-describing, human-readable, no extra services.
  Larger payloads, no enforced schema on the wire, types are loose (everything is
  text/number).
- **Option B — Avro + Confluent Schema Registry**: compact binary, strong schema
  evolution and enforcement, industry-standard for Kafka. Requires running and
  wiring a Schema Registry and managing `.avsc` schemas.
- **Option C — Avro without a registry (schema embedded / shipped out-of-band)**:
  compact, but loses the registry's evolution guarantees and complicates the
  consumer; awkward middle ground.

## Decision

We use **Option A — JSON** as the wire format for `transactions.raw`, behind a
small **`Serializer` abstraction** so Avro can be added later without touching the
generator or producer call sites.

Schema discipline is **not** sacrificed: records are validated against an explicit
schema in `ingestion/schema.py` *before* they are produced, so malformed events
never reach Kafka even though JSON itself is schemaless on the wire.

## Consequences

### Positive

- **Readable in Kafka UI** — instant inspectability for demos and debugging.
- **No Schema Registry container** — lighter, simpler local stack.
- Producer-side validation gives us schema guarantees without registry overhead.
- The `Serializer` interface keeps an **Avro upgrade path** open and cheap.

### Negative / trade-offs

- **Larger payloads** and higher parse cost than Avro at very high throughput.
- **No wire-level schema enforcement / evolution** — discipline lives in code and
  tests, not in a registry.
- Spark must parse JSON with an explicitly supplied schema (handled in bronze).

### Neutral / follow-ups

- If/when throughput or schema-evolution requirements grow, revisit with a
  Schema-Registry-backed Avro `Serializer` implementation; this ADR would then be
  partially superseded.
- Bronze ingestion (Stage 4) will define the matching Spark read schema.

## Links

- [ADR 0002 — Technology stack selection](0002-wybor-stacku.md)
- [Confluent — Avro, JSON, and Protobuf](https://docs.confluent.io/platform/current/schema-registry/fundamentals/serdes-develop/index.html)
