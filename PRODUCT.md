# Sky Event Ledger

Sky Event Ledger is a compact append-only event-sourcing service for systems that need deterministic aggregate replay, optimistic concurrency, idempotent writes, and tamper-evident stream verification without adopting a large event-store platform.

## Implemented product capabilities

- append-only domain-event streams;
- optimistic concurrency using `expectedVersion`;
- idempotency keys with conflict detection;
- deterministic per-stream SHA-256 hash chaining;
- fail-closed persistence loading when records are malformed, reordered, or tampered with;
- JSONL persistence suitable for a single service instance;
- aggregate replay example through the account projection;
- health, readiness, metrics, stream, append, and verification endpoints;
- 256 KiB event-data bound and safe identifier validation;
- graceful process shutdown;
- persistent Docker volume and hardened container settings;
- automated TypeScript, test, audit, and container gates.

## Commercial positioning

The standalone package can serve as:

1. an audit/event ledger for internal SaaS systems;
2. a teaching/reference event-sourcing implementation;
3. a lightweight local event store for prototypes and single-node services;
4. a SKYCOIN4444 event boundary for modules that do not require distributed consensus;
5. a foundation for commercial adapters, support, migration, or managed deployment work.

## Explicit non-claims

This version does **not** claim multi-node consensus, high availability, cross-process writer safety, database-grade fsync guarantees, Kafka/NATS replacement, global ordering, exactly-once distributed delivery, cryptographic signatures, encryption at rest, tenant RBAC, or an SLA. Those require separate implementations and verification.
