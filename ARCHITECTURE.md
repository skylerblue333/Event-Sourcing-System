# Architecture — Event Sourcing System

## Core model

The repository now has an executable event-sourcing path:

```text
HTTP command
    |
    v
validation
    |
    v
EventStore.append(aggregateId, type, data, expectedVersion)
    |
    +---- version mismatch ---> 409 Conflict
    |
    v
append-only stream
    |
    v
AccountAggregate.apply(event)
    |
    v
reconstructed state / query response
```

## Event contract

Every event contains:

- globally unique event ID;
- aggregate ID;
- monotonically increasing stream sequence;
- event type;
- application payload;
- UTC timestamp.

## Concurrency

`expectedVersion` implements optimistic concurrency. A writer must provide the stream version it observed when strict compare-and-append semantics are required. A stale writer receives `409 Conflict` instead of silently overwriting another command's state transition.

## Replay

Aggregates are reconstructed by applying events in sequence order. The account example supports creation, deposits, and withdrawals and exposes a deterministic snapshot containing the aggregate version.

## Production boundary

The current store is in-memory and is intentionally not represented as a durable enterprise event database. Production adoption requires a transactional durable event store, atomic append semantics, snapshots for long streams, retention policy, reliable publication, idempotent consumers, authentication/authorization, backup/restore, and disaster recovery.

## Verification

GitHub Actions builds the TypeScript project, runs Jest integration tests serially, and executes a high-severity dependency audit on pushes and pull requests.
