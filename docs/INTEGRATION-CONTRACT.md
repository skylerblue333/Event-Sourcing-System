# Institutional Integration Contract

## Role

`Event-Sourcing-System` is the durable domain-event boundary for auditable workflows. It is appropriate for order state, wallet/account state, fulfillment state, audit trails, and other domains that benefit from immutable event history.

## New verified capabilities on this branch

- idempotency-key support for retry-safe appends;
- SHA-256 hash chaining within each aggregate stream;
- stream integrity verification endpoint;
- preserved optimistic concurrency control;
- tests for idempotency and integrity verification.

## Integration sequence

`domain command -> Event-Sourcing-System -> immutable event stream -> projections / Py-Data-Pipeline / audit views`

Financial or regulated workflows still require durable storage, transactional guarantees, key management, access control, reconciliation, and domain-specific compliance controls before production use.

## Evidence boundary

The event store is currently in-memory. The new integrity and idempotency behavior is implemented and testable, but durable production persistence is not claimed until a real database/event log backend and deployment evidence are verified.
