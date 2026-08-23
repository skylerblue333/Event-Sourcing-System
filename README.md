# Sky Event Ledger

Sky Event Ledger is a focused TypeScript event-sourcing service for append-only domain history, optimistic concurrency, deterministic replay, idempotent writes, and tamper-evident stream verification.

> **Status:** productization branch under exact-head CI verification. This is a single-node event ledger, not a distributed event database.

## Core behavior

Each event contains:

- immutable event ID;
- aggregate/stream ID;
- monotonic sequence number;
- event type and JSON data;
- timestamp;
- optional idempotency key;
- previous-event hash;
- SHA-256 event hash.

On startup, persisted records are replayed and validated. Malformed JSON, broken sequence numbers, duplicate idempotency keys, previous-hash mismatches, or event-hash mismatches fail closed.

## Quick start

```bash
npm install
npm run check
npm test
npm run build
EVENT_STORE_FILE=./data/events.jsonl npm start
```

Or run the hardened standalone package:

```bash
docker compose up --build
```

The Compose configuration persists `/data/events.jsonl` in a named volume and binds the API to localhost by default.

## API

| Method | Path | Purpose |
|---|---|---|
| GET | `/healthz` | liveness |
| GET | `/readyz` | readiness and persistence mode |
| GET | `/metrics` | stream/event counts and uptime |
| POST | `/api/v1/events` | append an event |
| GET | `/api/v1/events/:aggregateId` | replay/read a stream |
| GET | `/api/v1/events/:aggregateId/verify` | verify stream hash chain |
| GET | `/api/v1/accounts/:id` | example aggregate projection |

Append example:

```bash
curl -X POST http://127.0.0.1:3000/api/v1/events \
  -H 'Content-Type: application/json' \
  -H 'Idempotency-Key: order-123-create-v1' \
  -d '{
    "aggregateId":"order-123",
    "type":"ORDER_CREATED",
    "expectedVersion":0,
    "data":{"customerId":"customer-9"}
  }'
```

Reusing the same idempotency key with the same event content returns the original event without incrementing the stream. Reusing it for different content returns a conflict.

## Architecture

```text
client
  |
  v
Express API
  |
  +--> validation / idempotency / expectedVersion
  |
  v
EventStore
  |
  +--> per-aggregate ordered stream
  +--> SHA-256 previousHash -> hash chain
  +--> replay / verify
  |
  v
optional append-only JSONL persistence
```

## Verification gates

GitHub Actions is configured to run Node 22, TypeScript checking, production compilation, Jest tests, high-severity dependency audit, Docker image build, and Compose validation.

## Product boundary

Sky Event Ledger does **not** claim distributed consensus, HA replication, global ordering, cross-process writer safety, exactly-once distributed delivery, signed events, encryption at rest, built-in authentication, or an SLA. See `PRODUCT.md` and `SECURITY.md` for the exact commercial and security boundaries.

## License

MIT.
