# Security Policy — Sky Event Ledger

## Security properties implemented

- Event identifiers, aggregate identifiers, and event types are bounded and validated.
- Event data is limited to 256 KiB before append.
- `expectedVersion` provides optimistic concurrency control inside one running process.
- Idempotency keys replay matching writes and reject conflicting reuse.
- Every stream is SHA-256 hash chained from `GENESIS` through the previous event hash.
- Persistent JSONL records are validated in sequence and fail closed on malformed, reordered, duplicate-idempotency, or hash-mismatched records.
- The container runs as the unprivileged `node` user and the Compose package drops Linux capabilities and enables `no-new-privileges`.
- The service disables Express `x-powered-by` and uses a bounded JSON request body.

## Trust boundaries

The hash chain is tamper-evident, not a digital signature. An attacker who can rewrite the whole event file and recompute every hash can create a self-consistent replacement history. Protect the persistence volume with host/cloud access controls and backups.

The JSONL adapter is intentionally single-process. It does not implement distributed locking, multi-process safe appends, transactional database durability, quorum replication, or consensus.

The HTTP API currently has no built-in authentication or TLS. Bind it to localhost/private networks or place it behind an authenticated TLS gateway before exposing it to untrusted networks.

## Production extension path

For stronger deployments, add a database-backed adapter with transactional uniqueness constraints, authenticated service identity, encryption at rest, signed event checkpoints, immutable backup retention, OpenTelemetry, and tested restore procedures.

## Vulnerability reporting

Do not publish secrets or exploit details in a public issue. Use the repository owner's private security/contact channel where available.
