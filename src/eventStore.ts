import { createHash, randomUUID } from 'crypto';
import { appendFileSync, existsSync, mkdirSync, readFileSync } from 'fs';
import { dirname } from 'path';

export interface DomainEvent<T = unknown> {
  id: string;
  aggregateId: string;
  sequence: number;
  type: string;
  data: T;
  timestamp: string;
  idempotencyKey?: string;
  previousHash: string;
  hash: string;
}

export interface EventStoreOptions {
  filePath?: string;
}

export class ConcurrencyError extends Error {
  constructor(message = 'aggregate version conflict') {
    super(message);
    this.name = 'ConcurrencyError';
  }
}

export class IdempotencyConflictError extends Error {
  constructor(message = 'idempotency key already used for another event') {
    super(message);
    this.name = 'IdempotencyConflictError';
  }
}

export class IntegrityError extends Error {
  constructor(message = 'event stream integrity check failed') {
    super(message);
    this.name = 'IntegrityError';
  }
}

function canonical(value: unknown): string {
  if (value === null || typeof value !== 'object') return JSON.stringify(value);
  if (Array.isArray(value)) return `[${value.map(canonical).join(',')}]`;
  const object = value as Record<string, unknown>;
  return `{${Object.keys(object)
    .sort()
    .map((key) => `${JSON.stringify(key)}:${canonical(object[key])}`)
    .join(',')}}`;
}

function eventHash(event: Omit<DomainEvent, 'hash'>): string {
  return createHash('sha256').update(canonical(event)).digest('hex');
}

function assertIdentifier(value: string, label: string): void {
  if (!/^[A-Za-z0-9][A-Za-z0-9._:-]{0,127}$/.test(value)) {
    throw new Error(`${label} must be 1-128 safe identifier characters`);
  }
}

export class EventStore {
  private readonly streams = new Map<string, DomainEvent[]>();
  private readonly idempotency = new Map<string, DomainEvent>();
  private readonly filePath?: string;

  constructor(options: EventStoreOptions = {}) {
    this.filePath = options.filePath;
    if (this.filePath) this.load();
  }

  append<T>(
    aggregateId: string,
    type: string,
    data: T,
    expectedVersion?: number,
    idempotencyKey?: string,
  ): DomainEvent<T> {
    assertIdentifier(aggregateId, 'aggregateId');
    assertIdentifier(type, 'type');
    if (idempotencyKey) assertIdentifier(idempotencyKey, 'idempotencyKey');

    const serializedData = canonical(data);
    if (Buffer.byteLength(serializedData, 'utf8') > 256 * 1024) {
      throw new Error('event data exceeds 256 KiB');
    }

    if (idempotencyKey) {
      const existing = this.idempotency.get(idempotencyKey);
      if (existing) {
        if (
          existing.aggregateId === aggregateId &&
          existing.type === type &&
          canonical(existing.data) === serializedData
        ) {
          return existing as DomainEvent<T>;
        }
        throw new IdempotencyConflictError();
      }
    }

    const stream = this.streams.get(aggregateId) ?? [];
    const currentVersion = stream.length;
    if (expectedVersion !== undefined && expectedVersion !== currentVersion) {
      throw new ConcurrencyError(
        `expected version ${expectedVersion}, current version ${currentVersion}`,
      );
    }

    const base: Omit<DomainEvent<T>, 'hash'> = {
      id: randomUUID(),
      aggregateId,
      sequence: currentVersion + 1,
      type,
      data,
      timestamp: new Date().toISOString(),
      idempotencyKey,
      previousHash: stream.at(-1)?.hash ?? 'GENESIS',
    };
    const event: DomainEvent<T> = { ...base, hash: eventHash(base) };

    if (this.filePath) this.persist(event);
    stream.push(event);
    this.streams.set(aggregateId, stream);
    if (idempotencyKey) this.idempotency.set(idempotencyKey, event);
    return event;
  }

  getStream(aggregateId: string): readonly DomainEvent[] {
    assertIdentifier(aggregateId, 'aggregateId');
    return [...(this.streams.get(aggregateId) ?? [])];
  }

  version(aggregateId: string): number {
    return this.streams.get(aggregateId)?.length ?? 0;
  }

  verifyStream(aggregateId: string): boolean {
    const stream = this.getStream(aggregateId);
    let previousHash = 'GENESIS';
    for (let index = 0; index < stream.length; index += 1) {
      const event = stream[index];
      if (event.sequence !== index + 1 || event.previousHash !== previousHash) return false;
      const { hash, ...base } = event;
      if (eventHash(base) !== hash) return false;
      previousHash = hash;
    }
    return true;
  }

  streamCount(): number {
    return this.streams.size;
  }

  eventCount(): number {
    let total = 0;
    for (const stream of this.streams.values()) total += stream.length;
    return total;
  }

  private persist(event: DomainEvent): void {
    if (!this.filePath) return;
    mkdirSync(dirname(this.filePath), { recursive: true });
    appendFileSync(this.filePath, `${JSON.stringify(event)}\n`, { encoding: 'utf8', mode: 0o600 });
  }

  private load(): void {
    if (!this.filePath || !existsSync(this.filePath)) return;
    const lines = readFileSync(this.filePath, 'utf8').split('\n').filter(Boolean);
    for (const [index, line] of lines.entries()) {
      let event: DomainEvent;
      try {
        event = JSON.parse(line) as DomainEvent;
      } catch {
        throw new IntegrityError(`invalid JSON record at line ${index + 1}`);
      }
      assertIdentifier(event.aggregateId, 'aggregateId');
      assertIdentifier(event.type, 'type');
      if (event.idempotencyKey) assertIdentifier(event.idempotencyKey, 'idempotencyKey');
      const stream = this.streams.get(event.aggregateId) ?? [];
      const expectedSequence = stream.length + 1;
      const expectedPreviousHash = stream.at(-1)?.hash ?? 'GENESIS';
      const { hash, ...base } = event;
      if (
        event.sequence !== expectedSequence ||
        event.previousHash !== expectedPreviousHash ||
        eventHash(base) !== hash
      ) {
        throw new IntegrityError(`tampered or out-of-order record at line ${index + 1}`);
      }
      stream.push(event);
      this.streams.set(event.aggregateId, stream);
      if (event.idempotencyKey) {
        if (this.idempotency.has(event.idempotencyKey)) {
          throw new IntegrityError(`duplicate idempotency key at line ${index + 1}`);
        }
        this.idempotency.set(event.idempotencyKey, event);
      }
    }
  }
}
