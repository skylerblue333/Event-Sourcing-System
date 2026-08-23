import { createHash, randomUUID } from 'crypto';

export interface DomainEvent<T = unknown> {
  id: string;
  aggregateId: string;
  sequence: number;
  type: string;
  data: T;
  timestamp: string;
  previousHash: string;
  hash: string;
  idempotencyKey?: string;
}

export class ConcurrencyError extends Error {
  constructor(message = 'aggregate version conflict') {
    super(message);
    this.name = 'ConcurrencyError';
  }
}

export class EventStore {
  private readonly streams = new Map<string, DomainEvent[]>();
  private readonly idempotency = new Map<string, string>();

  append<T>(
    aggregateId: string,
    type: string,
    data: T,
    expectedVersion?: number,
    idempotencyKey?: string,
  ): DomainEvent<T> {
    if (idempotencyKey) {
      const existingEventId = this.idempotency.get(idempotencyKey);
      if (existingEventId) {
        const existing = this.findById(existingEventId);
        if (existing) return existing as DomainEvent<T>;
      }
    }

    const stream = this.streams.get(aggregateId) ?? [];
    const currentVersion = stream.length;

    if (expectedVersion !== undefined && expectedVersion !== currentVersion) {
      throw new ConcurrencyError(
        `expected version ${expectedVersion}, current version ${currentVersion}`,
      );
    }

    const eventBase = {
      id: randomUUID(),
      aggregateId,
      sequence: currentVersion + 1,
      type,
      data,
      timestamp: new Date().toISOString(),
      previousHash: stream.at(-1)?.hash ?? 'GENESIS',
      ...(idempotencyKey ? { idempotencyKey } : {}),
    };

    const hash = createHash('sha256')
      .update(JSON.stringify(eventBase))
      .digest('hex');

    const event: DomainEvent<T> = { ...eventBase, hash };
    stream.push(event);
    this.streams.set(aggregateId, stream);

    if (idempotencyKey) this.idempotency.set(idempotencyKey, event.id);
    return event;
  }

  getStream(aggregateId: string): readonly DomainEvent[] {
    return [...(this.streams.get(aggregateId) ?? [])];
  }

  version(aggregateId: string): number {
    return this.streams.get(aggregateId)?.length ?? 0;
  }

  verifyStream(aggregateId: string): boolean {
    const stream = this.streams.get(aggregateId) ?? [];
    let previousHash = 'GENESIS';

    for (let index = 0; index < stream.length; index += 1) {
      const event = stream[index];
      const { hash, ...eventBase } = event;
      if (event.sequence !== index + 1 || event.previousHash !== previousHash) return false;
      const expectedHash = createHash('sha256')
        .update(JSON.stringify(eventBase))
        .digest('hex');
      if (expectedHash !== hash) return false;
      previousHash = hash;
    }

    return true;
  }

  private findById(eventId: string): DomainEvent | undefined {
    for (const stream of this.streams.values()) {
      const event = stream.find((candidate) => candidate.id === eventId);
      if (event) return event;
    }
    return undefined;
  }
}
