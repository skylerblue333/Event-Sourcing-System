import { randomUUID } from 'crypto';

export interface DomainEvent<T = unknown> {
  id: string;
  aggregateId: string;
  sequence: number;
  type: string;
  data: T;
  timestamp: string;
}

export class ConcurrencyError extends Error {
  constructor(message = 'aggregate version conflict') {
    super(message);
    this.name = 'ConcurrencyError';
  }
}

export class EventStore {
  private readonly streams = new Map<string, DomainEvent[]>();

  append<T>(
    aggregateId: string,
    type: string,
    data: T,
    expectedVersion?: number,
  ): DomainEvent<T> {
    const stream = this.streams.get(aggregateId) ?? [];
    const currentVersion = stream.length;

    if (expectedVersion !== undefined && expectedVersion !== currentVersion) {
      throw new ConcurrencyError(
        `expected version ${expectedVersion}, current version ${currentVersion}`,
      );
    }

    const event: DomainEvent<T> = {
      id: randomUUID(),
      aggregateId,
      sequence: currentVersion + 1,
      type,
      data,
      timestamp: new Date().toISOString(),
    };

    stream.push(event);
    this.streams.set(aggregateId, stream);
    return event;
  }

  getStream(aggregateId: string): readonly DomainEvent[] {
    return [...(this.streams.get(aggregateId) ?? [])];
  }

  version(aggregateId: string): number {
    return this.streams.get(aggregateId)?.length ?? 0;
  }
}
