import { mkdtempSync, readFileSync, rmSync, writeFileSync } from 'fs';
import { tmpdir } from 'os';
import { join } from 'path';
import {
  ConcurrencyError,
  EventStore,
  IdempotencyConflictError,
  IntegrityError,
} from '../src/eventStore';

describe('Sky Event Ledger store', () => {
  it('enforces optimistic concurrency', () => {
    const store = new EventStore();
    store.append('acct-1', 'ACCOUNT_CREATED', { initialBalance: 10 }, 0);
    expect(() =>
      store.append('acct-1', 'FUNDS_DEPOSITED', { amount: 5 }, 0),
    ).toThrow(ConcurrencyError);
  });

  it('replays the same event for a matching idempotency key', () => {
    const store = new EventStore();
    const first = store.append('acct-2', 'ACCOUNT_CREATED', { initialBalance: 10 }, 0, 'idem-1');
    const replay = store.append('acct-2', 'ACCOUNT_CREATED', { initialBalance: 10 }, 0, 'idem-1');
    expect(replay.id).toBe(first.id);
    expect(store.version('acct-2')).toBe(1);
  });

  it('rejects reuse of an idempotency key for different content', () => {
    const store = new EventStore();
    store.append('acct-3', 'ACCOUNT_CREATED', { initialBalance: 10 }, 0, 'idem-2');
    expect(() =>
      store.append('acct-3', 'ACCOUNT_CREATED', { initialBalance: 11 }, 1, 'idem-2'),
    ).toThrow(IdempotencyConflictError);
  });

  it('persists and reloads an append-only stream', () => {
    const directory = mkdtempSync(join(tmpdir(), 'sky-event-ledger-'));
    const filePath = join(directory, 'events.jsonl');
    try {
      const store = new EventStore({ filePath });
      store.append('acct-4', 'ACCOUNT_CREATED', { initialBalance: 100 }, 0, 'idem-3');
      store.append('acct-4', 'FUNDS_DEPOSITED', { amount: 25 }, 1, 'idem-4');

      const reloaded = new EventStore({ filePath });
      expect(reloaded.version('acct-4')).toBe(2);
      expect(reloaded.verifyStream('acct-4')).toBe(true);
      expect(reloaded.eventCount()).toBe(2);
    } finally {
      rmSync(directory, { recursive: true, force: true });
    }
  });

  it('fails closed when a persisted record is tampered with', () => {
    const directory = mkdtempSync(join(tmpdir(), 'sky-event-ledger-'));
    const filePath = join(directory, 'events.jsonl');
    try {
      const store = new EventStore({ filePath });
      store.append('acct-5', 'ACCOUNT_CREATED', { initialBalance: 100 }, 0);
      const raw = readFileSync(filePath, 'utf8').replace('100', '999');
      writeFileSync(filePath, raw, { mode: 0o600 });
      expect(() => new EventStore({ filePath })).toThrow(IntegrityError);
    } finally {
      rmSync(directory, { recursive: true, force: true });
    }
  });

  it('rejects unsafe identifiers and oversized event payloads', () => {
    const store = new EventStore();
    expect(() => store.append('../escape', 'EVENT', {}, 0)).toThrow();
    expect(() => store.append('safe', 'EVENT', { value: 'x'.repeat(300_000) }, 0)).toThrow();
  });
});
