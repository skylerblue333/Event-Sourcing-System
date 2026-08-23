import cors from 'cors';
import express from 'express';
import { AccountAggregate } from './account';
import {
  ConcurrencyError,
  EventStore,
  IdempotencyConflictError,
  IntegrityError,
} from './eventStore';

const filePath = process.env.EVENT_STORE_FILE?.trim() || undefined;

export const eventStore = new EventStore({ filePath });
export const app = express();

app.disable('x-powered-by');
app.use(cors({ origin: false }));
app.use(express.json({ limit: '256kb', strict: true }));

app.get('/healthz', (_req, res) => {
  res.json({ status: 'ok', service: 'sky-event-ledger' });
});

app.get('/readyz', (_req, res) => {
  res.json({ status: 'ready', persistent: Boolean(filePath) });
});

app.get('/metrics', (_req, res) => {
  res.json({
    streams: eventStore.streamCount(),
    events: eventStore.eventCount(),
    persistent: Boolean(filePath),
    uptimeSeconds: Math.floor(process.uptime()),
  });
});

// Backward-compatible alias retained for existing clients.
app.get('/health', (_req, res) => {
  res.json({
    status: 'healthy',
    domain: 'event-sourcing-system',
    uptime: process.uptime(),
  });
});

app.post('/api/v1/process', (req, res) => {
  const { payload } = req.body ?? {};
  if (typeof payload !== 'string' || payload.length === 0 || payload.length > 65536) {
    return res.status(400).json({ error: 'payload must be a non-empty string up to 64 KiB' });
  }

  try {
    const event = eventStore.append(
      'process',
      'PROCESS_REQUESTED',
      { payload },
      undefined,
      req.header('Idempotency-Key') || undefined,
    );
    return res.status(201).json({
      success: true,
      processed: payload,
      eventId: event.id,
      eventHash: event.hash,
      timestamp: event.timestamp,
    });
  } catch (error) {
    if (error instanceof IdempotencyConflictError) {
      return res.status(409).json({ error: error.message });
    }
    return res.status(400).json({ error: error instanceof Error ? error.message : 'invalid request' });
  }
});

app.post('/api/v1/events', (req, res) => {
  const { aggregateId, type, data, expectedVersion } = req.body ?? {};
  if (typeof aggregateId !== 'string' || typeof type !== 'string') {
    return res.status(400).json({ error: 'aggregateId and type are required' });
  }
  if (
    expectedVersion !== undefined &&
    (!Number.isSafeInteger(expectedVersion) || expectedVersion < 0)
  ) {
    return res.status(400).json({ error: 'expectedVersion must be a non-negative safe integer' });
  }

  try {
    const event = eventStore.append(
      aggregateId,
      type,
      data ?? {},
      expectedVersion,
      req.header('Idempotency-Key') || undefined,
    );
    return res.status(201).json(event);
  } catch (error) {
    if (error instanceof ConcurrencyError || error instanceof IdempotencyConflictError) {
      return res.status(409).json({ error: error.message });
    }
    if (error instanceof IntegrityError) {
      return res.status(503).json({ error: 'event store integrity failure' });
    }
    return res.status(400).json({ error: error instanceof Error ? error.message : 'invalid event' });
  }
});

app.get('/api/v1/events/:aggregateId', (req, res) => {
  try {
    const stream = eventStore.getStream(req.params.aggregateId);
    return res.json({ aggregateId: req.params.aggregateId, version: stream.length, events: stream });
  } catch (error) {
    return res.status(400).json({ error: error instanceof Error ? error.message : 'invalid aggregate' });
  }
});

app.get('/api/v1/events/:aggregateId/verify', (req, res) => {
  try {
    const valid = eventStore.verifyStream(req.params.aggregateId);
    return res.status(valid ? 200 : 409).json({ aggregateId: req.params.aggregateId, valid });
  } catch (error) {
    return res.status(400).json({ error: error instanceof Error ? error.message : 'invalid aggregate' });
  }
});

app.get('/api/v1/accounts/:id', (req, res) => {
  const aggregate = new AccountAggregate(req.params.id);
  try {
    for (const event of eventStore.getStream(req.params.id)) aggregate.apply(event);
  } catch {
    return res.status(422).json({ error: 'invalid account event stream' });
  }
  return res.json(aggregate.snapshot());
});

app.use((_req, res) => res.status(404).json({ error: 'not found' }));

if (require.main === module) {
  const port = Number(process.env.PORT || 3000);
  const host = process.env.HOST || '0.0.0.0';
  const server = app.listen(port, host, () => {
    console.log(`Sky Event Ledger listening on ${host}:${port}`);
  });

  const shutdown = () => {
    server.close((error) => {
      if (error) {
        console.error(error);
        process.exitCode = 1;
      }
    });
  };
  process.once('SIGTERM', shutdown);
  process.once('SIGINT', shutdown);
}

export default app;
