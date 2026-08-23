import cors from 'cors';
import express from 'express';
import { AccountAggregate } from './account';
import { ConcurrencyError, EventStore } from './eventStore';

export const app = express();
export const eventStore = new EventStore();

app.use(cors());
app.use(express.json({ limit: '256kb' }));

app.get('/health', (_req, res) => {
  res.json({
    status: 'healthy',
    domain: 'event-sourcing-system',
    uptime: process.uptime(),
  });
});

app.post('/api/v1/process', (req, res) => {
  const { payload } = req.body ?? {};
  if (typeof payload !== 'string' || payload.length === 0) {
    return res.status(400).json({ error: 'Missing payload' });
  }

  const idempotencyKey = req.header('idempotency-key') ?? undefined;
  const event = eventStore.append(
    'process',
    'PROCESS_REQUESTED',
    { payload },
    undefined,
    idempotencyKey,
  );
  return res.status(201).json({
    success: true,
    processed: payload,
    eventId: event.id,
    timestamp: event.timestamp,
    hash: event.hash,
  });
});

app.post('/api/v1/events', (req, res) => {
  const { aggregateId, type, data, expectedVersion, idempotencyKey } = req.body ?? {};
  if (
    typeof aggregateId !== 'string' ||
    aggregateId.length === 0 ||
    typeof type !== 'string' ||
    type.length === 0
  ) {
    return res.status(400).json({ error: 'aggregateId and type are required' });
  }

  const requestIdempotencyKey =
    typeof idempotencyKey === 'string' && idempotencyKey.length > 0
      ? idempotencyKey
      : req.header('idempotency-key') ?? undefined;

  try {
    const event = eventStore.append(
      aggregateId,
      type,
      data ?? {},
      expectedVersion,
      requestIdempotencyKey,
    );
    return res.status(201).json(event);
  } catch (error) {
    if (error instanceof ConcurrencyError) {
      return res.status(409).json({ error: error.message });
    }
    return res.status(500).json({ error: 'event append failed' });
  }
});

app.get('/api/v1/events/:aggregateId', (req, res) => {
  const stream = eventStore.getStream(req.params.aggregateId);
  return res.json({ aggregateId: req.params.aggregateId, version: stream.length, events: stream });
});

app.get('/api/v1/events/:aggregateId/verify', (req, res) => {
  const valid = eventStore.verifyStream(req.params.aggregateId);
  return res.status(valid ? 200 : 409).json({
    aggregateId: req.params.aggregateId,
    valid,
    version: eventStore.version(req.params.aggregateId),
  });
});

app.get('/api/v1/accounts/:id', (req, res) => {
  const aggregate = new AccountAggregate(req.params.id);
  for (const event of eventStore.getStream(req.params.id)) {
    try {
      aggregate.apply(event);
    } catch {
      return res.status(422).json({ error: 'invalid account event stream' });
    }
  }
  return res.json(aggregate.snapshot());
});

if (require.main === module) {
  app.listen(3000, () => console.log('Event-Sourcing-System API running on port 3000'));
}

export default app;
