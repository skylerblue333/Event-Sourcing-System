import request from 'supertest';
import app, { eventStore } from '../src/index';

describe('Event-Sourcing-System API', () => {
  it('GET /health returns 200', async () => {
    const res = await request(app).get('/health');
    expect(res.status).toBe(200);
    expect(res.body.status).toBe('healthy');
    expect(res.body.domain).toBe('event-sourcing-system');
  });

  it('POST /api/v1/process appends an event', async () => {
    const res = await request(app)
      .post('/api/v1/process')
      .send({ payload: 'test_data' });

    expect(res.status).toBe(201);
    expect(res.body.success).toBe(true);
    expect(res.body.processed).toBe('test_data');
    expect(res.body.eventId).toEqual(expect.any(String));
    expect(res.body.hash).toEqual(expect.any(String));
  });

  it('rejects a missing process payload', async () => {
    const res = await request(app).post('/api/v1/process').send({});
    expect(res.status).toBe(400);
  });

  it('appends a typed event and exposes the stream', async () => {
    const res = await request(app).post('/api/v1/events').send({
      aggregateId: 'acc-test',
      type: 'ACCOUNT_CREATED',
      data: { initialBalance: 100 },
      expectedVersion: 0,
    });

    expect(res.status).toBe(201);
    expect(res.body.sequence).toBe(1);
    expect(res.body.previousHash).toBe('GENESIS');

    const stream = await request(app).get('/api/v1/events/acc-test');
    expect(stream.status).toBe(200);
    expect(stream.body.version).toBe(1);
    expect(stream.body.events[0].type).toBe('ACCOUNT_CREATED');

    const verify = await request(app).get('/api/v1/events/acc-test/verify');
    expect(verify.status).toBe(200);
    expect(verify.body.valid).toBe(true);
  });

  it('is idempotent when the same key is retried', async () => {
    const first = await request(app)
      .post('/api/v1/events')
      .set('Idempotency-Key', 'event-retry-001')
      .send({ aggregateId: 'idempotent-test', type: 'ORDER_CREATED', data: { amount: 10 }, expectedVersion: 0 });

    const retry = await request(app)
      .post('/api/v1/events')
      .set('Idempotency-Key', 'event-retry-001')
      .send({ aggregateId: 'idempotent-test', type: 'ORDER_CREATED', data: { amount: 10 }, expectedVersion: 0 });

    expect(first.status).toBe(201);
    expect(retry.status).toBe(201);
    expect(retry.body.id).toBe(first.body.id);
    expect(retry.body.sequence).toBe(1);
  });

  it('returns 409 for optimistic concurrency conflicts', async () => {
    const res = await request(app).post('/api/v1/events').send({
      aggregateId: 'acc-test',
      type: 'FUNDS_DEPOSITED',
      data: { amount: 25 },
      expectedVersion: 0,
    });

    expect(res.status).toBe(409);
  });

  it('replays an account aggregate from its event stream', async () => {
    eventStore.append('acc-replay', 'ACCOUNT_CREATED', { initialBalance: 100 }, 0);
    eventStore.append('acc-replay', 'FUNDS_DEPOSITED', { amount: 50 }, 1);

    const res = await request(app).get('/api/v1/accounts/acc-replay');
    expect(res.status).toBe(200);
    expect(res.body.balance).toBe(150);
    expect(res.body.version).toBe(2);
  });
});
