import request from 'supertest';
import app from '../src/index';

describe('Sky Event Ledger product API', () => {
  it('exposes liveness readiness and metrics', async () => {
    expect((await request(app).get('/healthz')).status).toBe(200);
    expect((await request(app).get('/readyz')).status).toBe(200);
    expect((await request(app).get('/metrics')).status).toBe(200);
  });

  it('deduplicates identical writes by idempotency key', async () => {
    const body = {
      aggregateId: 'product-idem-1',
      type: 'ORDER_CREATED',
      data: { customerId: 'c-1' },
      expectedVersion: 0,
    };
    const first = await request(app)
      .post('/api/v1/events')
      .set('Idempotency-Key', 'product-idem-key-1')
      .send(body);
    const replay = await request(app)
      .post('/api/v1/events')
      .set('Idempotency-Key', 'product-idem-key-1')
      .send(body);

    expect(first.status).toBe(201);
    expect(replay.status).toBe(201);
    expect(replay.body.id).toBe(first.body.id);

    const stream = await request(app).get('/api/v1/events/product-idem-1');
    expect(stream.body.version).toBe(1);
  });

  it('rejects conflicting reuse of an idempotency key', async () => {
    const key = 'product-idem-key-2';
    await request(app)
      .post('/api/v1/events')
      .set('Idempotency-Key', key)
      .send({ aggregateId: 'product-idem-2', type: 'CREATED', data: { value: 1 }, expectedVersion: 0 });

    const conflict = await request(app)
      .post('/api/v1/events')
      .set('Idempotency-Key', key)
      .send({ aggregateId: 'product-idem-2', type: 'CREATED', data: { value: 2 }, expectedVersion: 1 });

    expect(conflict.status).toBe(409);
  });

  it('verifies an intact event stream', async () => {
    await request(app)
      .post('/api/v1/events')
      .send({ aggregateId: 'verify-api-1', type: 'CREATED', data: {}, expectedVersion: 0 });

    const result = await request(app).get('/api/v1/events/verify-api-1/verify');
    expect(result.status).toBe(200);
    expect(result.body.valid).toBe(true);
  });
});
