// k6 load test: create payments, replay some (idempotency), read them back.
//   docker compose up -d && k6 run loadtest/payments.js
import http from 'k6/http';
import { check } from 'k6';

export const options = {
  scenarios: {
    steady: { executor: 'constant-arrival-rate', rate: 200, timeUnit: '1s', duration: '30s', preAllocatedVUs: 50 },
  },
  thresholds: {
    http_req_failed: ['rate<0.01'],
    'http_req_duration{name:create}': ['p(95)<100'],
  },
};

const BASE = __ENV.BASE_URL || 'http://localhost:8080';

export default function () {
  const key = `k6-${__VU}-${__ITER % 50}`; // ~2% of requests are replays of an earlier key
  const body = JSON.stringify({ merchant_id: `shop-${__VU}`, amount: 1000 + (__ITER % 50), currency: 'KZT' });
  const res = http.post(`${BASE}/v1/payments`, body, {
    headers: { 'Content-Type': 'application/json', 'Idempotency-Key': key },
    tags: { name: 'create' },
  });
  check(res, { 'created or replayed': (r) => r.status === 201 || r.status === 200 });
  if (res.status === 201 || res.status === 200) {
    const get = http.get(`${BASE}/v1/payments/${res.json('id')}`, { tags: { name: 'get' } });
    check(get, { 'readable': (r) => r.status === 200 });
  }
}
