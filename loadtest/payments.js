// k6 load test: create payments, replay a few (idempotency), read them back.
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
    checks: ['rate>0.99'],
  },
};

const BASE = __ENV.BASE_URL || 'http://localhost:8080';
const RUN = __ENV.RUN_ID || `${Date.now()}`; // keeps keys unique across runs against the same database
const REPLAY_RATE = 0.02;

let last = null; // this VU's last created request (VU-local state in k6)

export default function () {
  // Every iteration creates a payment with a new key, except ~2% that resend
  // this VU's last created request (same key, same body) and must get the
  // original payment back with 200.
  const replay = last !== null && Math.random() < REPLAY_RATE;
  const req = replay ? last : {
    key: `k6-${RUN}-${__VU}-${__ITER}`,
    body: JSON.stringify({ merchant_id: `shop-${__VU}`, amount: 1000 + (__ITER % 50), currency: 'KZT' }),
  };
  const res = http.post(`${BASE}/v1/payments`, req.body, {
    headers: { 'Content-Type': 'application/json', 'Idempotency-Key': req.key },
    tags: { name: replay ? 'replay' : 'create' },
  });
  check(res, { 'created, or replayed': (r) => r.status === (replay ? 200 : 201) });
  if (!replay && res.status === 201) last = req;
  if (res.status === 201 || res.status === 200) {
    const get = http.get(`${BASE}/v1/payments/${res.json('id')}`, { tags: { name: 'get' } });
    check(get, { 'readable': (r) => r.status === 200 });
  }
}
