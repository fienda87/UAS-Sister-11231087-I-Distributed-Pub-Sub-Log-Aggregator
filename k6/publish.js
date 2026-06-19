import http from 'k6/http';
import { check, sleep } from 'k6';
import { SharedArray } from 'k6/data';

export const options = {
  scenarios: {
    load: {
      executor: 'constant-vus',
      vus: 50,
      duration: '20s',    // ini sudah cukup untuk > 20.000 event di setup kamu
    },
  },
};

const target = __ENV.TARGET_URL || 'http://localhost:8080/publish';

// POOL ID DIPERKECIL supaya sering keulang (banyak duplikat)
const ids = new SharedArray('ids', function () {
  const arr = [];
  // dulu 14000, sekarang misal 1000 saja
  for (let i = 0; i < 1000; i++) arr.push(`e-${i}`);
  return arr;
});

// Probabilitas pakai ID dari pool dinaikkan jadi 40%
// sehingga minimal ≈ 35–40% event akan jadi duplikat kalau total event >= 20k
function pickEventId() {
  const usePool = Math.random() < 0.4; // 40% event ambil dari pool -> sumber duplikat
  if (usePool) {
    return ids[Math.floor(Math.random() * ids.length)];
  }
  // sisanya tetap unik per VU + iterasi
  return `u-${__VU}-${__ITER}-${Math.random().toString(16).slice(2)}`;
}

export default function () {
  const batchSize = 50;  // 50 event per request
  const now = new Date().toISOString();

  const events = [];
  for (let i = 0; i < batchSize; i++) {
    events.push({
      topic: ['auth','payment','orders'][Math.floor(Math.random() * 3)],
      event_id: pickEventId(),
      timestamp: now,
      source: 'k6',
      payload: { n: __ITER, vu: __VU },
    });
  }

  const res = http.post(target, JSON.stringify(events), {
    headers: { 'Content-Type': 'application/json' },
    timeout: '30s',
  });

  check(res, { 'status is 200/202': (r) => r.status === 200 || r.status === 202 });
  sleep(0.1);
}
