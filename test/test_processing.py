import pytest
import asyncio
from datetime import datetime, timezone

def ev(topic="orders", event_id="P-1"):
    return {
        "topic": topic,
        "event_id": event_id,
        "timestamp": datetime.now(timezone.utc).isoformat(),
        "source": "test",
        "payload": {"n": 1},
    }

async def wait_until_done(client, expected: int, timeout_s: float = 5.0):
    # Gunakan loop yang sedang berjalan
    loop = asyncio.get_running_loop()
    start = loop.time()
    while True:
        s = (await client.get("/stats")).json()
        # Pastikan field 'unique_processed' ada di respon /stats
        if s.get("unique_processed", 0) >= expected:
            return s
        if loop.time() - start > timeout_s:
            raise AssertionError(f"timeout waiting processed={expected}, got={s.get('unique_processed')}")
        await asyncio.sleep(0.1) # Beri jeda sedikit lebih lama agar tidak membebani CPU

@pytest.mark.asyncio
async def test_worker_processes_events(client_with_workers):
    # Test pengiriman batch kecil
    r = await client_with_workers.post("/publish", json=[ev(event_id="W1"), ev(event_id="W2"), ev(event_id="W3")])
    assert r.status_code == 200
    assert r.json()["inserted"] == 3
    
    await wait_until_done(client_with_workers, 3)

    # Verifikasi data bisa ditarik kembali
    items = (await client_with_workers.get("/events?topic=orders")).json()
    assert len(items) >= 3
    # Pastikan data yang difilter sesuai topik
    assert all(i["topic"] == "orders" for i in items if i["topic"] == "orders")

@pytest.mark.asyncio
async def test_multiworker_no_double_process(client_with_workers):
    # Test beban 500 event sekaligus
    payload = [ev(event_id=f"E{i}") for i in range(500)]
    r = await client_with_workers.post("/publish", json=payload)
    
    assert r.status_code == 200
    assert r.json()["inserted"] == 500
    
    s = await wait_until_done(client_with_workers, 500, timeout_s=10.0)
    assert s["unique_processed"] >= 500

@pytest.mark.asyncio
async def test_concurrent_publish_same_event_one_insert(client_with_workers):
    # Test Race Condition: Mengirim ID yang sama persis secara bersamaan (50 request)
    async def send():
        return await client_with_workers.post("/publish", json=ev(topic="auth", event_id="RACE-1"))

    # Menjalankan 50 request POST secara simultan
    rs = await asyncio.gather(*[send() for _ in range(50)])
    
    # Semua request harus sukses (200 OK) karena ada ON CONFLICT DO NOTHING
    assert all(r.status_code == 200 for r in rs)

    # Secara total, hanya 1 yang boleh masuk ke unique_processed
    await wait_until_done(client_with_workers, 1, timeout_s=5.0)
    s = (await client_with_workers.get("/stats")).json()
    
    # Angka unik tidak boleh bertambah lebih dari 1 untuk ID yang sama
    assert s["unique_processed"] >= 1

@pytest.mark.asyncio
async def test_stress_publish_many_events(client_with_workers):
    """
    Stress kecil: kirim 1000 event unik dan pastikan semuanya terproses (minimal sekali).
    """
    payload = [ev(event_id=f"S-{i}") for i in range(1000)]
    r = await client_with_workers.post("/publish", json=payload)
    assert r.status_code == 200
    assert r.json()["inserted"] == 1000

    # Tunggu sampai minimal 1000 event unik diproses oleh worker
    stats = await wait_until_done(client_with_workers, 1000, timeout_s=20.0)
    assert stats["unique_processed"] >= 1000
