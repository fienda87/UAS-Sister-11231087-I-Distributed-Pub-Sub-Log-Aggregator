import pytest
from datetime import datetime, timezone


def make_ev(topic: str, event_id: str):
    return {
        "topic": topic,
        "event_id": event_id,
        "timestamp": datetime.now(timezone.utc).isoformat(),
        "source": "test-stats",
        "payload": {"k": "v"},
    }


@pytest.mark.asyncio
async def test_health_ok(client):
    """
    /health harus tersedia dan mengembalikan 200 OK.
    """
    r = await client.get("/health")
    assert r.status_code == 200


@pytest.mark.asyncio
async def test_stats_fields_and_invariants(client):
    """
    /stats punya field wajib dan relasi dasar antar counter konsisten.
    """
    s = (await client.get("/stats")).json()

    # Field wajib sesuai spesifikasi tugas
    for key in ("received", "unique_processed", "duplicate_dropped", "topics", "uptime"):
        assert key in s

    assert s["received"] >= 0
    assert s["unique_processed"] >= 0
    assert s["duplicate_dropped"] >= 0

    # Invarian async: received = done + duplicate + pending/processing.
    queue = s["queue"]
    in_flight = queue.get("pending", 0) + queue.get("processing", 0)
    assert s["received"] == s["unique_processed"] + s["duplicate_dropped"] + in_flight


@pytest.mark.asyncio
async def test_stats_updates_after_publish_unique_events(client):
    """
    Setelah publish beberapa event unik, counter di /stats harus bertambah konsisten.
    """
    before = (await client.get("/stats")).json()

    payload = [make_ev("stats-topic", f"ST-{i}") for i in range(5)]
    r = await client.post("/publish", json=payload)
    assert r.status_code == 200
    # Semua event unik, seharusnya semuanya masuk
    assert r.json()["inserted"] == len(payload)

    after = (await client.get("/stats")).json()

    # received naik sebanyak event yang dikirim
    assert after["received"] == before["received"] + len(payload)
    # unique_processed naik sebanyak event unik yang baru
    assert after["unique_processed"] == before["unique_processed"] + len(payload)
    # Tidak ada duplikat dalam batch ini
    assert after["duplicate_dropped"] == before["duplicate_dropped"]
