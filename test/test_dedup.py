import pytest
from datetime import datetime, timezone

def ev(topic="auth", event_id="DUP-1"):
    return {
        "topic": topic,
        "event_id": event_id,
        "timestamp": datetime.now(timezone.utc).isoformat(),
        "source": "test",
        "payload": {"k": "v"},
    }

@pytest.mark.asyncio
async def test_dedup_duplicate_dropped(client):
    # Menguji apakah event yang sama dikirim berurutan akan di-drop
    r1 = await client.post("/publish", json=ev())
    r2 = await client.post("/publish", json=ev())
    
    assert r1.status_code == 200
    assert r1.json()["inserted"] == 1
    
    assert r2.status_code == 200
    assert r2.json()["inserted"] == 0
    assert r2.json()["duplicates"] == 1

    # Verifikasi ke endpoint stats
    s = (await client.get("/stats")).json()
    assert s["unique_processed"] >= 1
    assert s["duplicate_dropped"] >= 1

@pytest.mark.asyncio
async def test_dedup_unique_constraint_across_topics(client):
    # Menguji bahwa event_id yang sama tapi beda topic dianggap data UNIK
    # (Karena Primary Key adalah gabungan topic + event_id)
    r = await client.post("/publish", json=[
        ev(topic="t1", event_id="X"), 
        ev(topic="t2", event_id="X")
    ])
    assert r.status_code == 200
    assert r.json()["inserted"] == 2

@pytest.mark.asyncio
async def test_batch_mixed_duplicates(client):
    # Menguji pengiriman batch dengan campuran data unik dan duplikat
    payload = [
        ev(event_id="M1"), 
        ev(event_id="M1"), # Duplikat
        ev(event_id="M2"), 
        ev(event_id="M2"), # Duplikat
        ev(event_id="M3")
    ]
    r = await client.post("/publish", json=payload)
    j = r.json()
    
    # Perbaikan: 'received' diubah menjadi 'accepted' sesuai return di main.py
    assert j["accepted"] == 5 
    assert j["inserted"] == 3
    assert j["duplicates"] == 2