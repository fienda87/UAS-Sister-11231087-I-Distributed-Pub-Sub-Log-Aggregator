import pytest

@pytest.mark.asyncio
async def test_publish_single_event(client):
    """Test kirim 1 event valid."""
    payload = {
        "topic": "test-topic",
        "event_id": "ev-001",
        "timestamp": "2023-10-27T10:00:00Z",
        "source": "pytest",
        "payload": {"data": "hello"}
    }
    response = await client.post("/publish", json=payload)
    assert response.status_code == 200
    assert response.json()["inserted"] == 1

@pytest.mark.asyncio
async def test_publish_duplicate_event(client):
    """Test kirim event yang sama 2x (Idempotensi)."""
    payload = {
        "topic": "orders",
        "event_id": "order-101",
        "timestamp": "2023-10-27T10:00:00Z",
        "source": "manual",
        "payload": {"item": "kopi"}
    }
    # Kirim pertama
    await client.post("/publish", json=payload)
    
    # Kirim kedua (duplikat)
    response = await client.post("/publish", json=payload)
    assert response.status_code == 200
    assert response.json()["inserted"] == 0
    assert response.json()["duplicates"] == 1

@pytest.mark.asyncio
async def test_publish_invalid_schema(client):
    """Test kirim data tanpa field 'topic' (Harus Error 400)."""
    payload = {
        "event_id": "ev-999",
        "timestamp": "2023-10-27T10:00:00Z",
        "payload": {}
    }
    response = await client.post("/publish", json=payload)
    assert response.status_code == 400