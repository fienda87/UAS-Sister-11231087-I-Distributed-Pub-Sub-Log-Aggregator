import pytest
from datetime import datetime, timezone


def make_ev(topic: str, event_id: str):
    return {
        "topic": topic,
        "event_id": event_id,
        "timestamp": datetime.now(timezone.utc).isoformat(),
        "source": "test-events",
        "payload": {"n": 1},
    }


@pytest.mark.asyncio
async def test_get_events_filters_by_topic(client):
    """
    /events?topic= hanya mengembalikan event untuk topic tersebut,
    dan event yang baru dikirim benar-benar muncul.
    """
    topic_a = "events-a"
    topic_b = "events-b"

    payload = [
        make_ev(topic_a, "EV-A-1"),
        make_ev(topic_a, "EV-A-2"),
        make_ev(topic_b, "EV-B-1"),
    ]
    r = await client.post("/publish", json=payload)
    assert r.status_code == 200

    r_a = await client.get(f"/events?topic={topic_a}")
    assert r_a.status_code == 200
    items = r_a.json()

    # Semua item di response harus bertopic topic_a
    assert all(item["topic"] == topic_a for item in items)

    event_ids = {item["event_id"] for item in items}
    # Minimal 2 event yang kita kirim tadi ada
    assert {"EV-A-1", "EV-A-2"}.issubset(event_ids)


@pytest.mark.asyncio
async def test_get_events_returns_unique_events_per_topic_event_id(client):
    """
    View /events harus mengandung hanya event unik berdasarkan (topic, event_id),
    meskipun duplikat pernah dikirim ke /publish.
    """
    topic = "events-unique"
    payload = [
        make_ev(topic, "U-1"),
        make_ev(topic, "U-1"),  # duplikat
        make_ev(topic, "U-2"),
        make_ev(topic, "U-2"),  # duplikat
    ]
    r = await client.post("/publish", json=payload)
    assert r.status_code == 200

    r_ev = await client.get(f"/events?topic={topic}")
    assert r_ev.status_code == 200
    items = [i for i in r_ev.json() if i["topic"] == topic]

    event_ids = [i["event_id"] for i in items]
    assert "U-1" in event_ids
    assert "U-2" in event_ids
    # Masing-masing ID hanya muncul satu kali
    assert event_ids.count("U-1") == 1
    assert event_ids.count("U-2") == 1


@pytest.mark.asyncio
async def test_get_events_unknown_topic_returns_empty_list(client):
    """
    Jika topic tidak pernah dipakai, /events?topic= harus mengembalikan list kosong.
    """
    r = await client.get("/events?topic=__no_such_topic__")
    assert r.status_code == 200
    items = r.json()
    assert isinstance(items, list)
    assert items == []
