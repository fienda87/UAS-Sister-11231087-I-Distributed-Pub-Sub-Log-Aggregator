import pytest
import httpx
from datetime import datetime, timezone
from asgi_lifespan import LifespanManager
from aggregator.app.main import create_app 
from aggregator.app.settings import Settings


def ev(topic="auth", event_id="RST-1"):
    return {
        "topic": topic,
        "event_id": event_id,
        "timestamp": datetime.now(timezone.utc).isoformat(),
        "source": "test",
        "payload": {"a": 1},
    }


@pytest.mark.asyncio
async def test_persistence_like_restart_same_db(pg):
    db_url = pg.get_connection_url().replace("+psycopg2", "")
    
    # Ambil stats awal untuk referensi (mencegah error jika DB sudah ada isinya)
    s_initial = Settings(database_url=db_url)
    app_init = create_app(s_initial)
    async with LifespanManager(app_init):
        async with httpx.AsyncClient(transport=httpx.ASGITransport(app=app_init), base_url="http://test") as c_init:
            r_stats = await c_init.get("/stats")
            base_unique = r_stats.json().get("unique_processed", 0)

    # --- SIMULASI SESSION 1 ---
    s1 = Settings(database_url=db_url, workers=0, batch_size=50, poll_interval_ms=10, stuck_processing_sec=60)
    app1 = create_app(s1)
    async with LifespanManager(app1):
        transport1 = httpx.ASGITransport(app=app1)
        async with httpx.AsyncClient(transport=transport1, base_url="http://test") as c1:
            # Gunakan ID yang unik agar tidak bentrok dengan tes lain, misal: 'RST-UNIQUE-99'
            r1 = await c1.post("/publish", json=ev(event_id="RST-UNIQUE-99"))
            assert r1.json()["inserted"] == 1

    # --- SIMULASI SESSION 2 (Restart app, DB tetap sama) ---
    s2 = Settings(database_url=db_url, workers=0, batch_size=50, poll_interval_ms=10, stuck_processing_sec=60)
    app2 = create_app(s2)
    async with LifespanManager(app2):
        transport2 = httpx.ASGITransport(app=app2)
        async with httpx.AsyncClient(transport=transport2, base_url="http://test") as c2:
            # Kirim data yang sama persis dengan session 1
            r2 = await c2.post("/publish", json=ev(event_id="RST-UNIQUE-99"))
            
            # Harusnya tidak masuk (inserted=0)
            assert r2.json()["inserted"] == 0
            
            stats = (await c2.get("/stats")).json()
            # Cek apakah jumlahnya sekarang adalah base + 1
            assert stats["unique_processed"] == base_unique + 1