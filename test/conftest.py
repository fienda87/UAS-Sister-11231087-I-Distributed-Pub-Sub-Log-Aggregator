import asyncio
import pytest
import httpx
from asgi_lifespan import LifespanManager
from testcontainers.postgres import PostgresContainer

from aggregator.app.main import create_app
from aggregator.app.settings import Settings

@pytest.fixture(scope="session")
def event_loop():
    """Memastikan satu event loop untuk seluruh sesi testing."""
    policy = asyncio.get_event_loop_policy()
    loop = policy.new_event_loop()
    yield loop
    loop.close()

@pytest.fixture(scope="session")
def pg():
    """Menjalankan Postgres di Docker sementara."""
    with PostgresContainer("postgres:16-alpine") as postgres:
        # PENTING: asyncpg hanya mau 'postgresql://', bukan 'postgresql+psycopg2://'
        url = postgres.get_connection_url().replace("+psycopg2", "")
        postgres.db_url = url
        yield postgres

@pytest.fixture
async def client(pg):
    """Fixture untuk testing API standar."""
    settings = Settings(
        database_url=pg.db_url,
        workers=0,
        batch_size=50
    )
    app = create_app(settings)
    async with LifespanManager(app, startup_timeout=30, shutdown_timeout=30):
        # Definisikan transport untuk menjembatani HTTPX ke FastAPI
        transport = httpx.ASGITransport(app=app)
        async with httpx.AsyncClient(transport=transport, base_url="http://test") as c:
            yield c

@pytest.fixture
async def client_with_workers(pg):
    """Fixture untuk testing dengan worker aktif."""
    settings = Settings(
        database_url=pg.db_url,
        workers=4,
        batch_size=200
    )
    app = create_app(settings)
    async with LifespanManager(app, startup_timeout=30, shutdown_timeout=30):
        # FIX: Gunakan ASGITransport untuk versi HTTPX terbaru
        transport = httpx.ASGITransport(app=app)
        async with httpx.AsyncClient(transport=transport, base_url="http://test") as c:
            yield c
