from __future__ import annotations
import asyncio
from contextlib import asynccontextmanager
from datetime import datetime, timezone
from typing import Any, List, Optional
import json
import logging

from fastapi import FastAPI, HTTPException, Query, Request
from pydantic import BaseModel, Field, ValidationError

from .db import init_db
from .settings import Settings 
from .worker import worker_loop

log = logging.getLogger("aggregator")

# Pydantic models
class EventIn(BaseModel):
    topic: str = Field(min_length=1)
    event_id: str = Field(min_length=1)
    timestamp: datetime
    source: str = Field(min_length=1)
    payload: dict[str, Any]


# Helper Functions

def _normalize_payload(body: Any) -> List[dict]:
    if isinstance(body, dict) and "events" in body:
        events = body["events"]
        if not isinstance(events, list):
            raise HTTPException(status_code=400, detail="'events' harus array")
        return events
    if isinstance(body, list): return body
    if isinstance(body, dict): return [body]
    raise HTTPException(status_code=400, detail="Body format salah")

def _parse_events(raw_events: List[dict]) -> List[EventIn]:
    try:
        return [EventIn.model_validate(e) for e in raw_events]
    except ValidationError as ve:
        raise HTTPException(status_code=400, detail=ve.errors())


# Factory Function (Untuk Pytest & Docker)

def create_app(settings: Optional[Settings] = None) -> FastAPI:
    # Jika settings tidak dipassing (saat running di Docker), ambil dari env
    if settings is None:
        settings = Settings()

    @asynccontextmanager
    async def lifespan(app: FastAPI):
        app.state.start_time = datetime.now(timezone.utc)
        pool = await init_db(settings.database_url)
        app.state.db_pool = pool
        app.state.settings = settings
        app.state.stop_event = asyncio.Event()
        app.state.worker_tasks = [
            asyncio.create_task(
                worker_loop(
                    pool,
                    settings.batch_size,
                    settings.poll_interval_ms,
                    stop_event=app.state.stop_event,
                )
            )
            for _ in range(max(0, settings.workers))
        ]
        if app.state.worker_tasks:
            log.info("started workers=%s", len(app.state.worker_tasks))
        try:
            yield
        finally:
            app.state.stop_event.set()
            for task in app.state.worker_tasks:
                task.cancel()
            if app.state.worker_tasks:
                await asyncio.gather(*app.state.worker_tasks, return_exceptions=True)
            await pool.close()

    app = FastAPI(title="PubSub Log Aggregator", lifespan=lifespan)

    @app.post("/publish")
    async def publish(request: Request):
        try:
            body = await request.json()
        except:
            raise HTTPException(status_code=400, detail="Invalid JSON body")

        events = _parse_events(_normalize_payload(body))
        pool = request.app.state.db_pool
        
        received, inserted, duplicates = len(events), 0, 0
        worker_mode = settings.workers > 0
        status = "pending" if worker_mode else "done"

        async with pool.acquire() as conn:
            async with conn.transaction():
                for e in events:
                    row = await conn.fetchrow(
                        """
                        INSERT INTO processed_events(
                            topic, event_id, ts_ingest, source, payload, status, processed_at
                        )
                        VALUES ($1, $2, $3, $4, $5::jsonb, $6, CASE WHEN $6 = 'done' THEN now() ELSE NULL END)
                        ON CONFLICT (topic, event_id) DO NOTHING
                        RETURNING 1
                        """,
                        e.topic,
                        e.event_id,
                        e.timestamp,
                        e.source,
                        json.dumps(e.payload),
                        status,
                    )
                    if row: inserted += 1
                    else: duplicates += 1

                await conn.execute("UPDATE stats SET val = val + $1 WHERE key = 'received'", received)
                if inserted and not worker_mode:
                    await conn.execute("UPDATE stats SET val = val + $1 WHERE key = 'unique_processed'", inserted)
                if duplicates: await conn.execute("UPDATE stats SET val = val + $1 WHERE key = 'duplicate_dropped'", duplicates)

        return {"accepted": received, "inserted": inserted, "duplicates": duplicates}

    @app.get("/events")
    async def list_events(request: Request, topic: Optional[str] = None, limit: int = Query(100, ge=1, le=5000)):
        q = "SELECT topic,event_id,ts_ingest,source,payload FROM processed_events"
        args = [limit]
        if topic:
            q = "SELECT topic,event_id,ts_ingest,source,payload FROM processed_events WHERE topic=$1 ORDER BY ts_ingest DESC LIMIT $2"
            args = [topic, limit]
        else:
            q += " ORDER BY ts_ingest DESC LIMIT $1"
        
        async with request.app.state.db_pool.acquire() as conn:
            rows = await conn.fetch(q, *args)
        return [dict(r) for r in rows]

    @app.get("/stats")
    async def get_stats(request: Request):
        pool = request.app.state.db_pool

        async with pool.acquire() as conn:
            # counters utama
            rows = await conn.fetch("SELECT key,val FROM stats")

            # daftar topic unik yang pernah diproses
            topic_rows = await conn.fetch(
                "SELECT DISTINCT topic FROM processed_events ORDER BY topic"
            )
            queue_rows = await conn.fetch(
                """
                SELECT status, count(*) AS total
                FROM processed_events
                GROUP BY status
                """
            )

        counters = {r["key"]: r["val"] for r in rows}
        topics = [r["topic"] for r in topic_rows]
        queue = {"pending": 0, "processing": 0, "done": 0}
        queue.update({r["status"]: r["total"] for r in queue_rows})

        # uptime dalam detik sejak app start
        start = getattr(request.app.state, "start_time", None)
        if start is not None:
            uptime = (datetime.now(timezone.utc) - start).total_seconds()
        else:
            uptime = 0.0

        # gabungkan semuanya
        return {
            **counters,
            "topics": topics,
            "queue": queue,
            "uptime": uptime,
        }

    @app.get("/health")
    async def health():
        return {"ok": True}

    return app

# Variabel global agar 'uvicorn app.main:app' 
app = create_app()
