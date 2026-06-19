import os

import asyncpg


SCHEMA_SQL = """
CREATE TABLE IF NOT EXISTS processed_events (
    id BIGSERIAL PRIMARY KEY,
    topic TEXT NOT NULL,
    event_id TEXT NOT NULL,
    ts_ingest TIMESTAMPTZ NOT NULL,
    source TEXT NOT NULL,
    payload JSONB NOT NULL,
    status TEXT NOT NULL DEFAULT 'pending',
    attempts INTEGER NOT NULL DEFAULT 0,
    locked_at TIMESTAMPTZ,
    processed_at TIMESTAMPTZ,
    last_error TEXT,
    CONSTRAINT uq_topic_event UNIQUE (topic, event_id)
);

CREATE INDEX IF NOT EXISTS idx_processed_status_id
    ON processed_events (status, id);
CREATE INDEX IF NOT EXISTS idx_processed_ts
    ON processed_events (ts_ingest DESC);
CREATE INDEX IF NOT EXISTS idx_processed_topic_ts
    ON processed_events (topic, ts_ingest DESC);

CREATE TABLE IF NOT EXISTS stats (
    key TEXT PRIMARY KEY,
    val BIGINT NOT NULL DEFAULT 0
);

INSERT INTO stats (key, val) VALUES
    ('received', 0),
    ('unique_processed', 0),
    ('duplicate_dropped', 0)
ON CONFLICT (key) DO NOTHING;
"""


MIGRATION_SQL = """
ALTER TABLE processed_events ADD COLUMN IF NOT EXISTS id BIGSERIAL;
ALTER TABLE processed_events ADD COLUMN IF NOT EXISTS status TEXT NOT NULL DEFAULT 'done';
ALTER TABLE processed_events ADD COLUMN IF NOT EXISTS attempts INTEGER NOT NULL DEFAULT 0;
ALTER TABLE processed_events ADD COLUMN IF NOT EXISTS locked_at TIMESTAMPTZ;
ALTER TABLE processed_events ADD COLUMN IF NOT EXISTS processed_at TIMESTAMPTZ;
ALTER TABLE processed_events ADD COLUMN IF NOT EXISTS last_error TEXT;

UPDATE processed_events
SET status = 'done',
    processed_at = COALESCE(processed_at, now())
WHERE status IS NULL;
"""


async def init_db(dsn: str = None):
    if dsn is None:
        dsn = os.getenv("DATABASE_URL", "postgresql://user:pass@localhost:5432/db")

    pool = await asyncpg.create_pool(dsn=dsn)

    async with pool.acquire() as conn:
        await conn.execute(SCHEMA_SQL)
        await conn.execute(MIGRATION_SQL)

    return pool


async def claim_events(pool, batch_size: int):
    async with pool.acquire() as conn:
        async with conn.transaction():
            return await conn.fetch(
                """
                WITH picked AS (
                    SELECT id
                    FROM processed_events
                    WHERE status = 'pending'
                    ORDER BY id
                    LIMIT $1
                    FOR UPDATE SKIP LOCKED
                )
                UPDATE processed_events e
                SET status = 'processing',
                    locked_at = now(),
                    attempts = attempts + 1,
                    last_error = NULL
                FROM picked
                WHERE e.id = picked.id
                RETURNING e.id, e.topic, e.event_id, e.payload
                """,
                batch_size,
            )


async def mark_done(pool, ids: list[int]) -> int:
    if not ids:
        return 0

    async with pool.acquire() as conn:
        async with conn.transaction():
            result = await conn.execute(
                """
                UPDATE processed_events
                SET status = 'done',
                    processed_at = now()
                WHERE id = ANY($1::bigint[])
                  AND status = 'processing'
                """,
                ids,
            )
            done = int(result.split()[-1])
            if done:
                await conn.execute(
                    "UPDATE stats SET val = val + $1 WHERE key = 'unique_processed'",
                    done,
                )
            return done


async def mark_failed(pool, event_id: int, error: str) -> None:
    async with pool.acquire() as conn:
        await conn.execute(
            """
            UPDATE processed_events
            SET status = 'pending',
                locked_at = NULL,
                last_error = $2
            WHERE id = $1
            """,
            event_id,
            error[:1000],
        )
