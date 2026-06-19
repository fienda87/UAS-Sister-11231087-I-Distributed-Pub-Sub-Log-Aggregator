import asyncio
import logging
from . import db

log = logging.getLogger("worker")


async def worker_loop(pool, batch_size: int, poll_interval_ms: int, *, stop_event: asyncio.Event) -> None:
    sleep_s = max(0.001, poll_interval_ms / 1000.0)
    while not stop_event.is_set():
        rows = await db.claim_events(pool, batch_size)
        if not rows:
            await asyncio.sleep(sleep_s)
            continue

        ids = [int(r["id"]) for r in rows]

        # "processing" side-effect: untuk tugas ini cukup mark done.
        # Kalau mau side-effect lain (mis. agregasi), letakkan di sini
        # dan pastikan commit idempotent berbasis event row yang unik.
        try:
            done = await db.mark_done(pool, ids)
            log.info("processed=%s", done)
        except Exception as e:
            log.exception("mark_done failed: %s", e)
            for rid in ids:
                try:
                    await db.mark_failed(pool, rid, repr(e))
                except Exception:
                    log.exception("mark_failed failed for id=%s", rid)
