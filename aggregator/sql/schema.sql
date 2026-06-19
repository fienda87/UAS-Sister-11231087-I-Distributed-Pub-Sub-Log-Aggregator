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

CREATE INDEX IF NOT EXISTS idx_processed_status_id ON processed_events (status, id);
CREATE INDEX IF NOT EXISTS idx_processed_ts ON processed_events (ts_ingest DESC);
CREATE INDEX IF NOT EXISTS idx_processed_topic_ts ON processed_events (topic, ts_ingest DESC);

CREATE TABLE IF NOT EXISTS stats (
  key TEXT PRIMARY KEY,
  val BIGINT NOT NULL DEFAULT 0
);

INSERT INTO stats(key, val) VALUES
  ('received', 0),
  ('unique_processed', 0),
  ('duplicate_dropped', 0)
ON CONFLICT (key) DO NOTHING;
