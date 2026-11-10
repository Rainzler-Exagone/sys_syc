CREATE TABLE IF NOT EXISTS public.notes (
  id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
  title VARCHAR(255) NOT NULL,
  content TEXT,
  version INTEGER NOT NULL DEFAULT 1,
  created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
  updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
  deleted_at TIMESTAMPTZ
);

CREATE INDEX IF NOT EXISTS idx_notes_updated_at
  ON public.notes (updated_at);

CREATE TABLE IF NOT EXISTS public.sync_metadata (
  id BOOLEAN PRIMARY KEY DEFAULT TRUE CHECK (id = TRUE),
  database_id UUID NOT NULL DEFAULT gen_random_uuid(),
  schema_version INTEGER NOT NULL,
  created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
  updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE TABLE IF NOT EXISTS public.sync_outbox (
  id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
  change_id UUID NOT NULL UNIQUE DEFAULT gen_random_uuid(),
  entity_type TEXT NOT NULL,
  entity_id TEXT NOT NULL,
  operation TEXT NOT NULL CHECK (operation IN ('CREATE', 'UPDATE', 'DELETE')),
  payload JSONB,
  revision INTEGER NOT NULL DEFAULT 1 CHECK (revision > 0),
  created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
  updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
  CONSTRAINT sync_outbox_entity_unique UNIQUE (entity_type, entity_id)
);

CREATE INDEX IF NOT EXISTS idx_sync_outbox_updated_at
  ON public.sync_outbox (updated_at);

CREATE TABLE IF NOT EXISTS public.sync_inbox (
  id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
  source_database_id UUID NOT NULL,
  change_id UUID NOT NULL,
  entity_type TEXT NOT NULL,
  entity_id TEXT NOT NULL,
  operation TEXT NOT NULL CHECK (operation IN ('CREATE', 'UPDATE', 'DELETE')),
  received_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
  CONSTRAINT sync_inbox_source_change_unique
    UNIQUE (source_database_id, change_id)
);

CREATE TABLE IF NOT EXISTS public.sync_conflicts (
  id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
  entity_type TEXT NOT NULL,
  entity_id TEXT NOT NULL,
  source_database_id UUID NOT NULL,
  local_payload JSONB,
  remote_payload JSONB,
  local_revision INTEGER,
  remote_revision INTEGER,
  status TEXT NOT NULL DEFAULT 'PENDING'
    CHECK (status IN ('PENDING', 'RESOLVED', 'DISMISSED')),
  resolution JSONB,
  created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
  resolved_at TIMESTAMPTZ
);

CREATE INDEX IF NOT EXISTS idx_sync_conflicts_status
  ON public.sync_conflicts (status);

CREATE INDEX IF NOT EXISTS idx_sync_conflicts_entity
  ON public.sync_conflicts (entity_type, entity_id);
