
export type ConflictStatus = "PENDING" | "RESOLVED" | "DISMISSED";

export interface SyncConflict {
  id: string;
  entity_type: string;
  entity_id: string;
  source_database_id: string;

  local_payload: Record<string, unknown> | null;
  remote_payload: Record<string, unknown> | null;

  local_revision: number | null;
  remote_revision: number | null;

  status: ConflictStatus;
  resolution: Record<string, unknown> | null;

  created_at: Date;
  resolved_at: Date | null;
}

export interface CreateConflictInput {
  entityType: string;
  entityId: string;
  sourceDatabaseId: string;
  localPayload: Record<string, unknown> | null;
  remotePayload: Record<string, unknown> | null;
  localRevision: number | null;
  remoteRevision: number | null;
}

export const CONFLICTS_TABLE = "public.sync_conflicts";
