
export type SyncOperation = "CREATE" | "UPDATE" | "DELETE";

export interface OutboxRecord {
  id: string;
  change_id: string;
  entity_type: string;
  entity_id: string;
  operation: SyncOperation;
  payload: Record<string, unknown> | null;
  revision: number;
  created_at: Date;
  updated_at: Date;
}

export interface EnqueueChangeInput {
  entityType: string;
  entityId: string;
  operation: SyncOperation;
  payload: Record<string, unknown> | null;
}

export const OUTBOX_TABLE = "public.sync_outbox";
