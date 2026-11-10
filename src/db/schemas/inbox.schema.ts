
import type { SyncOperation } from "./outbox.schema.js";

export interface InboxRecord {
  id: string;
  source_database_id: string;
  change_id: string;
  entity_type: string;
  entity_id: string;
  operation: SyncOperation;
  received_at: Date;
}

export interface ReceiveChangeInput {
  sourceDatabaseId: string;
  changeId: string;
  entityType: string;
  entityId: string;
  operation: SyncOperation;
}

export const INBOX_TABLE = "public.sync_inbox";
