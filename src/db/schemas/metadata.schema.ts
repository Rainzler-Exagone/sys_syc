
export interface SyncMetadata {
  id: boolean;
  database_id: string;
  schema_version: number;
  created_at: Date;
  updated_at: Date;
}

export const METADATA_TABLE = "public.sync_metadata";
