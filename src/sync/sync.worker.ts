
import type { Pool, PoolClient } from "pg";

import { localDB, cloudDB } from "../config/database.js";
import { InboxService } from "../services/inbox.service.js";
import { OutboxService } from "../services/outbox.service.js";

import type { OutboxRecord } from "../db/schemas/outbox.schema.js";
import type { ReceiveChangeInput } from "../db/schemas/inbox.schema.js";
import type { NotePayload } from "../db/schemas/notes.schema.js";

const ENTITY_TYPE = "note";

const SYNC_INTERVAL = 30_000;
const BATCH_SIZE = 100;

class PendingLocalChangeError extends Error {
  constructor() {
    super("Destination has pending local changes");
    this.name = "PendingLocalChangeError";
  }
}

export class SyncWorker {
  private readonly localOutbox: OutboxService;
  private readonly cloudOutbox: OutboxService;

  private readonly localInbox: InboxService;
  private readonly cloudInbox: InboxService;

  private interval: ReturnType<typeof setInterval> | null = null;
  private running = false;

  private readonly localDatabaseId: string;
  private readonly cloudDatabaseId: string;

  constructor() {
    this.localOutbox = new OutboxService(localDB);
    this.cloudOutbox = new OutboxService(cloudDB);

    this.localInbox = new InboxService(localDB);
    this.cloudInbox = new InboxService(cloudDB);

    this.localDatabaseId = process.env.LOCAL_DATABASE_ID!;
    this.cloudDatabaseId = process.env.CLOUD_DATABASE_ID!;

    if (!this.localDatabaseId || !this.cloudDatabaseId) {
      throw new Error(
        "LOCAL_DATABASE_ID and CLOUD_DATABASE_ID must be configured"
      );
    }

    if (this.localDatabaseId === this.cloudDatabaseId) {
      throw new Error("Local and cloud database IDs must be different");
    }
  }

  
  start(): void {
    if (this.interval) return;

    console.log("Sync worker started");

    void this.runOnce();

    this.interval = setInterval(() => {
      void this.runOnce();
    }, SYNC_INTERVAL);
  }


  stop(): void {
    if (this.interval) {
      clearInterval(this.interval);
      this.interval = null;
    }

    console.log("Sync worker stopped");
  }

 
  async runOnce(): Promise<void> {
    if (this.running) return;

    this.running = true;

    try {
      console.log("[SYNC] Starting synchronization");

      // Local -> Cloud
      await this.syncDirection(
        this.localOutbox,
        this.cloudInbox,
        cloudDB,
        this.localDatabaseId,
        "LOCAL -> CLOUD"
      );

      // Cloud -> Local
      await this.syncDirection(
        this.cloudOutbox,
        this.localInbox,
        localDB,
        this.cloudDatabaseId,
        "CLOUD -> LOCAL"
      );

      console.log("[SYNC] Synchronization completed");

    } catch (error) {
      console.error("[SYNC] Synchronization failed:", error);
    } finally {
      this.running = false;
    }
  }

  /**
   * Transfer pending changes from one database to another.
   */
  private async syncDirection(
    sourceOutbox: OutboxService,
    destinationInbox: InboxService,
    destinationPool: Pool,
    sourceDatabaseId: string,
    direction: string
  ): Promise<void> {

    const pendingChanges = await sourceOutbox.getPending(BATCH_SIZE);

    if (pendingChanges.length === 0) {
      return;
    }

    console.log(
      `[SYNC] ${direction}: ${pendingChanges.length} pending changes`
    );

    for (const record of pendingChanges) {
      try {
        if (record.entity_type !== ENTITY_TYPE) {
          console.warn(
            `[SYNC] Unsupported entity type: ${record.entity_type}`
          );
          continue;
        }

        const input: ReceiveChangeInput = {
          sourceDatabaseId,
          changeId: record.change_id,
          entityType: record.entity_type,
          entityId: record.entity_id,
          operation: record.operation,
        };

        const payload = record.payload as NotePayload;

        if (!payload || payload.id !== record.entity_id) {
          throw new Error("Invalid outbox payload");
        }

        // Apply through destination InboxService.
        const processed = await destinationInbox.processIncoming(
          input,
          async (client) => {
            await this.applyNoteChange(
              client,
              record,
              destinationPool
            );
          }
        );

        // Both a newly processed change and a duplicate
        // are safe to acknowledge.
        const acknowledged = await sourceOutbox.acknowledge(
          record.entity_type,
          record.entity_id,
          String(record.revision)
        );

        console.log(
          `[SYNC] ${direction}: ${record.entity_id}`,
          {
            processed,
            acknowledged,
          }
        );

      } catch (error) {
        // Leave the source outbox record untouched.
        // It will be retried during the next cycle.
        if (error instanceof PendingLocalChangeError) {
          console.warn(
            `[SYNC] ${direction}: conflict for ${record.entity_id}`
          );
          continue;
        }

        console.error(
          `[SYNC] ${direction}: failed for ${record.entity_id}`,
          error
        );
      }
    }
  }

 
  private async applyNoteChange(
    client: PoolClient,
    record: OutboxRecord,
    destinationPool: Pool
  ): Promise<void> {

    const payload = record.payload as NotePayload;

    // Serialize changes to this entity.
    await client.query(
      `SELECT pg_advisory_xact_lock(
         hashtextextended($1 || ':' || $2, 0)
       )`,
      [record.entity_type, record.entity_id]
    );

    // Do not overwrite pending changes on the destination.
    const pending = await client.query(
      `SELECT 1
       FROM public.sync_outbox
       WHERE entity_type = $1
         AND entity_id = $2`,
      [record.entity_type, record.entity_id]
    );

    if ((pending.rowCount ?? 0) > 0) {
      throw new PendingLocalChangeError();
    }

    if (record.operation === "CREATE" ||
        record.operation === "UPDATE") {

      await client.query(
        `INSERT INTO public.notes (
           id,
           title,
           content,
           version,
           created_at,
           updated_at,
           deleted_at
         )
         VALUES (
           $1, $2, $3,
           COALESCE($4, 1),
           COALESCE($5, NOW()),
           COALESCE($6, NOW()),
           $7
         )
         ON CONFLICT (id)
         DO UPDATE SET
           title = EXCLUDED.title,
           content = EXCLUDED.content,
           version = EXCLUDED.version,
           created_at = EXCLUDED.created_at,
           updated_at = EXCLUDED.updated_at,
           deleted_at = EXCLUDED.deleted_at`,
        [
          payload.id,
          payload.title,
          payload.content ?? null,
          payload.version ?? null,
          payload.created_at ?? null,
          payload.updated_at ?? null,
          payload.deleted_at ?? null,
        ]
      );

      return;
    }

    if (record.operation === "DELETE") {
      await client.query(
        `UPDATE public.notes
         SET deleted_at = COALESCE($2, NOW()),
             updated_at = COALESCE($3, NOW()),
             version = COALESCE($4, version + 1)
         WHERE id = $1
           AND deleted_at IS NULL`,
        [
          record.entity_id,
          payload.deleted_at ?? null,
          payload.updated_at ?? null,
          payload.version ?? null,
        ]
      );

      return;
    }

    throw new Error(`Unsupported operation: ${record.operation}`);
  }
}
