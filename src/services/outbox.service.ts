
import { Pool } from "pg";
import type { PoolClient } from "pg";
import type {
  EnqueueChangeInput,
  OutboxRecord,
  SyncOperation,
} from "../db/schemas/outbox.schema.js";

export class OutboxService {
  constructor(private readonly pool: Pool) {}

  /**
   * Enqueue a change using an existing transaction.
   * The caller is responsible for BEGIN / COMMIT / ROLLBACK.
   */
  async enqueueWithClient(
    client: PoolClient,
    input: EnqueueChangeInput
  ): Promise<OutboxRecord | null> {

    // Serialize changes to the same entity, including when no
    // outbox row exists yet.
    await client.query(
      `SELECT pg_advisory_xact_lock(
         hashtextextended($1 || ':' || $2, 0)
       )`,
      [input.entityType, input.entityId]
    );

    const existing = await client.query<OutboxRecord>(
      `SELECT *
       FROM public.sync_outbox
       WHERE entity_type = $1 AND entity_id = $2
       FOR UPDATE`,
      [input.entityType, input.entityId]
    );

    const previous = existing.rows[0];

    // CREATE + DELETE = Nothing to synchronize.
    if (
      previous?.operation === "CREATE" &&
      input.operation === "DELETE"
    ) {
      await client.query(
        `DELETE FROM public.sync_outbox
         WHERE entity_type = $1 AND entity_id = $2`,
        [input.entityType, input.entityId]
      );

      return null;
    }

    let operation: SyncOperation = input.operation;

    // Preserve CREATE if the entity hasn't reached the cloud.
    if (
      previous?.operation === "CREATE" &&
      input.operation === "UPDATE"
    ) {
      operation = "CREATE";
    }

    const result = await client.query<OutboxRecord>(
      `INSERT INTO public.sync_outbox (
         entity_type,
         entity_id,
         operation,
         payload
       )
       VALUES ($1, $2, $3, $4)
       ON CONFLICT (entity_type, entity_id)
       DO UPDATE SET
         operation = EXCLUDED.operation,
         payload = EXCLUDED.payload,
         revision = public.sync_outbox.revision + 1,
         change_id = gen_random_uuid(),
         updated_at = NOW()
       RETURNING *`,
      [
        input.entityType,
        input.entityId,
        operation,
        input.payload,
      ]
    );

    return result.rows[0] ?? null;
  }

  /**
   * Standalone enqueue for callers that don't already have a transaction.
   */
  async enqueue(
    input: EnqueueChangeInput
  ): Promise<OutboxRecord | null> {
    const client = await this.pool.connect();

    try {
      await client.query("BEGIN");

      const result = await this.enqueueWithClient(client, input);

      await client.query("COMMIT");

      return result;
    } catch (error) {
      await client.query("ROLLBACK");
      throw error;
    } finally {
      client.release();
    }
  }

  async getPending(limit = 100): Promise<OutboxRecord[]> {
    const result = await this.pool.query<OutboxRecord>(
      `SELECT *
       FROM public.sync_outbox
       ORDER BY updated_at ASC
       LIMIT $1`,
      [limit]
    );

    return result.rows;
  }

  async acknowledge(
    entityType: string,
    entityId: string,
    revision: string
  ): Promise<boolean> {
    const result = await this.pool.query(
      `DELETE FROM public.sync_outbox
       WHERE entity_type = $1
         AND entity_id = $2
         AND revision = $3`,
      [entityType, entityId, revision]
    );

    return (result.rowCount ?? 0) > 0;
  }
}
