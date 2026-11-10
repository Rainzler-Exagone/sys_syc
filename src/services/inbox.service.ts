
import { Pool } from "pg";
import type { PoolClient } from "pg";
import type { ReceiveChangeInput } from "../db/schemas/inbox.schema.js"

export class InboxService {
  constructor(private readonly pool: Pool) {}

  /**
   * Process an incoming remote change exactly once.
   *
   * Returns:
   * true  -> Change was new and successfully applied.
   * false -> Change was already processed.
   */
  async processIncoming(
    input: ReceiveChangeInput,
    applyChange: (client: PoolClient) => Promise<void>
  ): Promise<boolean> {
    const client = await this.pool.connect();

    try {
      await client.query("BEGIN");

      // 1. Register the incoming change.
      const result = await client.query(
        `INSERT INTO public.sync_inbox (
          source_database_id,
          change_id,
          entity_type,
          entity_id,
          operation
        )
        VALUES ($1, $2, $3, $4, $5)
        ON CONFLICT (source_database_id, change_id)
        DO NOTHING
        RETURNING id`,
        [
          input.sourceDatabaseId,
          input.changeId,
          input.entityType,
          input.entityId,
          input.operation,
        ]
      );

      // 2. Already processed: skip it.
      if (result.rowCount === 0) {
        await client.query("COMMIT");
        return false;
      }

      // 3. Apply the actual change.
      await applyChange(client);

      // 4. Commit both operations together.
      await client.query("COMMIT");

      return true;
    } catch (error) {
      await client.query("ROLLBACK");
      throw error;
    } finally {
      client.release();
    }
  }
}
