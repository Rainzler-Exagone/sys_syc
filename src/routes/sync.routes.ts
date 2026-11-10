
import { IncomingMessage, ServerResponse } from "node:http";
import type { PoolClient } from "pg";

import { localDB } from "../config/database.js";
import { InboxService } from "../services/inbox.service.js";

import type { ReceiveChangeInput } from "../db/schemas/inbox.schema.js";
import type { NotePayload } from "../db/schemas/notes.schema.js";

const inboxService = new InboxService(localDB);

const UUID_REGEX =
  /^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$/i;

const ENTITY_TYPE = "note";

class PendingLocalChangeError extends Error {
  constructor() {
    super("Local changes are pending synchronization");
    this.name = "PendingLocalChangeError";
  }
}

function sendJSON(
  res: ServerResponse,
  status: number,
  data: unknown
) {
  res.writeHead(status, {
    "Content-Type": "application/json",
  });

  res.end(JSON.stringify(data));
}

async function readBody(req: IncomingMessage): Promise<any> {
  let body = "";

  for await (const chunk of req) {
    body += chunk;
  }

  return JSON.parse(body);
}

function isObject(value: unknown): value is Record<string, any> {
  return (
    value !== null &&
    typeof value === "object" &&
    !Array.isArray(value)
  );
}

function validateIncomingChange(body: any): boolean {
  if (!isObject(body) || !isObject(body.payload)) {
    return false;
  }

  const { payload } = body;

  if (
    !UUID_REGEX.test(body.sourceDatabaseId) ||
    !UUID_REGEX.test(body.changeId) ||
    !UUID_REGEX.test(body.entityId) ||
    body.entityType !== ENTITY_TYPE ||
    !["CREATE", "UPDATE", "DELETE"].includes(body.operation) ||
    payload.id !== body.entityId
  ) {
    return false;
  }

  if (body.operation !== "DELETE") {
    if (
      typeof payload.title !== "string" ||
      (
        payload.content !== undefined &&
        payload.content !== null &&
        typeof payload.content !== "string"
      )
    ) {
      return false;
    }
  }

  if (
    payload.version !== undefined &&
    (
      !Number.isInteger(payload.version) ||
      payload.version < 1
    )
  ) {
    return false;
  }

  return true;
}

/**
 * Apply an incoming cloud snapshot to local notes.
 * The InboxService transaction owns the client.
 */
async function applyIncomingNote(
  client: PoolClient,
  operation: ReceiveChangeInput["operation"],
  entityId: string,
  payload: NotePayload
): Promise<void> {

  // Use the same advisory lock as OutboxService.
  await client.query(
    `SELECT pg_advisory_xact_lock(
       hashtextextended($1 || ':' || $2, 0)
     )`,
    [ENTITY_TYPE, entityId]
  );

  // Never overwrite a note with pending local changes.
  const pending = await client.query(
    `SELECT 1
     FROM public.sync_outbox
     WHERE entity_type = $1
       AND entity_id = $2
     FOR UPDATE`,
    [ENTITY_TYPE, entityId]
  );

  if (pending.rowCount && pending.rowCount > 0) {
    throw new PendingLocalChangeError();
  }

  // CREATE / UPDATE: apply complete cloud snapshot.
  if (operation === "CREATE" || operation === "UPDATE") {
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
         $1,
         $2,
         $3,
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

  // DELETE: preserve the note as a tombstone.
  if (operation === "DELETE") {
    await client.query(
      `UPDATE public.notes
       SET deleted_at = COALESCE($2, NOW()),
           updated_at = COALESCE($3, NOW()),
           version = COALESCE($4, version + 1)
       WHERE id = $1
         AND deleted_at IS NULL`,
      [
        entityId,
        payload.deleted_at ?? null,
        payload.updated_at ?? null,
        payload.version ?? null,
      ]
    );
  }
}

export async function syncRoutes(
  req: IncomingMessage,
  res: ServerResponse
): Promise<boolean> {

  const url = new URL(req.url || "/", "http://localhost");
  const method = req.method;
  const pathname = url.pathname;

  if (pathname !== "/api/sync/incoming") {
    return false;
  }

  if (method !== "POST") {
    sendJSON(res, 405, { error: "Method not allowed" });
    return true;
  }

  try {
    const body = await readBody(req);

    if (!validateIncomingChange(body)) {
      sendJSON(res, 400, {
        error: "Invalid incoming change",
      });
      return true;
    }

    const input: ReceiveChangeInput = {
      sourceDatabaseId: body.sourceDatabaseId,
      changeId: body.changeId,
      entityType: body.entityType,
      entityId: body.entityId,
      operation: body.operation,
    };

    const payload = body.payload as NotePayload;

    const processed = await inboxService.processIncoming(
      input,
      async (client) => {
        await applyIncomingNote(
          client,
          input.operation,
          input.entityId,
          payload
        );
      }
    );

    if (!processed) {
      sendJSON(res, 200, {
        processed: false,
        duplicate: true,
      });

      return true;
    }

    sendJSON(res, 200, {
      processed: true,
      duplicate: false,
      entityId: input.entityId,
    });

    return true;

  } catch (error) {
    if (error instanceof SyntaxError) {
      sendJSON(res, 400, {
        error: "Invalid JSON body",
      });

      return true;
    }

    if (error instanceof PendingLocalChangeError) {
      sendJSON(res, 409, {
        error: "Local changes are pending synchronization",
        retryable: true,
      });

      return true;
    }

    console.error("Incoming sync error:", error);

    sendJSON(res, 500, {
      error: "Failed to process incoming change",
    });

    return true;
  }
}
