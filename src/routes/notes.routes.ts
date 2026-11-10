
import { IncomingMessage, ServerResponse } from "node:http";
import type { PoolClient } from "pg";

import { localDB } from "../config/database.js";
import { OutboxService } from "../services/outbox.service.js";

const outboxService = new OutboxService(localDB);

const ENTITY_TYPE = "note";

const UUID_REGEX =
  /^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$/i;

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

/**
 * Execute multiple database operations atomically.
 */
async function withTransaction<T>(
  callback: (client: PoolClient) => Promise<T>
): Promise<T> {
  const client = await localDB.connect();

  try {
    await client.query("BEGIN");

    const result = await callback(client);

    await client.query("COMMIT");

    return result;
  } catch (error) {
    await client.query("ROLLBACK");
    throw error;
  } finally {
    client.release();
  }
}

function isValidNoteBody(body: any): boolean {
  return (
    body !== null &&
    typeof body === "object" &&
    !Array.isArray(body)
  );
}

export async function notesRoutes(
  req: IncomingMessage,
  res: ServerResponse
): Promise<boolean> {
  const url = new URL(req.url || "/", "http://localhost");
  const method = req.method;
  const pathname = url.pathname;

  if (
    pathname !== "/api/notes" &&
    !pathname.startsWith("/api/notes/")
  ) {
    return false;
  }

  try {
    // ----------------------------------------
    // GET /api/notes
    // ----------------------------------------
    if (method === "GET" && pathname === "/api/notes") {
      const result = await localDB.query(
        `SELECT *
         FROM public.notes
         WHERE deleted_at IS NULL
         ORDER BY created_at DESC`
      );

      sendJSON(res, 200, result.rows);
      return true;
    }

    // ----------------------------------------
    // POST /api/notes
    // ----------------------------------------
    if (method === "POST" && pathname === "/api/notes") {
      const body = await readBody(req);

      if (
        !isValidNoteBody(body) ||
        typeof body.title !== "string" ||
        !body.title.trim() ||
        (
          body.content !== undefined &&
          body.content !== null &&
          typeof body.content !== "string"
        )
      ) {
        sendJSON(res, 400, { error: "Invalid note data" });
        return true;
      }

      const note = await withTransaction(async (client) => {

        // 1. Create the note locally.
        const result = await client.query(
          `INSERT INTO public.notes (title, content)
           VALUES ($1, $2)
           RETURNING *`,
          [body.title.trim(), body.content ?? null]
        );

        const createdNote = result.rows[0];

        // 2. Add CREATE to the outbox.
        await outboxService.enqueueWithClient(client, {
          entityType: ENTITY_TYPE,
          entityId: createdNote.id,
          operation: "CREATE",
          payload: createdNote,
        });

        return createdNote;
      });

      sendJSON(res, 201, note);
      return true;
    }

    // ----------------------------------------
    // GET /api/notes/:id
    // ----------------------------------------
    const match = pathname.match(/^\/api\/notes\/([^/]+)$/);

    if (!match) {
      sendJSON(res, 404, { error: "Route not found" });
      return true;
    }

    const id = match[1];

    if (!UUID_REGEX.test(id as string)) {
      sendJSON(res, 400, { error: "Invalid note ID" });
      return true;
    }

    if (method === "GET") {
      const result = await localDB.query(
        `SELECT *
         FROM public.notes
         WHERE id = $1
           AND deleted_at IS NULL`,
        [id]
      );

      if (!result.rowCount) {
        sendJSON(res, 404, { error: "Note not found" });
        return true;
      }

      sendJSON(res, 200, result.rows[0]);
      return true;
    }

    // ----------------------------------------
    // PATCH /api/notes/:id
    // ----------------------------------------
    if (method === "PATCH") {
      const body = await readBody(req);

      if (
        !isValidNoteBody(body) ||
        (
          body.title !== undefined &&
          (
            typeof body.title !== "string" ||
            !body.title.trim()
          )
        ) ||
        (
          body.content !== undefined &&
          body.content !== null &&
          typeof body.content !== "string"
        ) ||
        (
          body.title === undefined &&
          body.content === undefined
        )
      ) {
        sendJSON(res, 400, { error: "Invalid update data" });
        return true;
      }

      const updatedNote = await withTransaction(async (client) => {

        // 1. Update the local note.
        const result = await client.query(
          `UPDATE public.notes
           SET title = COALESCE($2, title),
               content = CASE
                 WHEN $3 THEN $4
                 ELSE content
               END,
               version = version + 1,
               updated_at = NOW()
           WHERE id = $1
             AND deleted_at IS NULL
           RETURNING *`,
          [
            id,
            body.title !== undefined ? body.title.trim() : null,
            body.content !== undefined,
            body.content ?? null,
          ]
        );

        if (!result.rowCount) {
          return null;
        }

        const note = result.rows[0];

        // 2. Coalesce UPDATE into the existing outbox entry.
        await outboxService.enqueueWithClient(client, {
          entityType: ENTITY_TYPE,
          entityId: note.id,
          operation: "UPDATE",
          payload: note,
        });

        return note;
      });

      if (!updatedNote) {
        sendJSON(res, 404, { error: "Note not found" });
        return true;
      }

      sendJSON(res, 200, updatedNote);
      return true;
    }

    // ----------------------------------------
    // DELETE /api/notes/:id
    // ----------------------------------------
    if (method === "DELETE") {
      const deletedNote = await withTransaction(async (client) => {

        // 1. Soft-delete the local note.
        const result = await client.query(
          `UPDATE public.notes
           SET deleted_at = NOW(),
               updated_at = NOW(),
               version = version + 1
           WHERE id = $1
             AND deleted_at IS NULL
           RETURNING *`,
          [id]
        );

        if (!result.rowCount) {
          return null;
        }

        const note = result.rows[0];

        // 2. Enqueue DELETE with the latest note snapshot.
        await outboxService.enqueueWithClient(client, {
          entityType: ENTITY_TYPE,
          entityId: note.id,
          operation: "DELETE",
          payload: note,
        });

        return note;
      });

      if (!deletedNote) {
        sendJSON(res, 404, { error: "Note not found" });
        return true;
      }

      sendJSON(res, 200, {
        message: "Note deleted successfully",
        id: deletedNote.id,
      });

      return true;
    }

    sendJSON(res, 405, { error: "Method not allowed" });
    return true;

  } catch (error) {
    console.error("Notes route error:", error);

    if (error instanceof SyntaxError) {
      sendJSON(res, 400, { error: "Invalid JSON body" });
      return true;
    }

    sendJSON(res, 500, { error: "Internal server error" });
    return true;
  }
}
