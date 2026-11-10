
import "dotenv/config";
import { Pool } from "pg";
import { readdir, readFile } from "node:fs/promises";
import path from "node:path";

const migrationsDirectory = path.join(
  process.cwd(),
  "src/db/migrations"
);

async function migrateDatabase(
  name: string,
  pool: Pool
): Promise<void> {
  console.log(`\nRunning migrations on ${name}...`);

  const client = await pool.connect();

  try {
    // Track migrations already executed.
    await client.query(`
      CREATE TABLE IF NOT EXISTS public.schema_migrations (
        id SERIAL PRIMARY KEY,
        filename TEXT UNIQUE NOT NULL,
        executed_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
      )
    `);

    const files = (await readdir(migrationsDirectory))
      .filter((file) => file.endsWith(".sql"))
      .sort();

    for (const file of files) {
      const alreadyExecuted = await client.query(
        `SELECT 1
         FROM public.schema_migrations
         WHERE filename = $1`,
        [file]
      );

      if (alreadyExecuted.rowCount) {
        console.log(`⏭ Skipped: ${file}`);
        continue;
      }

      const sql = await readFile(
        path.join(migrationsDirectory, file),
        "utf-8"
      );

      await client.query("BEGIN");

      try {
        await client.query(sql);

        await client.query(
          `INSERT INTO public.schema_migrations (filename)
           VALUES ($1)`,
          [file]
        );

        await client.query("COMMIT");

        console.log(`✓ Applied: ${file}`);
      } catch (error) {
        await client.query("ROLLBACK");
        throw error;
      }
    }

    console.log(`✓ ${name} migrations completed.`);
  } finally {
    client.release();
  }
}

async function main() {
  const localUrl = process.env.LOCAL_DATABASE_URL;
  const cloudUrl = process.env.CLOUD_DATABASE_URL;

  if (!localUrl || !cloudUrl) {
    throw new Error("Missing database connection URLs in .env");
  }

  const localPool = new Pool({
    connectionString: localUrl,
  });

  const cloudPool = new Pool({
    connectionString: cloudUrl,
    ssl: { rejectUnauthorized: false },
  });

  try {
    await migrateDatabase("LOCAL", localPool);
    await migrateDatabase("CLOUD", cloudPool);
  } finally {
    await Promise.all([
      localPool.end(),
      cloudPool.end(),
    ]);
  }
}

main().catch((error) => {
  console.error("Migration failed:", error);
  process.exitCode = 1;
});
