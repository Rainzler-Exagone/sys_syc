
import { localDB, cloudDB } from "./config/database.js";

const schema = `
  CREATE TABLE IF NOT EXISTS notes (
    id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    title VARCHAR(255) NOT NULL,
    content TEXT,
    version INTEGER NOT NULL DEFAULT 1,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    deleted_at TIMESTAMPTZ
  );

  CREATE INDEX IF NOT EXISTS idx_notes_updated_at
  ON notes(updated_at);
`;

async function setup() {
  try {
    console.log("Setting up local database...");
    await localDB.query(schema);
    console.log("Local schema created.");

    console.log("Setting up cloud database...");
    await cloudDB.query(schema);
    console.log("Cloud schema created.");

    console.log("Both databases are ready.");
  } catch (error) {
    console.error("Database setup failed:", error);
    process.exitCode = 1;
  } finally {
    await Promise.all([
      localDB.end(),
      cloudDB.end(),
    ]);
  }
}

setup();
