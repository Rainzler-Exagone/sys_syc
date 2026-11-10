
import { localDB, cloudDB } from "../config/database.js";

async function checkDatabase(name: string, db: typeof localDB) {
  try {
    const result = await db.query(`
      SELECT
        current_database() AS database,
        current_user AS username,
        NOW() AS server_time
    `);

    console.log(`✅ ${name} connected`, result.rows[0]);
    return true;
  } catch (error) {
    console.error(`❌ ${name} connection failed:`, error);
    return false;
  }
}

export async function checkConnections() {
  const [local, cloud] = await Promise.all([
    checkDatabase("LOCAL", localDB),
    checkDatabase("CLOUD", cloudDB),
  ]);

  return { local, cloud };
}

