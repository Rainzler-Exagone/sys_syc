
import { Pool } from "pg";
import "dotenv/config";

function createPool(connectionString: string | undefined, name: string) {
  if (!connectionString) {
    throw new Error(`${name} connection string is missing`);
  }

  return new Pool({
    connectionString,
    connectionTimeoutMillis: 5000,
    max: 5,
    idleTimeoutMillis: 30000,
  });
}

export const localDB = createPool(
  process.env.LOCAL_DATABASE_URL,
  "Local database",
);

export const cloudDB = createPool(
  process.env.CLOUD_DATABASE_URL,
  "Cloud database",
);



export async function checkLocalConnection(): Promise<boolean> {
  try {
    await localDB.query("SELECT 1");
    console.log("LOCAL database connected");
    return true;
  } catch (error) {
    console.error("LOCAL database connection failed:", error);
    return false;
  }
}

export async function checkCloudConnection(): Promise<boolean> {
  try {
    await cloudDB.query("SELECT 1");
    console.log("CLOUD database connected");
    return true;
  } catch (error) {
    console.error("CLOUD database unavailable:", error);
    return false;
  }
}
