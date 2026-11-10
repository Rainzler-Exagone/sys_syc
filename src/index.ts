
import "dotenv/config";
import { createServer } from "node:http";
import {
    localDB,
    cloudDB,
    checkCloudConnection,
    checkLocalConnection,
} from "./config/database.js";
import { notesRoutes } from "./routes/notes.routes.js";
import { SyncWorker } from "./sync/sync.worker.js";

const PORT = Number(process.env.PORT) || 3000;

async function main() {
    // Verify both database connections before starting the API

    const localConnected = await checkLocalConnection();
    const cloudConnected = await checkCloudConnection();

    if (!cloudConnected) {
        console.error("CLOUD database unavailable.");
    }
    if (!localConnected) {
        console.error("LOCAL database unavailable. API startup aborted.");
        process.exitCode = 1;
        return;
    }

    
    // Start the synchronization worker
    const syncWorker = new SyncWorker();
    syncWorker.start();

    const server = createServer(async (req, res) => {
        try {
            const handled = await notesRoutes(req, res);

            if (!handled) {
                res.writeHead(404, {
                    "Content-Type": "application/json",
                });

                res.end(JSON.stringify({
                    error: "Route not found",
                }));
            }
        } catch (error) {
            console.error("Server error:", error);

            if (!res.headersSent) {
                res.writeHead(500, {
                    "Content-Type": "application/json",
                });
            }

            if (!res.writableEnded) {
                res.end(JSON.stringify({
                    error: "Internal server error",
                }));
            }
        }
    });

    server.listen(PORT, () => {
        console.log(`API running at http://localhost:${PORT}`);
    });

    // Graceful shutdown
    const shutdown = () => {
        console.log("\nShutting down server...");

        syncWorker.stop();

        server.close(async () => {
            await Promise.all([
                localDB.end(),
                cloudDB.end(),
            ]);

            console.log("Database connections closed.");
            process.exit(0);
        });
    };

    process.on("SIGINT", shutdown);
    process.on("SIGTERM", shutdown);
}

main().catch((error) => {
    console.error("Startup error:", error);
    process.exitCode = 1;
});
