import { inject } from "vitest";
import { OPFSSyncStorage } from "../../store/backend/opfs-storage.js";
import { Engine } from "../engine.js";

declare module "vitest" {
  export interface ProvidedContext {
    backend: "opfs" | "wal";
  }
}

/**
 * Opens an engine on the backend of the current vitest project (vitest.config.ts):
 * - "opfs": OPFSSyncStorage directly — the engine in isolation.
 * - "wal":  Engine.open, the production stack (WAL + checkpoints), with 4 KB
 *           pages and a low checkpoint threshold so that ordinary test data
 *           reaches page limits and checkpoints.
 */
export function openTestEngine(dbName: string): Promise<Engine> {
  if (inject("backend") === "wal") {
    return Engine.open(dbName, { pageSize: 4096, checkpointThreshold: 8 });
  }
  return Engine.create(new OPFSSyncStorage(dbName));
}
