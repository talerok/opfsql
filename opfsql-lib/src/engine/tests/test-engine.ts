import { inject, vi } from "vitest";
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
 *
 * The engine is integrity-checked when closed, so every engine-level test
 * also verifies the database it leaves behind.
 */
export async function openTestEngine(dbName: string): Promise<Engine> {
  const engine =
    inject("backend") === "wal"
      ? await Engine.open(dbName, { pageSize: 4096, checkpointThreshold: 8 })
      : await Engine.create(new OPFSSyncStorage(dbName));

  const close = engine.close.bind(engine);
  vi.spyOn(engine, "close").mockImplementation(() => {
    const problems = engine.checkIntegrity();
    close();
    if (problems.length > 0) {
      throw new Error(
        `integrity check failed for "${dbName}":\n  ${problems.join("\n  ")}`,
      );
    }
  });
  return engine;
}
