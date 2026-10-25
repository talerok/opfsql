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
export async function openTestEngine(
  databaseName: string,
  options: { busyTimeout?: number } = {},
): Promise<Engine> {
  const engine =
    inject("backend") === "wal"
      ? await Engine.open(databaseName, {
          ...options,
          pageSize: 4096,
          checkpointThreshold: 8,
        })
      : await Engine.create(new OPFSSyncStorage(databaseName), options);

  const close = engine.close.bind(engine);
  let closing: Promise<void> | null = null;
  vi.spyOn(engine, "close").mockImplementation(async () => {
    closing ??= (async () => {
      const problems = engine.checkIntegrity();
      await close();
      if (problems.length > 0) {
        throw new Error(
          `integrity check failed for "${databaseName}":\n  ${problems.join("\n  ")}`,
        );
      }
    })();
    return closing;
  });
  return engine;
}
