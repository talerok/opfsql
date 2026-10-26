import { inject, vi } from "vitest";
import { Engine } from "../engine.js";

declare module "vitest" {
  export interface ProvidedContext {
    pages: "default" | "small";
  }
}

/** What the "small-pages" project opens databases with. */
export const SMALL_PAGES = { pageSize: 4096, checkpointThreshold: 8 };

/**
 * Opens an engine with Engine.open, as the vitest project says
 * (vitest.config.ts):
 * - "default": the default page size and checkpoint threshold;
 * - "small-pages": 4 KB pages and a low checkpoint threshold, so that
 *   ordinary test data splits B-trees, spills into overflow pages and
 *   reaches checkpoints.
 *
 * The engine is integrity-checked when closed, so every engine-level test
 * also verifies the database it leaves behind.
 */
export async function openTestEngine(
  databaseName: string,
  options: { busyTimeout?: number } = {},
): Promise<Engine> {
  const pages = inject("pages") === "small" ? SMALL_PAGES : {};
  const engine = await Engine.open(databaseName, { ...options, ...pages });

  const close = engine.close.bind(engine);
  let closing: Promise<void> | null = null;
  vi.spyOn(engine, "close").mockImplementation(async () => {
    closing ??= (async () => {
      const problems = await engine.checkIntegrity();
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
