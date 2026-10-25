// "opfsql/worker": what a worker imports — serveDatabase() for the worker
// that serves the page, and the engine itself for code that already runs in
// a worker. Errors and types come from "opfsql".

import { Engine, engineContract } from "./engine/index.js";
import { type HostScope, startHost } from "./worker/index.js";

/**
 * Makes this dedicated worker serve a database to openDatabase() in the page.
 * Call it once, in the worker's script.
 */
export function serveDatabase(scope?: HostScope): void {
  startHost(scope ?? globalThis, async (name, options) =>
    engineContract(await Engine.open(name, options)),
  );
}

export {
  Engine,
  type PreparedStatement,
  type Session,
} from "./engine/index.js";
export type { HostScope } from "./worker/index.js";
