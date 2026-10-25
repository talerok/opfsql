import {
  type Connection,
  type Database,
  MisuseError,
  openDatabase,
  OpfsqlError,
  type Result,
  type Value,
} from "../../../src/index.js";
import * as errors from "../../../src/index.js";
import type { FixtureApi, Outcome, PlainError, PlainResult } from "./api.js";

const workerUrl = new URL("worker.ts", import.meta.url);

let database: Database | null = null;
let connection: Connection | null = null;
const started = new Map<string, Promise<Outcome>>();

function current(): Connection {
  if (connection === null) throw new MisuseError("no database is open");
  return connection;
}

async function run(sql: string, parameters?: unknown[]): Promise<Result[]> {
  const values = parameters as Value[] | undefined;
  return values === undefined
    ? current().execute(sql)
    : [await current().query(sql, values)];
}

function plainResult(result: Result): PlainResult {
  let objects: PlainResult["objects"];
  try {
    objects = result.objects();
  } catch {
    objects = null;
  }
  return {
    columns: [...result.columns],
    rows: result.rows.map((row) => [...row]),
    objects,
    changes: result.changes,
    lastInsertRowid: result.lastInsertRowid,
  };
}

function plainError(error: unknown): PlainError {
  if (!(error instanceof Error)) {
    return {
      name: "unknown",
      code: null,
      message: String(error),
      isOpfsqlError: false,
      isItsClass: false,
      fields: {},
    };
  }
  const exported: unknown = Reflect.get(errors, error.name);
  return {
    name: error.name,
    code: error instanceof OpfsqlError ? error.code : null,
    message: error.message,
    isOpfsqlError: error instanceof OpfsqlError,
    isItsClass: typeof exported === "function" && error instanceof exported,
    fields: Object.fromEntries(Object.entries(error)),
  };
}

async function outcomeOf(work: Promise<Result[]>): Promise<Outcome> {
  try {
    const results = await work;
    return { ok: true, results: results.map((result) => plainResult(result)) };
  } catch (error) {
    return { ok: false, error: plainError(error) };
  }
}

const api: FixtureApi = {
  async open(name, options = {}) {
    database = await openDatabase(name, {
      ...options,
      worker: new Worker(workerUrl, { type: "module" }),
    });
    connection = await database.connect();
  },

  async attemptOpen(name) {
    return outcomeOf(
      (async () => {
        await api.open(name);
        return [];
      })(),
    );
  },

  async exec(sql, parameters) {
    const results = await run(sql, parameters);
    return results.map((result) => plainResult(result));
  },

  async attempt(sql, parameters) {
    return outcomeOf(run(sql, parameters));
  },

  start(id, sql) {
    started.set(id, outcomeOf(run(sql)));
  },

  async settle(id) {
    const outcome = started.get(id);
    if (outcome === undefined)
      throw new MisuseError(`nothing started as ${id}`);
    started.delete(id);
    return outcome;
  },

  async disconnect() {
    await connection?.close();
  },

  async close() {
    await connection?.close();
    connection = null;
    await database?.close();
    database = null;
  },

  async openIn(url) {
    const worker = new Worker(new URL(url, location.href), { type: "module" });
    return outcomeOf(
      (async () => {
        const opened = await openDatabase("unreachable", { worker });
        return [await opened.query("SELECT 1")];
      })(),
    );
  },

  async cleanOpfs(name) {
    const root = await navigator.storage.getDirectory();
    for (const suffix of [".opfsql", ".opfsql-wal", ".opfsql-shm"]) {
      try {
        await root.removeEntry(`${name}${suffix}`);
      } catch {
        // already gone
      }
    }
  },
};

Object.assign(globalThis, { opfsql: api });
const status = document.querySelector("#status");
if (status !== null) status.textContent = "ready";
