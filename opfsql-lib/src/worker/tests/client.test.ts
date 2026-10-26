import { resetMockOPFS } from "opfs-mock";
import { afterEach, beforeEach, describe, expect, it, vi } from "vitest";
import { collectGarbage } from "../../../test/support/gc.js";
import type { SessionContract } from "../../engine/api/index.js";
import { Engine, engineContract, Result } from "../../engine/index.js";
import { createEngine } from "../../engine/engine.js";
import {
  AbortError,
  BusyError,
  DatabaseInUseError,
  InternalError,
  MisuseError,
  SemanticError,
  SqlSyntaxError,
  WorkerError,
} from "../../errors.js";
import { MemoryFile } from "../../storage/file/sync-file.js";
import { type Database, openDatabase } from "../client.js";
import { failureOf } from "../../../test/support/failure.js";
import { Channel, CLOSE_TIMEOUT, START_TIMEOUT } from "../channel.js";
import {
  startSilentWorker,
  startTestWorker,
  type TestWorker,
} from "./harness.js";
import { rowsOf } from "../../../test/support/results.js";

// The client and the host together, over a real MessageChannel: what a page
// sees of a database served by a worker.

let seq = 0;
const opened: Database[] = [];

async function open(
  options: Partial<Parameters<typeof openDatabase>[1]> = {},
): Promise<{ database: Database; worker: TestWorker }> {
  const worker = startTestWorker();
  const database = await openDatabase(`client-${String(seq++)}`, {
    ...options,
    worker,
  });
  opened.push(database);
  return { database, worker };
}

const tick = async () => new Promise((resolve) => setTimeout(resolve, 20));

beforeEach(() => {
  resetMockOPFS();
});

afterEach(async () => {
  await Promise.all(opened.splice(0).map(async (database) => database.close()));
});

describe("a database served by a worker", () => {
  it("runs statements and returns results as Result objects", async () => {
    const { database } = await open();
    await database.execute(
      "CREATE TABLE t (id INTEGER PRIMARY KEY, v ANY); CREATE TABLE u (id INTEGER PRIMARY KEY)",
    );
    const insert = await database.query("INSERT INTO t (v) VALUES ($1), ($2)", [
      "a",
      new Uint8Array([1, 2]),
    ]);
    expect(insert).toBeInstanceOf(Result);
    expect(insert).toMatchObject({ changes: 2, lastInsertRowid: 2 });

    const result = await database.query(
      "SELECT t.id, t.id, v FROM t ORDER BY id",
    );
    expect(result.columns).toEqual(["id", "id", "v"]);
    expect(result.rows).toEqual([
      [1, 1, "a"],
      [2, 2, new Uint8Array([1, 2])],
    ]);
    expect(() => result.objects()).toThrow(MisuseError);

    const empty = await database.query(
      "SELECT id AS row_id, v FROM t WHERE id > 9",
    );
    expect(empty.columns).toEqual(["row_id", "v"]);
    expect(empty.objects()).toEqual([]);
  });

  it("passes its open options to the engine", async () => {
    const { database } = await open({ busyTimeout: 25 });
    await database.query("CREATE TABLE t (id INTEGER PRIMARY KEY)");
    const holder = await database.connect();
    const waiter = await database.connect();
    await holder.query("BEGIN IMMEDIATE");

    const error = await failureOf(waiter.query("INSERT INTO t VALUES (1)"));
    expect(error).toBeInstanceOf(BusyError);
    expect(error).toMatchObject({ code: "BUSY", timeout: 25 });
    await holder.query("ROLLBACK");
  });

  it("reports checkpoint failures through onCheckpointError", async () => {
    const failures: unknown[] = [];
    const worker = startTestWorker(async (name, options) => {
      const database = new MemoryFile();
      const files = { database, wal: new MemoryFile(), marks: null };
      const engine = await createEngine(files, {
        ...options,
        name,
        checkpointThreshold: 1,
      });
      // Opened: from now on, only a checkpoint writes the database file.
      vi.spyOn(database, "write").mockImplementation(() => {
        throw new Error("disk on fire");
      });
      return engineContract(engine);
    });
    const database = await openDatabase(`client-${String(seq++)}`, {
      worker,
      onCheckpointError: (error) => {
        failures.push(error);
      },
    });
    opened.push(database);

    await database.query("CREATE TABLE t (id INTEGER PRIMARY KEY)");
    await tick();
    expect(failures[0]).toBeInstanceOf(InternalError);
    expect(failures[0]).toMatchObject({ message: "disk on fire" });
  });

  it("fails to open with DATABASE_IN_USE where one worker per database is allowed, and terminates the worker", async () => {
    // opfs-mock allows one sync handle per file, as Firefox and WebKit do.
    const name = `client-${String(seq++)}`;
    const first = await openDatabase(name, { worker: startTestWorker() });
    opened.push(first);

    const worker = startTestWorker();
    const second = openDatabase(name, { worker });
    await expect(second).rejects.toBeInstanceOf(DatabaseInUseError);
    await expect(second).rejects.toMatchObject({
      code: "DATABASE_IN_USE",
      database: name,
    });
    await expect(second).rejects.toHaveProperty(
      "cause.name",
      "InvalidStateError",
    );
    expect(worker.isTerminated).toBe(true);
  });

  it("serves one database per worker", async () => {
    const { database, worker } = await open();
    await expect(openDatabase("another", { worker })).rejects.toBeInstanceOf(
      MisuseError,
    );
    // The worker still serves the first database.
    expect(await rowsOf(database.query("SELECT 1 AS one"))).toEqual([[1]]);
  });

  it("fails every pending and later call when the worker breaks", async () => {
    const { database, worker } = await open({ busyTimeout: Infinity });
    await database.query("CREATE TABLE t (id INTEGER PRIMARY KEY)");
    const holder = await database.connect();
    await holder.query("BEGIN IMMEDIATE");
    const pending = expect(
      database.query("INSERT INTO t VALUES (1)"),
    ).rejects.toThrow(WorkerError);

    worker.crash("out of memory");
    await pending;
    await expect(database.query("SELECT 1")).rejects.toThrow(/out of memory/);
    await expect(database.query("SELECT 1")).rejects.toMatchObject({
      code: "WORKER_ERROR",
    });
    expect(worker.isTerminated).toBe(true);
  });

  it("fails everything when a reply cannot be read", async () => {
    const { database, worker } = await open();
    worker.garble();
    await expect(database.query("SELECT 1")).rejects.toThrow(/unreadable/);
    await expect(database.query("SELECT 1")).rejects.toBeInstanceOf(
      WorkerError,
    );
  });

  it("reports a DOMException as the engine does: IO_ERROR, not INTERNAL", async () => {
    const refusal = new DOMException("the files are locked", "NotAllowedError");
    // Opening fails the way a file API does, after a while.
    const worker = startTestWorker(async () => {
      await tick();
      throw refusal;
    });
    await expect(
      openDatabase(`client-${String(seq++)}`, { worker }),
    ).rejects.toMatchObject({ code: "IO_ERROR", name: "IoError" });
  });

  it("refuses parameters that cannot cross to the worker, and goes on working", async () => {
    const { database } = await open();
    const notAValue = (() => 1) as unknown as string;
    await expect(
      database.query("SELECT $1", [notAValue]),
    ).rejects.toBeInstanceOf(MisuseError);
    expect(await rowsOf(database.query("SELECT 1 AS one"))).toEqual([[1]]);
  });

  it("refuses values that cross but are not values, as the engine does", async () => {
    const { database } = await open();
    const date = new Date(0) as unknown as string;
    await expect(database.query("SELECT $1", [date])).rejects.toMatchObject({
      name: "MisuseError",
      message: expect.stringMatching(/^parameter \$1 is a Date/) as unknown,
    });
  });

  it("closes once, settling what waits, terminating the worker", async () => {
    const { database, worker } = await open({ busyTimeout: Infinity });
    await database.query("CREATE TABLE t (id INTEGER PRIMARY KEY)");
    const holder = await database.connect();
    await holder.query("BEGIN IMMEDIATE");
    const waiting = expect(
      database.query("INSERT INTO t VALUES (1)"),
    ).rejects.toBeInstanceOf(AbortError);

    await Promise.all([database.close(), database.close()]);
    await waiting;
    expect(worker.isTerminated).toBe(true);
    await expect(database.query("SELECT 1")).rejects.toBeInstanceOf(
      MisuseError,
    );
    await expect(holder.query("SELECT 1")).rejects.toBeInstanceOf(MisuseError);
  });

  it("supports await using", async () => {
    const worker = startTestWorker();
    {
      await using database = await openDatabase(`client-${String(seq++)}`, {
        worker,
      });
      await database.query("SELECT 1");
    }
    expect(worker.isTerminated).toBe(true);
  });
});

describe("errors from the worker", () => {
  it("arrive as their classes, with their fields", async () => {
    const { database } = await open();
    await expect(database.query("SELEC 1")).rejects.toBeInstanceOf(
      SqlSyntaxError,
    );
    const error = await failureOf(database.query("SELECT 1 FROM t WHERE"));
    expect(error).toMatchObject({ code: "SYNTAX_ERROR", offset: 21 });
    await expect(database.query("SELECT * FROM nope")).rejects.toBeInstanceOf(
      SemanticError,
    );
    await expect(database.query("SELECT 1; SELECT 2")).rejects.toBeInstanceOf(
      MisuseError,
    );
  });

  it("say which statement of a script failed, which then left nothing behind", async () => {
    const { database } = await open();
    await database.query("CREATE TABLE t (id INTEGER PRIMARY KEY)");
    await expect(
      database.execute(
        "INSERT INTO t VALUES (1); INSERT INTO t VALUES (1); INSERT INTO t VALUES (2)",
      ),
    ).rejects.toMatchObject({
      code: "CONSTRAINT_VIOLATION",
      statementIndex: 1,
    });
    expect(await rowsOf(database.query("SELECT id FROM t"))).toEqual([]);
  });
});

describe("connections", () => {
  it("wait for the write lock one by one, without holding up readers", async () => {
    const { database } = await open({ busyTimeout: Infinity });
    await database.query("CREATE TABLE t (id INTEGER PRIMARY KEY)");
    const holder = await database.connect();
    const writer = await database.connect();
    await holder.query("BEGIN IMMEDIATE");
    await holder.query("INSERT INTO t VALUES (1)");

    let isWritten = false;
    const write = (async () => {
      await writer.query("INSERT INTO t VALUES (2)");
      isWritten = true;
    })();
    // The writer waits, and neither the database nor other connections do.
    expect(await rowsOf(database.query("SELECT count(*) AS n FROM t"))).toEqual(
      [[0]],
    );
    expect(isWritten).toBe(false);

    await holder.query("COMMIT");
    await write;
    expect(
      await rowsOf(database.query("SELECT id FROM t ORDER BY id")),
    ).toEqual([[1], [2]]);
  });

  it("run their calls in the order made, whatever is awaited", async () => {
    const { database } = await open();
    await database.query("CREATE TABLE t (id INTEGER PRIMARY KEY)");
    const connection = await database.connect();
    await Promise.all([
      connection.execute("BEGIN; INSERT INTO t VALUES (1); ROLLBACK"),
      connection.query("INSERT INTO t VALUES (2)"),
      connection.query("INSERT INTO t VALUES (3)"),
    ]);
    expect(
      await rowsOf(database.query("SELECT id FROM t ORDER BY id")),
    ).toEqual([[2], [3]]);
  });

  it("run their calls in the order their messages arrived, whatever is awaited on the way", async () => {
    // Each query waits before it reaches the session, the earlier ones
    // longer: only the host's queue keeps the later ones from overtaking.
    const waits: number[] = [];
    const worker = startTestWorker(async (name, options) => {
      const contract = engineContract(await Engine.open(name, options));
      return {
        ...contract,
        connect: (sessionOptions) =>
          waitingBeforeQuery(contract.connect(sessionOptions), waits),
      };
    });
    const database = await openDatabase(`client-${String(seq++)}`, {
      worker,
    });
    opened.push(database);
    await database.execute(
      "CREATE TABLE log (id INTEGER PRIMARY KEY AUTOINCREMENT, v TEXT)",
    );
    const connection = await database.connect();
    waits.push(60, 30, 0);
    await Promise.all(
      ["first", "second", "third"].map(async (v) =>
        connection.query("INSERT INTO log (v) VALUES ($1)", [v]),
      ),
    );
    expect(
      await rowsOf(database.query("SELECT v FROM log ORDER BY id")),
    ).toEqual([["first"], ["second"], ["third"]]);
  });

  it("can be cancelled while waiting for the lock", async () => {
    const { database } = await open({ busyTimeout: Infinity });
    await database.query("CREATE TABLE t (id INTEGER PRIMARY KEY)");
    const holder = await database.connect();
    await holder.query("BEGIN IMMEDIATE");

    const controller = new AbortController();
    const waiting = expect(
      database.query("INSERT INTO t VALUES (1)", [], {
        signal: controller.signal,
      }),
    ).rejects.toBeInstanceOf(AbortError);
    await tick();
    controller.abort();
    await waiting;

    await holder.query("COMMIT");
    expect(await rowsOf(database.query("SELECT id FROM t"))).toEqual([]);
  });

  it("closed while a call waits for the lock, cancel it and keep the lock free", async () => {
    const { database } = await open({ busyTimeout: Infinity });
    await database.query("CREATE TABLE t (id INTEGER PRIMARY KEY)");
    const holder = await database.connect();
    const waiter = await database.connect();
    await holder.query("BEGIN IMMEDIATE");

    const waiting = expect(
      waiter.query("INSERT INTO t VALUES (1)"),
    ).rejects.toBeInstanceOf(AbortError);
    // Queued behind it, in the worker: closing fails it too, unrun.
    const queued = expect(
      waiter.query("INSERT INTO t VALUES (3)"),
    ).rejects.toBeInstanceOf(AbortError);
    await tick();
    await waiter.close();
    await waiting;
    await queued;
    await expect(waiter.query("SELECT 1")).rejects.toBeInstanceOf(MisuseError);

    await holder.query("ROLLBACK");
    await database.query("INSERT INTO t VALUES (2)");
  });

  it("collected without close(), roll back and release the lock", async () => {
    const { database } = await open({ busyTimeout: 200 });
    await database.query("CREATE TABLE t (id INTEGER PRIMARY KEY)");
    await (async () => {
      const forgotten = await database.connect();
      await forgotten.query("BEGIN IMMEDIATE");
      await forgotten.query("INSERT INTO t VALUES (1)");
    })();

    await collectGarbage();
    await tick();
    await database.query("INSERT INTO t VALUES (2)");
    expect(await rowsOf(database.query("SELECT id FROM t"))).toEqual([[2]]);
  });
});

describe("connections nobody holds", () => {
  it("stay open while a call on them runs", async () => {
    const { database } = await open({ busyTimeout: Infinity });
    await database.query("CREATE TABLE t (id INTEGER PRIMARY KEY)");
    const holder = await database.connect();
    await holder.query("BEGIN IMMEDIATE");
    const pending = (async () => {
      const connection = await database.connect();
      return connection.query("INSERT INTO t VALUES (1)");
    })();

    await tick();
    await collectGarbage();
    await holder.query("COMMIT");
    await expect(pending).resolves.toMatchObject({ changes: 1 });
  });
});

describe("transactions", () => {
  it("cannot outlive a call on the database's own connection", async () => {
    const { database } = await open();
    await database.query("CREATE TABLE t (id INTEGER PRIMARY KEY)");
    await expect(database.query("BEGIN")).rejects.toBeInstanceOf(MisuseError);
    await expect(
      database.execute("BEGIN; INSERT INTO t VALUES (1)"),
    ).rejects.toBeInstanceOf(MisuseError);
    expect(await rowsOf(database.query("SELECT id FROM t"))).toEqual([]);

    await database.execute("BEGIN; INSERT INTO t VALUES (2); COMMIT");
    expect(await rowsOf(database.query("SELECT id FROM t"))).toEqual([[2]]);
  });

  it("transaction() commits, and runs apart from other calls on the database", async () => {
    const { database } = await open();
    await database.query("CREATE TABLE t (id INTEGER PRIMARY KEY)");
    const value = await database.transaction(async (tx) => {
      await tx.query("INSERT INTO t VALUES (1)");
      // The database's own connection does not see the uncommitted row.
      expect(await rowsOf(database.query("SELECT id FROM t"))).toEqual([]);
      return "done";
    });
    expect(value).toBe("done");
    expect(await rowsOf(database.query("SELECT id FROM t"))).toEqual([[1]]);
  });

  it("transaction() rolls back and rethrows when fn throws", async () => {
    const { database } = await open();
    await database.query("CREATE TABLE t (id INTEGER PRIMARY KEY)");
    const failure = new Error("changed my mind");
    await expect(
      database.transaction(async (tx) => {
        await tx.query("INSERT INTO t VALUES (1)");
        throw failure;
      }),
    ).rejects.toBe(failure);
    expect(await rowsOf(database.query("SELECT id FROM t"))).toEqual([]);
    await database.query("INSERT INTO t VALUES (2)");
  });

  it("transaction() commits what work did around a statement error it caught", async () => {
    const { database } = await open();
    await database.query("CREATE TABLE t (id INTEGER PRIMARY KEY)");
    await database.transaction(async (tx) => {
      await tx.query("INSERT INTO t VALUES (1)");
      const failure = await failureOf(tx.query("INSERT INTO t VALUES (1)"));
      expect(failure).toMatchObject({ code: "CONSTRAINT_VIOLATION" });
      await tx.query("INSERT INTO t VALUES (2)");
    });
    expect(await rowsOf(database.query("SELECT id FROM t"))).toEqual([
      [1],
      [2],
    ]);
  });
});

describe("prepared statements", () => {
  it("fail at prepare when they cannot bind", async () => {
    const { database } = await open();
    await expect(database.prepare("SELECT * FROM nope")).rejects.toBeInstanceOf(
      SemanticError,
    );
  });

  it("bind again after a schema change, with the new columns", async () => {
    const { database } = await open();
    await database.query("CREATE TABLE t (id INTEGER PRIMARY KEY)");
    await database.query("INSERT INTO t VALUES (1)");
    const statement = await database.prepare("SELECT * FROM t WHERE id = $1");
    expect(statement.columns).toEqual(["id"]);

    const other = await database.connect();
    await other.query("ALTER TABLE t ADD COLUMN v INTEGER DEFAULT 5");
    const result = await statement.query([1]);
    expect(result.objects()).toEqual([{ id: 1, v: 5 }]);
    expect(statement.columns).toEqual(["id", "v"]);

    await statement.close();
    await expect(statement.query([1])).rejects.toBeInstanceOf(MisuseError);
  });
});

describe("schema()", () => {
  it("describes tables and indexes without storage details", async () => {
    const { database } = await open();
    await database.execute(`
      CREATE TABLE users (id INTEGER PRIMARY KEY, email TEXT NOT NULL UNIQUE, data JSON);
      CREATE INDEX users_email ON users (lower(email));
      CREATE UNIQUE INDEX users_name ON users (data.name, id);
    `);
    const schema = await database.schema();
    expect(schema).toEqual({
      tables: [
        {
          name: "users",
          columns: [
            expect.objectContaining({ name: "id", primaryKey: true }),
            expect.objectContaining({
              name: "email",
              type: "TEXT",
              nullable: false,
              unique: true,
            }),
            expect.objectContaining({ name: "data", type: "JSON" }),
          ],
          indexes: [
            expect.objectContaining({
              name: "users_email",
              unique: false,
              expressions: [expect.stringMatching(/^lower\(email\)$/i)],
            }),
            {
              name: "users_name",
              unique: true,
              expressions: ["data.name", "id"],
            },
          ],
        },
      ],
    });
    expect(JSON.stringify(schema)).not.toMatch(/PageNo|version/);
  });
});

describe("the worker's life cycle", () => {
  afterEach(() => {
    vi.useRealTimers();
  });

  it("refuses a page of another protocol version", async () => {
    const worker = startTestWorker();
    const channel = new Channel(worker, undefined);
    const opening = channel.request({
      type: "open",
      protocol: 99,
      name: `client-${String(seq++)}`,
      options: {},
    });
    await expect(opening).rejects.toBeInstanceOf(MisuseError);
    await expect(opening).rejects.toThrow(/protocol 99/);
    channel.terminate();
  });

  it("gives up on a worker that never serves a database", async () => {
    vi.useFakeTimers({ toFake: ["setTimeout", "clearTimeout"] });
    const worker = startSilentWorker();
    const opening = expect(
      openDatabase(`client-${String(seq++)}`, { worker }),
    ).rejects.toMatchObject({
      code: "WORKER_ERROR",
      message: expect.stringMatching(/serveDatabase/) as unknown,
    });
    await vi.advanceTimersByTimeAsync(START_TIMEOUT);
    await opening;
    expect(worker.isTerminated).toBe(true);
  });

  it("terminates a worker that does not finish closing in time", async () => {
    const worker = startTestWorker(async (name, options) => {
      const engine = await Engine.open(name, options);
      const never = Promise.withResolvers<undefined>();
      return { ...engineContract(engine), close: async () => never.promise };
    });
    const database = await openDatabase(`client-${String(seq++)}`, {
      worker,
    });
    vi.useFakeTimers({ toFake: ["setTimeout", "clearTimeout"] });
    const closing = database.close();
    await vi.advanceTimersByTimeAsync(CLOSE_TIMEOUT);
    await closing;
    expect(worker.isTerminated).toBe(true);
  });
});

describe("the database's own calls", () => {
  it("run concurrently: a read never waits behind a write waiting for the lock", async () => {
    const { database } = await open({ busyTimeout: Infinity });
    await database.query("CREATE TABLE t (id INTEGER PRIMARY KEY)");
    const holder = await database.connect();
    await holder.query("BEGIN IMMEDIATE");
    const write = database.query("INSERT INTO t VALUES (1)");
    expect(await rowsOf(database.query("SELECT count(*) FROM t"))).toEqual([
      [0],
    ]);
    await holder.query("COMMIT");
    await write;
    expect(await rowsOf(database.query("SELECT id FROM t"))).toEqual([[1]]);
  });

  it("prepare a statement on a session of its own, closed with it", async () => {
    const { database } = await open({ busyTimeout: 0 });
    await database.query("CREATE TABLE t (id INTEGER PRIMARY KEY)");
    const statement = await database.prepare("INSERT INTO t VALUES ($1)");
    expect(statement.connection).toBeNull();
    await statement.query([1]);
    await statement.close();
    await expect(statement.query([2])).rejects.toBeInstanceOf(MisuseError);
    await database.query("INSERT INTO t VALUES (2)");
    expect(await rowsOf(database.query("SELECT id FROM t"))).toEqual([
      [1],
      [2],
    ]);
  });
});

/**
 * `session`, with an await before each query reaches it: the next of
 * `waits`, in ms, while there are any.
 */
function waitingBeforeQuery(
  session: SessionContract,
  waits: number[],
): SessionContract {
  return {
    query: async (...call) => {
      await new Promise((resolve) => setTimeout(resolve, waits.shift() ?? 0));
      return session.query(...call);
    },
    execute: async (...call) => session.execute(...call),
    prepare: async (...call) => session.prepare(...call),
    begin: async (...call) => session.begin(...call),
    commit: async (...call) => session.commit(...call),
    rollback: async (...call) => session.rollback(...call),
    schema: async (...call) => session.schema(...call),
    close: async () => session.close(),
  };
}
