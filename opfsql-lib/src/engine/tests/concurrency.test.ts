import { resetMockOPFS } from "opfs-mock";
import { afterEach, beforeEach, describe, expect, it } from "vitest";
import { collectGarbage } from "../../../test/support/gc.js";
import { AbortError, BusyError, MisuseError } from "../../errors.js";
import { Engine } from "../engine.js";
import type { Session } from "../session.js";
import { tableNamesOf } from "../../../test/support/results.js";

// Sessions of one engine called concurrently, as async code does: calls not
// awaited one by one, sessions closed while they wait, engines closed under
// open transactions.

let seq = 0;
let engine: Engine;

async function openEngine(busyTimeout = 1000): Promise<Engine> {
  engine = await Engine.open(`concurrency-${String(seq++)}`, { busyTimeout });
  const setup = engine.connect();
  await setup.execute("CREATE TABLE t (id INTEGER PRIMARY KEY, v TEXT)");
  await setup.close();
  return engine;
}

async function ids(session: Session): Promise<unknown[]> {
  const result = await session.query("SELECT id FROM t ORDER BY id");
  return result.rows.map(([id]) => id);
}

const tick = async () => new Promise((resolve) => setTimeout(resolve, 20));

beforeEach(() => {
  resetMockOPFS();
});

afterEach(async () => {
  await engine.close();
});

describe("one session, calls not awaited one by one", () => {
  it("runs them in the order they were made", async () => {
    await openEngine();
    const session = engine.connect();
    await Promise.all([
      session.execute("BEGIN; INSERT INTO t (id) VALUES (1); ROLLBACK"),
      session.query("INSERT INTO t (id) VALUES (2)"),
    ]);
    expect(await ids(session)).toEqual([2]);
  });

  it("keeps a transaction's statements together when they are queued at once", async () => {
    await openEngine();
    const session = engine.connect();
    const other = engine.connect();
    const calls = [
      session.query("BEGIN"),
      session.query("INSERT INTO t (id) VALUES (1)"),
      other.query("SELECT count(*) FROM t"),
      session.query("INSERT INTO t (id) VALUES (2)"),
      session.query("COMMIT"),
    ];
    const results = await Promise.all(calls);
    expect(results[2]?.rows).toEqual([[0]]);
    expect(await ids(other)).toEqual([1, 2]);
  });
});

describe("several sessions writing at once", () => {
  it("all succeed, one after another", async () => {
    await openEngine();
    const sessions = Array.from({ length: 5 }, () => engine.connect());
    await Promise.all(
      sessions.map(async (session, index) =>
        session.query("INSERT INTO t (id) VALUES ($1)", [index + 1]),
      ),
    );
    expect(await ids(sessions[0])).toEqual([1, 2, 3, 4, 5]);
    expect(engine.checkIntegrity()).toEqual([]);
  });

  it("keep both tables when two sessions create one each", async () => {
    await openEngine();
    const [first, second] = [engine.connect(), engine.connect()];
    await Promise.all([
      first.query("CREATE TABLE a (id INTEGER PRIMARY KEY)"),
      second.query("CREATE TABLE b (id INTEGER PRIMARY KEY)"),
    ]);
    const names = await tableNamesOf(first.schema());
    expect(names).toEqual(["t", "a", "b"]);
    expect(engine.checkIntegrity()).toEqual([]);
  });

  it("see each other's schema after a rolled-back DDL", async () => {
    await openEngine();
    const [first, second] = [engine.connect(), engine.connect()];
    await first.execute("BEGIN; CREATE TABLE x (id INTEGER PRIMARY KEY)");
    await first.execute("ROLLBACK");
    await second.query("CREATE TABLE y (id INTEGER PRIMARY KEY)");
    await first.query("INSERT INTO y VALUES (1)");
    const names = await tableNamesOf(first.schema());
    expect(names).toEqual(["t", "y"]);
  });
});

describe("immediate transactions", () => {
  it("take the lock at BEGIN, so a writer waits before the first write", async () => {
    await openEngine(30);
    const holder = engine.connect();
    const writer = engine.connect();
    await holder.query("BEGIN IMMEDIATE");
    await expect(
      writer.query("INSERT INTO t (id) VALUES (1)"),
    ).rejects.toBeInstanceOf(BusyError);
    await holder.query("ROLLBACK");

    // A deferred BEGIN takes nothing until it writes.
    await holder.query("BEGIN");
    await writer.query("INSERT INTO t (id) VALUES (1)");
    await holder.query("ROLLBACK");
  });

  it("never fail to serialize", async () => {
    await openEngine();
    const reader = engine.connect();
    const writer = engine.connect();
    await reader.query("BEGIN IMMEDIATE");
    await reader.query("SELECT * FROM t");
    const waiting = writer.query("INSERT INTO t (id) VALUES (1)");
    await tick();
    await reader.query("INSERT INTO t (id) VALUES (2)");
    await reader.query("COMMIT");
    await waiting;
    expect(await ids(reader)).toEqual([1, 2]);
  });
});

describe("transaction()", () => {
  it("commits what fn did and returns its value", async () => {
    await openEngine();
    const session = engine.connect();
    const value = await session.transaction(async (tx) => {
      await tx.query("INSERT INTO t (id) VALUES (1)");
      return "done";
    });
    expect(value).toBe("done");
    expect(session.inTransaction).toBe(false);
    expect(await ids(session)).toEqual([1]);
  });

  it("rolls back and rethrows when fn throws", async () => {
    await openEngine();
    const session = engine.connect();
    const failure = new Error("no");
    await expect(
      session.transaction(async (tx) => {
        await tx.query("INSERT INTO t (id) VALUES (1)");
        throw failure;
      }),
    ).rejects.toBe(failure);
    expect(session.inTransaction).toBe(false);
    expect(await ids(session)).toEqual([]);
  });

  it("does not nest", async () => {
    await openEngine();
    const session = engine.connect();
    await expect(
      session.transaction(async () => session.transaction(() => 1)),
    ).rejects.toBeInstanceOf(MisuseError);
    expect(session.inTransaction).toBe(false);
  });
});

describe("closing while work waits", () => {
  it("a session closed while its write waits for the lock cancels it, and holds nothing", async () => {
    await openEngine(Infinity);
    const holder = engine.connect();
    const waiter = engine.connect();
    await holder.query("BEGIN IMMEDIATE");

    const waiting = expect(
      waiter.query("INSERT INTO t (id) VALUES (1)"),
    ).rejects.toBeInstanceOf(AbortError);
    await tick();
    await waiter.close();
    await waiting;
    await expect(waiter.query("SELECT 1")).rejects.toBeInstanceOf(MisuseError);

    await holder.query("COMMIT");
    const other = engine.connect();
    await other.query("INSERT INTO t (id) VALUES (2)");
    expect(await ids(other)).toEqual([2]);
  });

  it("a closed engine cancels what waits and refuses new work", async () => {
    await openEngine(Infinity);
    const holder = engine.connect();
    const waiter = engine.connect();
    await holder.query("BEGIN IMMEDIATE");
    await holder.query("INSERT INTO t (id) VALUES (1)");

    const waiting = expect(
      waiter.query("INSERT INTO t (id) VALUES (2)"),
    ).rejects.toBeInstanceOf(AbortError);
    await tick();
    await engine.close();
    await waiting;

    expect(() => engine.connect()).toThrow(MisuseError);
    await expect(holder.query("SELECT 1")).rejects.toBeInstanceOf(MisuseError);
    await expect(engine.checkpoint()).rejects.toBeInstanceOf(MisuseError);
  });

  it("an engine closed under an open transaction rolls it back", async () => {
    const name = `concurrency-${String(seq++)}`;
    engine = await Engine.open(name);
    const session = engine.connect();
    await session.execute(
      "CREATE TABLE t (id INTEGER PRIMARY KEY); BEGIN; INSERT INTO t VALUES (1)",
    );
    await engine.close();

    engine = await Engine.open(name, { busyTimeout: 0 });
    const reopened = engine.connect();
    await reopened.query("INSERT INTO t VALUES (2)");
    expect(await ids(reopened)).toEqual([2]);
  });
});

describe("abandoned sessions", () => {
  it("are closed when collected, releasing their lock and snapshot", async () => {
    await openEngine(200);
    await (async () => {
      const forgotten = engine.connect();
      await forgotten.query("BEGIN IMMEDIATE");
      await forgotten.query("INSERT INTO t (id) VALUES (1)");
    })();

    await collectGarbage();
    const session = engine.connect();
    await session.query("INSERT INTO t (id) VALUES (2)");
    expect(await ids(session)).toEqual([2]);
  });

  it("stay open while a call on them runs, held or not", async () => {
    await openEngine(Infinity);
    const holder = engine.connect();
    await holder.query("BEGIN IMMEDIATE");
    const pending = engine.connect().query("INSERT INTO t (id) VALUES (1)");

    await collectGarbage();
    await holder.query("COMMIT");
    await expect(pending).resolves.toMatchObject({ changes: 1 });
  });

  it("are closed at the end of an await using block", async () => {
    await openEngine(0);
    {
      await using session = engine.connect();
      await session.query("BEGIN IMMEDIATE");
    }
    const session = engine.connect();
    await session.query("INSERT INTO t (id) VALUES (1)");
    expect(await ids(session)).toEqual([1]);
  });
});
