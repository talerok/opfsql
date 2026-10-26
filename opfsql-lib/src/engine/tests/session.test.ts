import { resetMockOPFS } from "opfs-mock";
import { afterEach, beforeEach, describe, expect, it, vi } from "vitest";
import { MemoryFile } from "../../storage/file/sync-file.js";
import type { Engine } from "../index.js";
import { connectSession, createEngine as engineOn } from "../engine.js";
import { openTestEngine } from "./test-engine.js";

let engine: Engine;
let seq = 0;

async function createEngine(): Promise<Engine> {
  engine = await openTestEngine(`session-test-${String(seq++)}`);
  return engine;
}

beforeEach(() => {
  resetMockOPFS();
});

afterEach(async () => {
  await engine?.close();
});

// ---------------------------------------------------------------------------
// Session isolation — uncommitted writes not visible to other sessions
// ---------------------------------------------------------------------------

describe("session isolation", () => {
  it("session B does not see session A's uncommitted INSERT", async () => {
    await createEngine();
    const sA = engine.connect();
    await sA.execute("CREATE TABLE t (id INTEGER PRIMARY KEY)");
    await sA.close();

    const sB = engine.connect();
    const sC = engine.connect();

    await sC.execute("BEGIN");
    await sC.execute("INSERT INTO t VALUES (1)");

    // sB should see empty table (C hasn't committed)
    const [result] = await sB.execute("SELECT * FROM t");
    expect(result.objects()).toHaveLength(0);

    await sC.execute("ROLLBACK");
    await sB.close();
    await sC.close();
  });

  it("session B sees data after session A commits", async () => {
    await createEngine();
    const setup = engine.connect();
    await setup.execute("CREATE TABLE t (id INTEGER PRIMARY KEY)");
    await setup.close();

    const sA = engine.connect();
    await sA.execute("INSERT INTO t VALUES (1)");
    await sA.close();

    const sB = engine.connect();
    const [result] = await sB.execute("SELECT * FROM t");
    expect(result.objects()).toEqual([{ id: 1 }]);
    await sB.close();
  });

  it("DDL commit propagates catalog to new sessions", async () => {
    await createEngine();
    const sA = engine.connect();
    await sA.execute("CREATE TABLE t (id INTEGER PRIMARY KEY, name TEXT)");
    await sA.close();

    const sB = engine.connect();
    await sB.execute("INSERT INTO t VALUES (1, 'hello')");
    const [result] = await sB.execute("SELECT * FROM t");
    expect(result.objects()).toEqual([{ id: 1, name: "hello" }]);
    await sB.close();
  });

  it("already-open session sees DDL after other session commits", async () => {
    await createEngine();
    const sA = engine.connect();
    const sB = engine.connect();

    // A creates a table and commits
    await sA.execute("CREATE TABLE t (id INTEGER PRIMARY KEY)");
    await sA.close();

    // B was opened before the CREATE TABLE, but should see it now
    await sB.execute("INSERT INTO t VALUES (1)");
    const [result] = await sB.execute("SELECT * FROM t");
    expect(result.objects()).toEqual([{ id: 1 }]);
    await sB.close();
  });
});

// ---------------------------------------------------------------------------
// Write lock
// ---------------------------------------------------------------------------

describe("write lock", () => {
  it("a second writer waits for the first to commit", async () => {
    await createEngine();
    const setup = engine.connect();
    await setup.execute("CREATE TABLE t (id INTEGER PRIMARY KEY)");
    await setup.close();

    const sA = engine.connect();
    const sB = engine.connect();

    await sA.execute("BEGIN");
    await sA.execute("INSERT INTO t VALUES (1)");

    let isDone = false;
    const second = (async () => {
      await sB.execute("INSERT INTO t VALUES (2)");
      isDone = true;
    })();
    await new Promise((resolve) => setTimeout(resolve, 20));
    expect(isDone).toBe(false);

    await sA.execute("COMMIT");
    await second;
    const [result] = await sB.execute("SELECT id FROM t ORDER BY id");
    expect(result.rows).toEqual([[1], [2]]);
    await sA.close();
    await sB.close();
  });

  it("a second writer that waits past the busy timeout fails with BUSY", async () => {
    engine = await openTestEngine(`session-test-${String(seq++)}`, {
      busyTimeout: 30,
    });
    const sA = engine.connect();
    const sB = engine.connect();
    await sA.execute("CREATE TABLE t (id INTEGER PRIMARY KEY)");

    await sA.execute("BEGIN");
    await sA.execute("INSERT INTO t VALUES (1)");
    await expect(sB.execute("INSERT INTO t VALUES (2)")).rejects.toMatchObject({
      name: "BusyError",
      code: "BUSY",
      timeout: 30,
    });
    await sA.execute("COMMIT");

    await sB.execute("INSERT INTO t VALUES (2)");
    await sA.close();
    await sB.close();
  });

  it("write lock released on COMMIT, other session can write", async () => {
    await createEngine();
    const setup = engine.connect();
    await setup.execute("CREATE TABLE t (id INTEGER PRIMARY KEY)");
    await setup.close();

    const sA = engine.connect();
    const sB = engine.connect();

    await sA.execute("BEGIN");
    await sA.execute("INSERT INTO t VALUES (1)");
    await sA.execute("COMMIT");

    // Now B can write
    await sB.execute("INSERT INTO t VALUES (2)");

    const [result] = await sB.execute("SELECT * FROM t ORDER BY id");
    expect(result.objects()).toEqual([{ id: 1 }, { id: 2 }]);

    await sA.close();
    await sB.close();
  });

  it("write lock released on ROLLBACK", async () => {
    await createEngine();
    const setup = engine.connect();
    await setup.execute("CREATE TABLE t (id INTEGER PRIMARY KEY)");
    await setup.close();

    const sA = engine.connect();
    const sB = engine.connect();

    await sA.execute("BEGIN");
    await sA.execute("INSERT INTO t VALUES (1)");
    await sA.execute("ROLLBACK");

    // B can write after A rolled back
    await sB.execute("INSERT INTO t VALUES (2)");
    const [result] = await sB.execute("SELECT * FROM t");
    expect(result.objects()).toEqual([{ id: 2 }]);

    await sA.close();
    await sB.close();
  });

  it("write lock released on session close", async () => {
    await createEngine();
    const setup = engine.connect();
    await setup.execute("CREATE TABLE t (id INTEGER PRIMARY KEY)");
    await setup.close();

    const sA = engine.connect();
    const sB = engine.connect();

    await sA.execute("BEGIN");
    await sA.execute("INSERT INTO t VALUES (1)");
    await sA.close(); // close without commit → rollback + release lock

    await sB.execute("INSERT INTO t VALUES (2)");
    const [result] = await sB.execute("SELECT * FROM t");
    expect(result.objects()).toEqual([{ id: 2 }]);

    await sB.close();
  });

  it("autocommit acquires and releases lock within single execute", async () => {
    await createEngine();
    const setup = engine.connect();
    await setup.execute("CREATE TABLE t (id INTEGER PRIMARY KEY)");
    await setup.close();

    const sA = engine.connect();
    const sB = engine.connect();

    // Autocommit: lock acquired and released within execute()
    await sA.execute("INSERT INTO t VALUES (1)");

    // B should be able to write immediately after
    await sB.execute("INSERT INTO t VALUES (2)");

    const [result] = await sB.execute("SELECT * FROM t ORDER BY id");
    expect(result.objects()).toEqual([{ id: 1 }, { id: 2 }]);

    await sA.close();
    await sB.close();
  });

  it("readers are not blocked by writer", async () => {
    await createEngine();
    const setup = engine.connect();
    await setup.execute("CREATE TABLE t (id INTEGER PRIMARY KEY)");
    await setup.execute("INSERT INTO t VALUES (1)");
    await setup.close();

    const writer = engine.connect();
    const reader = engine.connect();

    await writer.execute("BEGIN");
    await writer.execute("INSERT INTO t VALUES (2)");

    // Reader can still SELECT (sees only committed data)
    const [result] = await reader.execute("SELECT * FROM t");
    expect(result.objects()).toEqual([{ id: 1 }]);

    await writer.execute("COMMIT");
    await writer.close();
    await reader.close();
  });
});

// ---------------------------------------------------------------------------
// Failed commit — nothing of the transaction survives
// ---------------------------------------------------------------------------

describe("failed commit", () => {
  /** Opens an engine whose next commit fails the way a storage write would. */
  async function createEngineWithFailingCommit(): Promise<() => void> {
    const wal = new MemoryFile();
    engine = await engineOn({ database: new MemoryFile(), wal, marks: null });
    return () => {
      vi.spyOn(wal, "write").mockImplementationOnce(() => {
        throw new Error("injected commit failure");
      });
    };
  }

  it("a failed COMMIT rolls back the whole transaction", async () => {
    const failNextCommit = await createEngineWithFailingCommit();
    const s = engine.connect();
    await s.execute("CREATE TABLE t (id INTEGER PRIMARY KEY, body TEXT)");

    await s.execute("BEGIN");
    await s.execute("INSERT INTO t VALUES (1, 'a')");
    await s.execute("INSERT INTO t VALUES (2, 'b')");
    failNextCommit();
    await expect(s.execute("COMMIT")).rejects.toMatchObject({
      code: "IO_ERROR",
      cause: new Error("injected commit failure"),
    });

    const [empty] = await s.execute("SELECT * FROM t");
    expect(empty.objects()).toEqual([]);
    // The session is out of the transaction and usable again.
    await s.execute("INSERT INTO t VALUES (3, 'c')");
    const [result] = await s.execute("SELECT * FROM t");
    expect(result.objects()).toEqual([{ id: 3, body: "c" }]);
    expect(await engine.checkIntegrity()).toEqual([]);
    await s.close();
  });

  it("a commit the browser refuses for want of space fails with STORAGE_FULL", async () => {
    const wal = new MemoryFile();
    engine = await engineOn({ database: new MemoryFile(), wal, marks: null });
    const s = engine.connect();
    await s.execute("CREATE TABLE t (id INTEGER PRIMARY KEY)");
    vi.spyOn(wal, "write").mockImplementationOnce(() => {
      throw new DOMException("quota", "QuotaExceededError");
    });

    await expect(s.execute("INSERT INTO t VALUES (1)")).rejects.toMatchObject({
      code: "STORAGE_FULL",
    });
    await s.execute("INSERT INTO t VALUES (2)");
    const [result] = await s.execute("SELECT id FROM t");
    expect(result.rows).toEqual([[2]]);
    await s.close();
  });

  it("schema from a failed commit is never published", async () => {
    const failNextCommit = await createEngineWithFailingCommit();
    const s = engine.connect();
    await s.execute("CREATE TABLE t (id INTEGER PRIMARY KEY)");

    await s.execute("BEGIN");
    await s.execute("CREATE TABLE u (id INTEGER PRIMARY KEY, body TEXT)");
    await s.execute("INSERT INTO u VALUES (1, 'a')");
    failNextCommit();
    await expect(s.execute("COMMIT")).rejects.toMatchObject({
      code: "IO_ERROR",
      cause: new Error("injected commit failure"),
    });

    const other = engine.connect();
    const schema = await other.schema();
    expect(schema.tables.map((table) => table.name)).toEqual(["t"]);
    await expect(other.execute("SELECT * FROM u")).rejects.toThrow();
    expect(await engine.checkIntegrity()).toEqual([]);
    await other.close();
    await s.close();
  });
});

// ---------------------------------------------------------------------------
// Shared sessions — no call leaves a transaction behind
// ---------------------------------------------------------------------------

/** Another session takes the write lock at once: none is left held. */
async function expectLockFree(): Promise<void> {
  const other = engine.connect();
  await other.begin({ mode: "immediate" });
  await other.rollback();
  await other.close();
}

describe("a session shared by unrelated callers", () => {
  it("ends a transaction a call left open, and fails the call with MISUSE", async () => {
    engine = await engineOn(
      { database: new MemoryFile(), wal: new MemoryFile(), marks: null },
      { busyTimeout: 0 },
    );
    const shared = connectSession(engine, { callScopedTransactions: true });
    await expect(shared.query("BEGIN IMMEDIATE")).rejects.toMatchObject({
      code: "MISUSE",
    });
    expect(shared.inTransaction).toBe(false);
    await expectLockFree();
    await shared.close();
  });

  it("ends the transaction of a call that fails with it open, keeping the call's own error", async () => {
    engine = await engineOn(
      { database: new MemoryFile(), wal: new MemoryFile(), marks: null },
      { busyTimeout: 0 },
    );
    const shared = connectSession(engine, { callScopedTransactions: true });
    await shared.query("CREATE TABLE t (id INTEGER PRIMARY KEY)");
    await expect(
      shared.execute(
        "BEGIN IMMEDIATE; INSERT INTO t VALUES (1); SELECT * FROM nope",
      ),
    ).rejects.toMatchObject({ code: "SEMANTIC_ERROR", statementIndex: 2 });
    expect(shared.inTransaction).toBe(false);
    await expectLockFree();
    const [rows] = await shared.execute("SELECT count(*) FROM t");
    expect(rows.rows).toEqual([[0]]);
    await shared.close();
  });
});

describe("the write lock", () => {
  it("reports a lock request the browser fails as IO_ERROR, and then works", async () => {
    engine = await engineOn({
      database: new MemoryFile(),
      wal: new MemoryFile(),
      marks: null,
    });
    const s = engine.connect();
    await s.query("CREATE TABLE t (id INTEGER PRIMARY KEY)");
    vi.spyOn(navigator.locks, "request").mockRejectedValueOnce(
      new DOMException("locks are unavailable", "InvalidStateError"),
    );
    await expect(s.query("INSERT INTO t VALUES (1)")).rejects.toMatchObject({
      code: "IO_ERROR",
      cause: { name: "InvalidStateError" },
    });
    await s.query("INSERT INTO t VALUES (2)");
    const [rows] = await s.execute("SELECT id FROM t");
    expect(rows.rows).toEqual([[2]]);
    await s.close();
  });
});
