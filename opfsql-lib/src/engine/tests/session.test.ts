import { resetMockOPFS } from "opfs-mock";
import { afterEach, beforeEach, describe, expect, it, vi } from "vitest";
import { OPFSSyncStorage } from "../../store/backend/opfs-storage.js";
import { Engine } from "../index.js";
import { openTestEngine } from "./test-engine.js";

let engine: Engine;
let seq = 0;

async function createEngine(): Promise<Engine> {
  engine = await openTestEngine(`session-test-${seq++}`);
  return engine;
}

beforeEach(() => {
  resetMockOPFS();
});

afterEach(() => {
  engine?.close();
});

// ---------------------------------------------------------------------------
// Session isolation — uncommitted writes not visible to other sessions
// ---------------------------------------------------------------------------

describe("session isolation", () => {
  it("session B does not see session A's uncommitted INSERT", async () => {
    await createEngine();
    const sA = engine.createSession();
    await sA.execute("CREATE TABLE t (id INTEGER PRIMARY KEY)");
    sA.close();

    const sB = engine.createSession();
    const sC = engine.createSession();

    await sC.execute("BEGIN");
    await sC.execute("INSERT INTO t VALUES (1)");

    // sB should see empty table (C hasn't committed)
    const [result] = await sB.execute("SELECT * FROM t");
    expect(result.rows).toHaveLength(0);

    await sC.execute("ROLLBACK");
    sB.close();
    sC.close();
  });

  it("session B sees data after session A commits", async () => {
    await createEngine();
    const setup = engine.createSession();
    await setup.execute("CREATE TABLE t (id INTEGER PRIMARY KEY)");
    setup.close();

    const sA = engine.createSession();
    await sA.execute("INSERT INTO t VALUES (1)");
    sA.close();

    const sB = engine.createSession();
    const [result] = await sB.execute("SELECT * FROM t");
    expect(result.rows).toEqual([{ id: 1 }]);
    sB.close();
  });

  it("DDL commit propagates catalog to new sessions", async () => {
    await createEngine();
    const sA = engine.createSession();
    await sA.execute("CREATE TABLE t (id INTEGER PRIMARY KEY, name TEXT)");
    sA.close();

    const sB = engine.createSession();
    await sB.execute("INSERT INTO t VALUES (1, 'hello')");
    const [result] = await sB.execute("SELECT * FROM t");
    expect(result.rows).toEqual([{ id: 1, name: "hello" }]);
    sB.close();
  });

  it("already-open session sees DDL after other session commits", async () => {
    await createEngine();
    const sA = engine.createSession();
    const sB = engine.createSession();

    // A creates a table and commits
    await sA.execute("CREATE TABLE t (id INTEGER PRIMARY KEY)");
    sA.close();

    // B was opened before the CREATE TABLE, but should see it now
    await sB.execute("INSERT INTO t VALUES (1)");
    const [result] = await sB.execute("SELECT * FROM t");
    expect(result.rows).toEqual([{ id: 1 }]);
    sB.close();
  });
});

// ---------------------------------------------------------------------------
// Write lock
// ---------------------------------------------------------------------------

describe("write lock", () => {
  it("second writer throws 'database is locked'", async () => {
    await createEngine();
    const setup = engine.createSession();
    await setup.execute("CREATE TABLE t (id INTEGER PRIMARY KEY)");
    setup.close();

    const sA = engine.createSession();
    const sB = engine.createSession();

    await sA.execute("BEGIN");
    await sA.execute("INSERT INTO t VALUES (1)");

    await expect(sB.execute("INSERT INTO t VALUES (2)")).rejects.toThrow(
      "database is locked",
    );

    await sA.execute("COMMIT");
    sA.close();
    sB.close();
  });

  it("write lock released on COMMIT, other session can write", async () => {
    await createEngine();
    const setup = engine.createSession();
    await setup.execute("CREATE TABLE t (id INTEGER PRIMARY KEY)");
    setup.close();

    const sA = engine.createSession();
    const sB = engine.createSession();

    await sA.execute("BEGIN");
    await sA.execute("INSERT INTO t VALUES (1)");
    await sA.execute("COMMIT");

    // Now B can write
    await sB.execute("INSERT INTO t VALUES (2)");

    const [result] = await sB.execute("SELECT * FROM t ORDER BY id");
    expect(result.rows).toEqual([{ id: 1 }, { id: 2 }]);

    sA.close();
    sB.close();
  });

  it("write lock released on ROLLBACK", async () => {
    await createEngine();
    const setup = engine.createSession();
    await setup.execute("CREATE TABLE t (id INTEGER PRIMARY KEY)");
    setup.close();

    const sA = engine.createSession();
    const sB = engine.createSession();

    await sA.execute("BEGIN");
    await sA.execute("INSERT INTO t VALUES (1)");
    await sA.execute("ROLLBACK");

    // B can write after A rolled back
    await sB.execute("INSERT INTO t VALUES (2)");
    const [result] = await sB.execute("SELECT * FROM t");
    expect(result.rows).toEqual([{ id: 2 }]);

    sA.close();
    sB.close();
  });

  it("write lock released on session close", async () => {
    await createEngine();
    const setup = engine.createSession();
    await setup.execute("CREATE TABLE t (id INTEGER PRIMARY KEY)");
    setup.close();

    const sA = engine.createSession();
    const sB = engine.createSession();

    await sA.execute("BEGIN");
    await sA.execute("INSERT INTO t VALUES (1)");
    sA.close(); // close without commit → rollback + release lock

    await sB.execute("INSERT INTO t VALUES (2)");
    const [result] = await sB.execute("SELECT * FROM t");
    expect(result.rows).toEqual([{ id: 2 }]);

    sB.close();
  });

  it("autocommit acquires and releases lock within single execute", async () => {
    await createEngine();
    const setup = engine.createSession();
    await setup.execute("CREATE TABLE t (id INTEGER PRIMARY KEY)");
    setup.close();

    const sA = engine.createSession();
    const sB = engine.createSession();

    // Autocommit: lock acquired and released within execute()
    await sA.execute("INSERT INTO t VALUES (1)");

    // B should be able to write immediately after
    await sB.execute("INSERT INTO t VALUES (2)");

    const [result] = await sB.execute("SELECT * FROM t ORDER BY id");
    expect(result.rows).toEqual([{ id: 1 }, { id: 2 }]);

    sA.close();
    sB.close();
  });

  it("readers are not blocked by writer", async () => {
    await createEngine();
    const setup = engine.createSession();
    await setup.execute("CREATE TABLE t (id INTEGER PRIMARY KEY)");
    await setup.execute("INSERT INTO t VALUES (1)");
    setup.close();

    const writer = engine.createSession();
    const reader = engine.createSession();

    await writer.execute("BEGIN");
    await writer.execute("INSERT INTO t VALUES (2)");

    // Reader can still SELECT (sees only committed data)
    const [result] = await reader.execute("SELECT * FROM t");
    expect(result.rows).toEqual([{ id: 1 }]);

    await writer.execute("COMMIT");
    writer.close();
    reader.close();
  });
});

// ---------------------------------------------------------------------------
// Failed commit — nothing of the transaction survives
// ---------------------------------------------------------------------------

describe("failed commit", () => {
  /** Opens an engine whose next commit fails the way a storage write would. */
  async function createEngineWithFailingCommit(): Promise<() => void> {
    const backend = new OPFSSyncStorage(`session-test-${seq++}`);
    engine = await Engine.create(backend);
    return () => {
      vi.spyOn(backend, "commit").mockImplementationOnce(() => {
        throw new Error("injected commit failure");
      });
    };
  }

  it("a failed COMMIT rolls back the whole transaction", async () => {
    const failNextCommit = await createEngineWithFailingCommit();
    const s = engine.createSession();
    await s.execute("CREATE TABLE t (id INTEGER PRIMARY KEY, body TEXT)");

    await s.execute("BEGIN");
    await s.execute("INSERT INTO t VALUES (1, 'a')");
    await s.execute("INSERT INTO t VALUES (2, 'b')");
    failNextCommit();
    await expect(s.execute("COMMIT")).rejects.toThrow(
      "injected commit failure",
    );

    const [empty] = await s.execute("SELECT * FROM t");
    expect(empty.rows).toEqual([]);
    // The session is out of the transaction and usable again.
    await s.execute("INSERT INTO t VALUES (3, 'c')");
    const [result] = await s.execute("SELECT * FROM t");
    expect(result.rows).toEqual([{ id: 3, body: "c" }]);
    expect(engine.checkIntegrity()).toEqual([]);
    s.close();
  });

  it("schema from a failed commit is never published", async () => {
    const failNextCommit = await createEngineWithFailingCommit();
    const s = engine.createSession();
    await s.execute("CREATE TABLE t (id INTEGER PRIMARY KEY)");

    await s.execute("BEGIN");
    await s.execute("CREATE TABLE u (id INTEGER PRIMARY KEY, body TEXT)");
    await s.execute("INSERT INTO u VALUES (1, 'a')");
    failNextCommit();
    await expect(s.execute("COMMIT")).rejects.toThrow(
      "injected commit failure",
    );

    const other = engine.createSession();
    expect(other.getSchema().tables.map((t) => t.name)).toEqual(["t"]);
    await expect(other.execute("SELECT * FROM u")).rejects.toThrow();
    expect(engine.checkIntegrity()).toEqual([]);
    other.close();
    s.close();
  });
});
