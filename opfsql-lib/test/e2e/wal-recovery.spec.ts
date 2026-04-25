import { test, expect } from "@playwright/test";
import {
  openTab,
  initDb,
  openDb,
  exec,
  closeDb,
  crashTab,
  cleanOpfs,
} from "./infra/db";

test.describe("WAL recovery after tab crash", () => {
  test("Committed data survives crash", async ({ context }) => {
    const tabA = await openTab(context);
    await initDb(tabA);
    await exec(tabA, "CREATE TABLE t (id INTEGER PRIMARY KEY, val TEXT)");
    await exec(tabA, "INSERT INTO t VALUES (1, 'committed')");
    await exec(tabA, "INSERT INTO t VALUES (2, 'also-committed')");

    // Crash Tab A — kills worker without graceful shutdown
    await crashTab(tabA);

    // Tab B opens fresh — should recover from WAL
    const tabB = await openTab(context);
    await openDb(tabB);

    const results = await exec(tabB, "SELECT * FROM t ORDER BY id");
    expect(results[0].rows).toEqual([
      { id: 1, val: "committed" },
      { id: 2, val: "also-committed" },
    ]);

    await closeDb(tabB);
    await cleanOpfs(tabB);
  });

  test("Uncommitted transaction data does NOT survive crash", async ({
    context,
  }) => {
    const tabA = await openTab(context);
    await initDb(tabA);
    await exec(tabA, "CREATE TABLE t (id INTEGER PRIMARY KEY, val TEXT)");
    await exec(tabA, "INSERT INTO t VALUES (1, 'committed')");

    // Start transaction but do not commit
    await exec(tabA, "BEGIN");
    await exec(tabA, "INSERT INTO t VALUES (2, 'uncommitted')");

    // Crash without commit
    await crashTab(tabA);

    // New tab should only see committed data
    const tabB = await openTab(context);
    await openDb(tabB);

    const results = await exec(tabB, "SELECT * FROM t ORDER BY id");
    expect(results[0].rows).toEqual([{ id: 1, val: "committed" }]);

    await closeDb(tabB);
    await cleanOpfs(tabB);
  });

  test("Write lock released after crash — Tab B can write", async ({
    context,
  }) => {
    const tabA = await openTab(context);
    await initDb(tabA);
    await exec(tabA, "CREATE TABLE t (id INTEGER PRIMARY KEY, val TEXT)");
    await exec(tabA, "INSERT INTO t VALUES (1, 'committed')");

    // Tab A holds write lock via open transaction
    await exec(tabA, "BEGIN");
    await exec(tabA, "INSERT INTO t VALUES (2, 'uncommitted')");

    // Crash Tab A — Web Lock should be released when worker dies
    await crashTab(tabA);

    // Tab B should be able to open AND write (not blocked by dead lock)
    const tabB = await openTab(context);
    await openDb(tabB);

    await exec(tabB, "INSERT INTO t VALUES (3, 'from-B')");

    const results = await exec(tabB, "SELECT * FROM t ORDER BY id");
    expect(results[0].rows).toEqual([
      { id: 1, val: "committed" },
      { id: 3, val: "from-B" },
    ]);

    await closeDb(tabB);
    await cleanOpfs(tabB);
  });
});
