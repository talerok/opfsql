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

test.describe("Uncommitted data not visible", () => {
  test("BEGIN without COMMIT — other tab does not see data", async ({
    context,
  }) => {
    const tabA = await openTab(context);
    await initDb(tabA);
    await exec(tabA, "CREATE TABLE t (id INTEGER PRIMARY KEY, val TEXT)");
    await exec(tabA, "INSERT INTO t VALUES (1, 'committed')");

    // Tab A starts transaction but does not commit
    await exec(tabA, "BEGIN");
    await exec(tabA, "INSERT INTO t VALUES (2, 'will-be-lost')");
    await exec(tabA, "INSERT INTO t VALUES (3, 'also-lost')");

    // Tab B should NOT see uncommitted rows
    const tabB = await openTab(context);
    await openDb(tabB);

    const results = await exec(tabB, "SELECT * FROM t ORDER BY id");
    expect(results[0].rows).toEqual([{ id: 1, val: "committed" }]);

    await closeDb(tabB);
    await closeDb(tabA);
    await cleanOpfs(tabA);
  });

  test("Crash with open transaction — uncommitted data lost", async ({
    context,
  }) => {
    const tabA = await openTab(context);
    await initDb(tabA);
    await exec(tabA, "CREATE TABLE t (id INTEGER PRIMARY KEY, val TEXT)");
    await exec(tabA, "INSERT INTO t VALUES (1, 'committed')");

    await exec(tabA, "BEGIN");
    await exec(tabA, "INSERT INTO t VALUES (2, 'uncommitted')");

    // Crash without commit/rollback
    await crashTab(tabA);

    // New tab should only see committed data
    const tabB = await openTab(context);
    await openDb(tabB);

    const results = await exec(tabB, "SELECT * FROM t ORDER BY id");
    expect(results[0].rows).toEqual([{ id: 1, val: "committed" }]);

    await closeDb(tabB);
    await cleanOpfs(tabB);
  });

  test("ROLLBACK discards data", async ({ context }) => {
    const tabA = await openTab(context);
    await initDb(tabA);
    await exec(tabA, "CREATE TABLE t (id INTEGER PRIMARY KEY)");
    await exec(tabA, "INSERT INTO t VALUES (1)");

    await exec(tabA, "BEGIN");
    await exec(tabA, "INSERT INTO t VALUES (2)");
    await exec(tabA, "ROLLBACK");

    const tabB = await openTab(context);
    await openDb(tabB);

    const results = await exec(tabB, "SELECT * FROM t");
    expect(results[0].rows).toHaveLength(1);

    await closeDb(tabB);
    await closeDb(tabA);
    await cleanOpfs(tabA);
  });
});
