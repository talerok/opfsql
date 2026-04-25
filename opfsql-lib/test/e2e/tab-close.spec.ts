import { test, expect } from "@playwright/test";
import { openTab, initDb, openDb, exec, closeDb, cleanOpfs } from "./infra/db";

test.describe("Tab resilience after close", () => {
  test("Tab B continues after Tab A closes", async ({ context }) => {
    const tabA = await openTab(context);
    await initDb(tabA);
    await exec(tabA, "CREATE TABLE t (id INTEGER PRIMARY KEY, val TEXT)");
    await exec(tabA, "INSERT INTO t VALUES (1, 'from-A')");

    const tabB = await openTab(context);
    await openDb(tabB);

    let results = await exec(tabB, "SELECT * FROM t");
    expect(results[0].rows).toHaveLength(1);

    // Tab A closes gracefully
    await closeDb(tabA);
    await tabA.close();

    // Tab B should still work
    await exec(tabB, "INSERT INTO t VALUES (2, 'from-B')");
    results = await exec(tabB, "SELECT * FROM t ORDER BY id");
    expect(results[0].rows).toEqual([
      { id: 1, val: "from-A" },
      { id: 2, val: "from-B" },
    ]);

    await closeDb(tabB);
    const cleanup = await openTab(context);
    await cleanOpfs(cleanup);
    await cleanup.close();
  });
});
