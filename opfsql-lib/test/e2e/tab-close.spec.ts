import { test, expect } from "./infra/fixtures";
import {
  openTab,
  initDatabase,
  openDatabase,
  exec,
  closeDatabase,
  cleanOpfs,
} from "./infra/database";

test.describe("Tab resilience after close", { tag: "@multi-tab" }, () => {
  test("Tab B continues after Tab A closes", async ({ context }) => {
    const tabA = await openTab(context);
    await initDatabase(tabA);
    await exec(tabA, "CREATE TABLE t (id INTEGER PRIMARY KEY, val TEXT)");
    await exec(tabA, "INSERT INTO t VALUES (1, 'from-A')");

    const tabB = await openTab(context);
    await openDatabase(tabB);

    let results = await exec(tabB, "SELECT * FROM t");
    expect(results[0].objects).toHaveLength(1);

    // Tab A closes gracefully
    await closeDatabase(tabA);
    await tabA.close();

    // Tab B should still work
    await exec(tabB, "INSERT INTO t VALUES (2, 'from-B')");
    results = await exec(tabB, "SELECT * FROM t ORDER BY id");
    expect(results[0].objects).toEqual([
      { id: 1, val: "from-A" },
      { id: 2, val: "from-B" },
    ]);

    await closeDatabase(tabB);
    const cleanup = await openTab(context);
    await cleanOpfs(cleanup);
    await cleanup.close();
  });
});
