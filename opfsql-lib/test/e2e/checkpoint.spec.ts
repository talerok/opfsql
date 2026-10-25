import { test, expect } from "./infra/fixtures";
import {
  openTab,
  initDatabase,
  openDatabase,
  exec,
  closeDatabase,
  cleanOpfs,
} from "./infra/database";

test.describe("Checkpoint between tabs", { tag: "@multi-tab" }, () => {
  test("Tab A reopens (WAL replay), Tab B sees all data", async ({
    context,
  }) => {
    const tabA = await openTab(context);
    await initDatabase(tabA);
    await exec(tabA, "CREATE TABLE t (id INTEGER PRIMARY KEY, val INTEGER)");

    for (let i = 1; i <= 10; i++) {
      await exec(tabA, `INSERT INTO t VALUES (${i}, ${i * 10})`);
    }

    const tabB = await openTab(context);
    await openDatabase(tabB);

    let results = await exec(tabB, "SELECT COUNT(*) AS cnt FROM t");
    expect(results[0].rows).toEqual([[10]]);

    // Close and reopen Tab A — WAL gets replayed on reopen (epoch change)
    await closeDatabase(tabA);
    await openDatabase(tabA);

    // Tab A writes more data after reopen
    await exec(tabA, "INSERT INTO t VALUES (11, 110)");

    // Tab B should see all data after catchUp handles epoch change
    results = await exec(tabB, "SELECT COUNT(*) AS cnt FROM t");
    expect(results[0].rows).toEqual([[11]]);

    await closeDatabase(tabB);
    await closeDatabase(tabA);
    await cleanOpfs(tabA);
  });
});
