import { test, expect } from "@playwright/test";
import { openTab, initDb, openDb, exec, closeDb, cleanOpfs } from "./infra/db";

test.describe("Checkpoint between tabs", () => {
  test("Tab A reopens (WAL replay), Tab B sees all data", async ({
    context,
  }) => {
    const tabA = await openTab(context);
    await initDb(tabA);
    await exec(tabA, "CREATE TABLE t (id INTEGER PRIMARY KEY, val INTEGER)");

    for (let i = 1; i <= 10; i++) {
      await exec(tabA, `INSERT INTO t VALUES (${i}, ${i * 10})`);
    }

    const tabB = await openTab(context);
    await openDb(tabB);

    let results = await exec(tabB, "SELECT COUNT(*) AS cnt FROM t");
    expect(results[0].rows[0].cnt).toBe(10);

    // Close and reopen Tab A — WAL gets replayed on reopen (epoch change)
    await closeDb(tabA);
    await openDb(tabA);

    // Tab A writes more data after reopen
    await exec(tabA, "INSERT INTO t VALUES (11, 110)");

    // Tab B should see all data after catchUp handles epoch change
    results = await exec(tabB, "SELECT COUNT(*) AS cnt FROM t");
    expect(results[0].rows[0].cnt).toBe(11);

    await closeDb(tabB);
    await closeDb(tabA);
    await cleanOpfs(tabA);
  });
});
