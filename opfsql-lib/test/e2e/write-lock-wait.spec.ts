import { test, expect } from "@playwright/test";
import { openTab, initDb, openDb, exec, closeDb, cleanOpfs } from "./infra/db";

test.describe("Write lock contention", () => {
  test("Tab B waits for Tab A lock, then succeeds", async ({ context }) => {
    const tabA = await openTab(context);
    await initDb(tabA);
    await exec(tabA, "CREATE TABLE t (id INTEGER PRIMARY KEY)");

    const tabB = await openTab(context);
    await openDb(tabB);

    // Tab A holds the Web Lock via open transaction
    await exec(tabA, "BEGIN");
    await exec(tabA, "INSERT INTO t VALUES (1)");

    // Tab B tries to write — blocks on Web Lock
    const tabBWrite = exec(tabB, "INSERT INTO t VALUES (2)");

    // Release Tab A lock after a delay
    await new Promise((r) => setTimeout(r, 300));
    await exec(tabA, "COMMIT");

    // Tab B should now complete
    await tabBWrite;

    const results = await exec(tabA, "SELECT * FROM t ORDER BY id");
    expect(results[0].rows).toHaveLength(2);

    await closeDb(tabB);
    await closeDb(tabA);
    await cleanOpfs(tabA);
  });
});
