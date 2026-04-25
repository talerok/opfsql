import { test, expect } from "@playwright/test";
import { openTab, initDb, openDb, exec, closeDb, cleanOpfs } from "./infra/db";

test.describe("Cross-tab read visibility", () => {
  test("Tab A writes → Tab B sees data", async ({ context }) => {
    const tabA = await openTab(context);
    await initDb(tabA);

    await exec(tabA, "CREATE TABLE users (id INTEGER PRIMARY KEY, name TEXT)");
    await exec(tabA, "INSERT INTO users VALUES (1, 'Alice')");
    await exec(tabA, "INSERT INTO users VALUES (2, 'Bob')");

    const tabB = await openTab(context);
    await openDb(tabB);

    const results = await exec(tabB, "SELECT * FROM users ORDER BY id");
    expect(results[0].rows).toEqual([
      { id: 1, name: "Alice" },
      { id: 2, name: "Bob" },
    ]);

    await closeDb(tabB);
    await closeDb(tabA);
    await cleanOpfs(tabA);
  });

  test("Tab B already open → Tab A writes → Tab B sees via catchUp", async ({
    context,
  }) => {
    const tabA = await openTab(context);
    await initDb(tabA);
    await exec(tabA, "CREATE TABLE items (id INTEGER PRIMARY KEY, val TEXT)");

    const tabB = await openTab(context);
    await openDb(tabB);

    // Tab A writes AFTER Tab B is connected
    await exec(tabA, "INSERT INTO items VALUES (1, 'hello')");

    // Tab B reads — catchUp triggers on execute
    const results = await exec(tabB, "SELECT * FROM items");
    expect(results[0].rows).toEqual([{ id: 1, val: "hello" }]);

    await closeDb(tabB);
    await closeDb(tabA);
    await cleanOpfs(tabA);
  });

  test("Snapshot isolation — transaction does not see external writes", async ({
    context,
  }) => {
    const tabA = await openTab(context);
    await initDb(tabA);
    await exec(tabA, "CREATE TABLE t (id INTEGER PRIMARY KEY, val TEXT)");
    await exec(tabA, "INSERT INTO t VALUES (1, 'initial')");

    const tabB = await openTab(context);
    await openDb(tabB);

    // Tab B starts a read transaction
    await exec(tabB, "BEGIN");
    const before = await exec(tabB, "SELECT * FROM t");
    expect(before[0].rows).toHaveLength(1);

    // Tab A writes while Tab B is in transaction
    await exec(tabA, "INSERT INTO t VALUES (2, 'new')");

    // Tab B should NOT see Tab A's write (catchUp skipped inside transaction)
    const during = await exec(tabB, "SELECT * FROM t");
    expect(during[0].rows).toHaveLength(1);

    // After commit, Tab B sees the new data
    await exec(tabB, "COMMIT");
    const after = await exec(tabB, "SELECT * FROM t ORDER BY id");
    expect(after[0].rows).toHaveLength(2);

    await closeDb(tabB);
    await closeDb(tabA);
    await cleanOpfs(tabA);
  });
});
