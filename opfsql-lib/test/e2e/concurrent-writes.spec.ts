import { test, expect } from "@playwright/test";
import { openTab, initDb, openDb, exec, closeDb, cleanOpfs } from "./infra/db";

test.describe("Concurrent writes", () => {
  test("Both tabs write autocommit — all data preserved", async ({
    context,
  }) => {
    const tabA = await openTab(context);
    await initDb(tabA);
    await exec(tabA, "CREATE TABLE nums (id INTEGER PRIMARY KEY, source TEXT)");

    const tabB = await openTab(context);
    await openDb(tabB);

    // Both tabs write concurrently (autocommit)
    await Promise.all([
      exec(tabA, "INSERT INTO nums VALUES (1, 'A')"),
      exec(tabB, "INSERT INTO nums VALUES (2, 'B')"),
    ]);

    // More writes, serialized
    await exec(tabA, "INSERT INTO nums VALUES (3, 'A')");
    await exec(tabB, "INSERT INTO nums VALUES (4, 'B')");

    const expected = [
      { id: 1, source: "A" },
      { id: 2, source: "B" },
      { id: 3, source: "A" },
      { id: 4, source: "B" },
    ];

    const resultsA = await exec(tabA, "SELECT * FROM nums ORDER BY id");
    const resultsB = await exec(tabB, "SELECT * FROM nums ORDER BY id");

    expect(resultsA[0].rows).toEqual(expected);
    expect(resultsB[0].rows).toEqual(expected);

    await closeDb(tabB);
    await closeDb(tabA);
    await cleanOpfs(tabA);
  });

  test("Sequential transactions both commit", async ({ context }) => {
    const tabA = await openTab(context);
    await initDb(tabA);
    await exec(tabA, "CREATE TABLE kv (k TEXT PRIMARY KEY, v INTEGER)");

    const tabB = await openTab(context);
    await openDb(tabB);

    await exec(tabA, "BEGIN");
    await exec(tabA, "INSERT INTO kv VALUES ('a', 1)");
    await exec(tabA, "COMMIT");

    await exec(tabB, "BEGIN");
    await exec(tabB, "INSERT INTO kv VALUES ('b', 2)");
    await exec(tabB, "COMMIT");

    const results = await exec(tabA, "SELECT * FROM kv ORDER BY k");
    expect(results[0].rows).toEqual([
      { k: "a", v: 1 },
      { k: "b", v: 2 },
    ]);

    await closeDb(tabB);
    await closeDb(tabA);
    await cleanOpfs(tabA);
  });
});
