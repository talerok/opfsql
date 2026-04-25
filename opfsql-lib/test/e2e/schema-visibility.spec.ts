import { test, expect } from "@playwright/test";
import { openTab, initDb, openDb, exec, closeDb, cleanOpfs } from "./infra/db";

test.describe("Schema changes visibility across tabs", () => {
  test("CREATE TABLE visible to Tab B", async ({ context }) => {
    const tabA = await openTab(context);
    await initDb(tabA);

    const tabB = await openTab(context);
    await openDb(tabB);

    await exec(
      tabA,
      "CREATE TABLE users (id INTEGER PRIMARY KEY, name TEXT, email TEXT)",
    );
    await exec(tabA, "INSERT INTO users VALUES (1, 'Alice', 'alice@test.com')");

    const results = await exec(tabB, "SELECT * FROM users");
    expect(results[0].rows).toEqual([
      { id: 1, name: "Alice", email: "alice@test.com" },
    ]);

    await closeDb(tabB);
    await closeDb(tabA);
    await cleanOpfs(tabA);
  });

  test("CREATE INDEX visible to Tab B", async ({ context }) => {
    const tabA = await openTab(context);
    await initDb(tabA);
    await exec(
      tabA,
      "CREATE TABLE products (id INTEGER PRIMARY KEY, name TEXT, price REAL)",
    );
    await exec(tabA, "INSERT INTO products VALUES (1, 'Widget', 9.99)");

    const tabB = await openTab(context);
    await openDb(tabB);

    await exec(tabA, "CREATE INDEX idx_price ON products (price)");

    // Tab B should be able to query using the index
    const results = await exec(
      tabB,
      "SELECT name FROM products WHERE price < 10.00",
    );
    expect(results[0].rows).toEqual([{ name: "Widget" }]);

    await closeDb(tabB);
    await closeDb(tabA);
    await cleanOpfs(tabA);
  });

  test("ALTER TABLE ADD COLUMN visible to Tab B", async ({ context }) => {
    const tabA = await openTab(context);
    await initDb(tabA);
    await exec(tabA, "CREATE TABLE t (id INTEGER PRIMARY KEY, name TEXT)");
    await exec(tabA, "INSERT INTO t VALUES (1, 'Alice')");

    const tabB = await openTab(context);
    await openDb(tabB);

    await exec(tabA, "ALTER TABLE t ADD COLUMN age INTEGER DEFAULT 25");

    const results = await exec(tabB, "SELECT id, name, age FROM t");
    expect(results[0].rows).toEqual([{ id: 1, name: "Alice", age: 25 }]);

    await closeDb(tabB);
    await closeDb(tabA);
    await cleanOpfs(tabA);
  });

  test("DROP TABLE visible to Tab B", async ({ context }) => {
    const tabA = await openTab(context);
    await initDb(tabA);
    await exec(tabA, "CREATE TABLE t (id INTEGER PRIMARY KEY)");

    const tabB = await openTab(context);
    await openDb(tabB);

    await exec(tabA, "DROP TABLE t");

    // Tab B should get an error trying to query dropped table
    await expect(exec(tabB, "SELECT * FROM t")).rejects.toThrow();

    await closeDb(tabB);
    await closeDb(tabA);
    await cleanOpfs(tabA);
  });
});
