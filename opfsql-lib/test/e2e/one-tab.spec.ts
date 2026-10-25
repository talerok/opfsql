import { test, expect } from "./infra/fixtures";
import { openTab, initDb, openDb, exec, closeDb, cleanOpfs } from "./infra/db";

test.describe("One tab per database", () => {
  test("data survives closing and reopening the database", async ({
    context,
  }) => {
    const tab = await openTab(context);
    await initDb(tab);
    await exec(tab, "CREATE TABLE notes (id INTEGER PRIMARY KEY, body TEXT)");
    await exec(tab, "INSERT INTO notes VALUES (1, 'first'), (2, 'second')");
    await closeDb(tab);

    await openDb(tab);
    const [result] = await exec(tab, "SELECT body FROM notes ORDER BY id");
    expect(result.rows).toEqual([{ body: "first" }, { body: "second" }]);

    await closeDb(tab);
    await cleanOpfs(tab);
  });

  test("a second tab is told the database is in use", async ({
    context,
    browserName,
  }) => {
    test.skip(
      browserName === "chromium",
      "Chromium lets several tabs open one database",
    );

    const first = await openTab(context);
    await initDb(first);

    const second = await openTab(context);
    const error = await second.evaluate(() =>
      (window as any).__opfsql.open("e2e-test").then(
        () => null,
        (err: Error) => ({ name: err.name, message: err.message }),
      ),
    );
    expect(error).toEqual({
      name: "DatabaseInUseError",
      message: expect.stringContaining("open in another tab"),
    });

    await closeDb(first);
    await cleanOpfs(first);
  });
});
