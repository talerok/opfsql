import { test, expect } from "./infra/fixtures";
import {
  openTab,
  initDatabase,
  openDatabase,
  exec,
  closeDatabase,
  cleanOpfs,
} from "./infra/database";

test.describe("One tab per database", () => {
  test("data survives closing and reopening the database", async ({
    context,
  }) => {
    const tab = await openTab(context);
    await initDatabase(tab);
    await exec(tab, "CREATE TABLE notes (id INTEGER PRIMARY KEY, body TEXT)");
    await exec(tab, "INSERT INTO notes VALUES (1, 'first'), (2, 'second')");
    await closeDatabase(tab);

    await openDatabase(tab);
    const [result] = await exec(tab, "SELECT body FROM notes ORDER BY id");
    expect(result.objects).toEqual([{ body: "first" }, { body: "second" }]);

    await closeDatabase(tab);
    await cleanOpfs(tab);
  });

  // Only where one tab may hold a database: the config leaves Chromium out.
  test(
    "a second tab is told the database is in use",
    { tag: "@one-tab-only" },
    async ({ context }) => {
      const first = await openTab(context);
      await initDatabase(first);

      const second = await openTab(context);
      const outcome = await second.evaluate(async () =>
        globalThis.opfsql.attemptOpen("e2e-test"),
      );
      expect(outcome).toMatchObject({
        ok: false,
        error: {
          name: "DatabaseInUseError",
          code: "DATABASE_IN_USE",
          isItsClass: true,
          message: expect.stringContaining("open in another tab"),
          fields: { database: "e2e-test" },
        },
      });

      await closeDatabase(first);
      await cleanOpfs(first);
    },
  );
});
