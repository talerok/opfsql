import {
  attempt,
  cleanOpfs,
  closeDatabase,
  exec,
  initDatabase,
  openTab,
} from "./infra/database";
import { expect, test } from "./infra/fixtures";

// The client API in a real browser, through a real worker.

test.describe("Results and errors through the worker", () => {
  test("results keep every column and report what they changed", async ({
    context,
  }) => {
    const tab = await openTab(context);
    await initDatabase(tab);
    await exec(tab, "CREATE TABLE t (id INTEGER PRIMARY KEY, v TEXT)");

    const [insert] = await exec(tab, "INSERT INTO t (v) VALUES ($1), ($2)", [
      "a",
      "b",
    ]);
    expect(insert).toMatchObject({ changes: 2, lastInsertRowid: 2 });

    const [joined] = await exec(
      tab,
      "SELECT x.id, y.id FROM t x JOIN t y ON y.id = x.id ORDER BY x.id",
    );
    expect(joined).toMatchObject({
      columns: ["id", "id"],
      rows: [
        [1, 1],
        [2, 2],
      ],
      objects: null,
    });

    const [empty] = await exec(tab, "SELECT id, v FROM t WHERE id > 5");
    expect(empty).toMatchObject({ columns: ["id", "v"], rows: [] });

    await closeDatabase(tab);
    await cleanOpfs(tab);
  });

  test("errors arrive as their own classes, with their fields", async ({
    context,
  }) => {
    const tab = await openTab(context);
    await initDatabase(tab);
    await exec(tab, "CREATE TABLE t (id INTEGER PRIMARY KEY)");

    expect(await attempt(tab, "SELECT * FROM t WHERE id = = 1")).toMatchObject({
      ok: false,
      error: {
        name: "SqlSyntaxError",
        code: "SYNTAX_ERROR",
        isOpfsqlError: true,
        isItsClass: true,
        fields: { offset: 27, length: 1 },
      },
    });
    expect(await attempt(tab, "SELECT * FROM nope")).toMatchObject({
      ok: false,
      error: { name: "SemanticError", isItsClass: true },
    });
    expect(
      await attempt(tab, "INSERT INTO t VALUES (1); INSERT INTO t VALUES (1)"),
    ).toMatchObject({
      ok: false,
      error: {
        code: "CONSTRAINT_VIOLATION",
        isOpfsqlError: true,
        fields: { statementIndex: 1 },
      },
    });
    const [count] = await exec(tab, "SELECT count(*) FROM t");
    expect(count.rows).toEqual([[0]]);

    await closeDatabase(tab);
    await cleanOpfs(tab);
  });

  test("a worker that cannot start fails the open with INTERNAL", async ({
    context,
  }) => {
    const tab = await openTab(context);
    const outcome = await tab.evaluate(async () =>
      globalThis.opfsql.openIn("/no-such-worker.js"),
    );
    expect(outcome).toMatchObject({
      ok: false,
      error: { name: "InternalError", code: "INTERNAL", isItsClass: true },
    });
  });
});
