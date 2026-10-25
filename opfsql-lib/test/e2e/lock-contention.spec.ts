import {
  attempt,
  cleanOpfs,
  closeDatabase,
  disconnect,
  exec,
  initDatabase,
  openDatabase,
  openTab,
  settle,
  start,
} from "./infra/database";
import { expect, test } from "./infra/fixtures";

const sleep = async (ms: number) =>
  new Promise((resolve) => setTimeout(resolve, ms));

test.describe("Writers in several tabs", { tag: "@multi-tab" }, () => {
  test("a CREATE TABLE that waited for the lock keeps the other tab's table", async ({
    context,
  }) => {
    const tabA = await openTab(context);
    await initDatabase(tabA);
    const tabB = await openTab(context);
    await openDatabase(tabB);

    await exec(tabB, "BEGIN");
    await exec(tabB, "CREATE TABLE b_tbl (id INTEGER PRIMARY KEY)");
    await start(tabA, "create", "CREATE TABLE a_tbl (id INTEGER PRIMARY KEY)");
    await sleep(200);
    await exec(tabB, "COMMIT");
    expect(await settle(tabA, "create")).toMatchObject({ ok: true });

    for (const [index, tab] of [tabA, tabB].entries()) {
      await exec(tab, "INSERT INTO a_tbl VALUES ($1)", [index]);
      await exec(tab, "INSERT INTO b_tbl VALUES ($1)", [index]);
    }
    const [counts] = await exec(
      tabA,
      "SELECT (SELECT count(*) FROM a_tbl) AS a, (SELECT count(*) FROM b_tbl) AS b",
    );
    expect(counts.objects).toEqual([{ a: 2, b: 2 }]);

    await closeDatabase(tabB);
    await closeDatabase(tabA);
    await cleanOpfs(tabA);
  });

  test("a writer waits no longer than its busy timeout", async ({
    context,
  }) => {
    const tabA = await openTab(context);
    await initDatabase(tabA);
    await exec(tabA, "CREATE TABLE t (id INTEGER PRIMARY KEY)");
    const tabB = await openTab(context);
    await openDatabase(tabB, undefined, { busyTimeout: 300 });

    await exec(tabA, "BEGIN IMMEDIATE");
    expect(await attempt(tabB, "INSERT INTO t VALUES (1)")).toMatchObject({
      ok: false,
      error: {
        name: "BusyError",
        code: "BUSY",
        isItsClass: true,
        fields: { timeout: 300 },
      },
    });
    await exec(tabA, "COMMIT");
    await exec(tabB, "INSERT INTO t VALUES (1)");

    await closeDatabase(tabB);
    await closeDatabase(tabA);
    await cleanOpfs(tabA);
  });

  test("a failed script leaves no lock behind", async ({ context }) => {
    const tabA = await openTab(context);
    await initDatabase(tabA);
    await exec(tabA, "CREATE TABLE t (id INTEGER PRIMARY KEY)");
    await exec(tabA, "INSERT INTO t VALUES (1)");
    const tabB = await openTab(context);
    await openDatabase(tabB, undefined, { busyTimeout: 0 });

    expect(
      await attempt(
        tabA,
        "BEGIN; INSERT INTO t VALUES (2); INSERT INTO t VALUES (1); COMMIT",
      ),
    ).toMatchObject({
      ok: false,
      error: { code: "CONSTRAINT_VIOLATION", fields: { statementIndex: 2 } },
    });
    await exec(tabB, "INSERT INTO t VALUES (3)");
    const [rows] = await exec(tabA, "SELECT id FROM t ORDER BY id");
    expect(rows.rows).toEqual([[1], [3]]);

    await closeDatabase(tabB);
    await closeDatabase(tabA);
    await cleanOpfs(tabA);
  });

  test("closing a connection cancels its wait for the lock", async ({
    context,
  }) => {
    const tabA = await openTab(context);
    await initDatabase(tabA);
    await exec(tabA, "CREATE TABLE t (id INTEGER PRIMARY KEY)");
    const tabB = await openTab(context);
    await openDatabase(tabB);

    await exec(tabA, "BEGIN IMMEDIATE");
    await start(tabB, "insert", "INSERT INTO t VALUES (1)");
    await sleep(100);
    await disconnect(tabB);
    expect(await settle(tabB, "insert")).toMatchObject({
      ok: false,
      error: { name: "AbortError", code: "ABORTED" },
    });
    await exec(tabA, "INSERT INTO t VALUES (2)");
    await exec(tabA, "COMMIT");
    const [rows] = await exec(tabA, "SELECT id FROM t");
    expect(rows.rows).toEqual([[2]]);

    await closeDatabase(tabB);
    await closeDatabase(tabA);
    await cleanOpfs(tabA);
  });
});
