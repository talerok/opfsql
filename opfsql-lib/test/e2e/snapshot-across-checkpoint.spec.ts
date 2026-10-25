import { test, expect } from "@playwright/test";
import { openTab, initDb, openDb, exec, closeDb, cleanOpfs } from "./infra/db";

// Enough single-row commits to pass the default checkpoint threshold
// (1000 WAL frames) and make the writing tab checkpoint.
const BATCH = 400;

function inserts(from: number): string {
  return Array.from(
    { length: BATCH },
    (_, i) => `INSERT INTO t VALUES (${from + i}, 'original')`,
  ).join(";\n");
}

async function count(
  page: Parameters<typeof exec>[0],
  where = "1 = 1",
): Promise<number> {
  const [result] = await exec(
    page,
    `SELECT COUNT(*) AS n FROM t WHERE ${where}`,
  );
  return result.rows[0].n;
}

test.describe("Snapshots across tabs", () => {
  test("a transaction keeps its snapshot through another tab's checkpoints", async ({
    context,
  }) => {
    const tabA = await openTab(context);
    await initDb(tabA);
    await exec(tabA, "CREATE TABLE t (id INTEGER PRIMARY KEY, val TEXT)");
    // Checkpoints move these rows into the main file, where tab B will read them.
    await exec(tabA, inserts(1));

    const tabB = await openTab(context);
    await openDb(tabB);
    await exec(tabB, "BEGIN");
    expect(await count(tabB)).toBe(BATCH);

    // Tab A rewrites pages tab B reads from the main file, then checkpoints.
    await exec(tabA, "UPDATE t SET val = 'changed' WHERE id <= 200");
    await exec(tabA, inserts(BATCH + 1));

    expect(await count(tabB)).toBe(BATCH);
    expect(await count(tabB, "val = 'changed'")).toBe(0);

    await exec(tabB, "COMMIT");
    expect(await count(tabB)).toBe(2 * BATCH);
    expect(await count(tabB, "val = 'changed'")).toBe(200);

    await closeDb(tabB);
    await closeDb(tabA);
    await cleanOpfs(tabA);
  });
});
