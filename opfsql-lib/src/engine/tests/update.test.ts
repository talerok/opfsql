import { resetMockOPFS } from "opfs-mock";
import { afterEach, beforeEach, describe, expect, it } from "vitest";
import { failureOf } from "../../../test/support/failure.js";
import { rowsOf } from "../../../test/support/results.js";
import type { Engine } from "../engine.js";
import type { Session } from "../session.js";
import { openTestEngine } from "./test-engine.js";

// What UPDATE leaves in the table: every new row computed from the table as
// it was before the statement (docs/semantics.md, Departures from SQLite),
// whether the rows are written one at a time or all computed first.

let engine: Engine;
let session: Session;
let seq = 0;

beforeEach(async () => {
  resetMockOPFS();
  engine = await openTestEngine(`update-${String(seq++)}`);
  session = engine.connect();
  await session.execute(
    "CREATE TABLE t (id INTEGER PRIMARY KEY, x INTEGER, code TEXT UNIQUE)",
  );
  await session.execute(
    "INSERT INTO t VALUES (1, 1, 'a'), (2, 2, 'b'), (3, 3, 'c')",
  );
});

afterEach(async () => {
  await session.close();
  await engine.close();
});

async function table(): Promise<readonly unknown[]> {
  return rowsOf(session.query("SELECT id, x, code FROM t ORDER BY id"));
}

const UNCHANGED = [
  [1, 1, "a"],
  [2, 2, "b"],
  [3, 3, "c"],
];

describe("UPDATE", () => {
  it("changes the rows its WHERE selects, each from its own old values", async () => {
    const result = await session.query(
      "UPDATE t SET x = x * 10, code = upper(code) WHERE x >= 2",
    );
    expect(result.changes).toBe(2);
    expect(await table()).toEqual([
      [1, 1, "a"],
      [2, 20, "B"],
      [3, 30, "C"],
    ]);
  });

  it("computes every assignment from the old row, the rowid column too", async () => {
    await session.query("UPDATE t SET x = id + 100, id = x + 10");
    expect(await table()).toEqual([
      [11, 101, "a"],
      [12, 102, "b"],
      [13, 103, "c"],
    ]);
  });

  it("moves rows to rowids the statement freed, and refuses one still taken", async () => {
    await session.query("UPDATE t SET id = id - 1");
    expect(await table()).toEqual([
      [0, 1, "a"],
      [1, 2, "b"],
      [2, 3, "c"],
    ]);
    // Row 0 moves to 1, which row 1 still holds: nothing changes.
    expect(
      await failureOf(session.query("UPDATE t SET id = id + 1")),
    ).toMatchObject({
      code: "CONSTRAINT_VIOLATION",
    });
    expect(await table()).toEqual([
      [0, 1, "a"],
      [1, 2, "b"],
      [2, 3, "c"],
    ]);
  });

  it("rolls back the rows it wrote when a later row breaks UNIQUE", async () => {
    await session.execute("BEGIN");
    // Row 2 takes 'same' and is written; row 3 then breaks UNIQUE.
    expect(
      await failureOf(
        session.query("UPDATE t SET code = 'same' WHERE id >= 2"),
      ),
    ).toMatchObject({ code: "CONSTRAINT_VIOLATION" });
    expect(session.inTransaction).toBe(true);
    expect(await table()).toEqual(UNCHANGED);
    expect(
      await rowsOf(session.query("SELECT id FROM t WHERE code = 'b'")),
    ).toEqual([[2]]);
    await session.execute("COMMIT");
    expect(await table()).toEqual(UNCHANGED);
  });

  it("reads the target table as it was in an assignment's subquery: running sums 1, 3, 6", async () => {
    await session.query(
      "UPDATE t SET x = (SELECT sum(x) FROM t u WHERE u.id <= t.id)",
    );
    expect(await table()).toEqual([
      [1, 1, "a"],
      [2, 3, "b"],
      [3, 6, "c"],
    ]);
  });

  it("reads the target table as it was in an uncorrelated subquery", async () => {
    await session.query("UPDATE t SET x = x + (SELECT max(x) FROM t)");
    expect(await table()).toEqual([
      [1, 4, "a"],
      [2, 5, "b"],
      [3, 6, "c"],
    ]);
  });

  it("changes more rows than a batch holds, each once", async () => {
    await session.execute("DELETE FROM t");
    const values = Array.from(
      { length: 1500 },
      (_, index) => `(${String(index + 1)}, ${String(index)}, NULL)`,
    );
    await session.execute(`INSERT INTO t VALUES ${values.join(", ")}`);
    const result = await session.query("UPDATE t SET x = x + 1");
    expect(result.changes).toBe(1500);
    expect(
      await rowsOf(
        session.query("SELECT count(*), sum(x), min(x), max(x) FROM t"),
      ),
    ).toEqual([[1500, (1500 * 1501) / 2, 1, 1500]]);
  });
});
