import { afterEach, beforeEach, describe, expect, it } from "vitest";
import { type Check, unordered } from "./check.js";
import { multisetDifference, type Rows, valueKey } from "./compare.js";
import { closeCopies, type Copies, loadCopies } from "./database.js";
import { runCheck } from "./run-check.js";
import type { ColumnSpec, DatabaseSpec } from "./schema.js";

// The harness itself: a planted mistake in a check must fail it, and the
// errors that are not results must be told from the ones that are bugs.

const column: ColumnSpec = { name: "c", type: "INTEGER", constraint: null };
const spec: DatabaseSpec = {
  tables: [{ name: "t", columns: [column], rowCount: 3 }],
  indexes: [
    {
      name: "t_c",
      table: "t",
      keys: [{ column, form: "column" }],
      isEarly: false,
    },
  ],
};

let copies: Copies;

beforeEach(async () => {
  const rows = new Map([["t", [["1"], ["NULL"], ["-1"]]]]);
  copies = await loadCopies(spec, rows, 4096);
});

afterEach(async () => {
  await closeCopies(copies);
});

/** TLP over `forms`: the first query's rows are the others' together. */
function partitionCheck(...forms: string[]): Check {
  return {
    oracle: "planted",
    forms: forms.map((sql) => unordered(sql)),
    verdict: ([whole = [], ...split]: readonly Rows[]) =>
      multisetDifference(whole, split.flat(), ["the query", "its parts"]),
  };
}

describe("the metamorphic harness", () => {
  it("passes a check whose forms agree", async () => {
    const check = partitionCheck(
      "SELECT c FROM t",
      "SELECT c FROM t WHERE c > 0",
      "SELECT c FROM t WHERE NOT c > 0",
      "SELECT c FROM t WHERE (c > 0) IS NULL",
    );
    expect(await runCheck(check, copies)).toEqual({ kind: "agreed" });
  });

  it("fails a check missing a partition, on both copies", async () => {
    const check = partitionCheck(
      "SELECT c FROM t",
      "SELECT c FROM t WHERE c > 0",
      "SELECT c FROM t WHERE NOT c > 0",
    );
    const outcome = await runCheck(check, copies);
    expect(outcome.kind).toBe("failed");
    expect(outcome.kind === "failed" ? outcome.reason : "").toMatch(
      /^plain copy: 3 rows in the query against 2 in its parts\n.*\nindexed copy: 3 rows in the query against 2 in its parts/su,
    );
  });

  it("fails a form that differs between the copies", async () => {
    await copies.indexed.session.query("DELETE FROM t WHERE c = 1");
    const check: Check = {
      oracle: "copies only",
      forms: [unordered("SELECT c FROM t")],
      verdict: () => null,
    };
    const outcome = await runCheck(check, copies);
    expect(outcome.kind === "failed" ? outcome.reason : "").toMatch(
      /^form 1 on the two copies/u,
    );
  });

  it("skips a check whose form fails at run time", async () => {
    const check = partitionCheck("SELECT CAST('x' || c AS INTEGER) FROM t");
    expect(await runCheck(check, copies)).toEqual({
      kind: "skipped",
      code: "DATATYPE_MISMATCH",
    });
  });

  it("fails a check whose form does not bind: the generator's mistake", async () => {
    const outcome = await runCheck(
      partitionCheck("SELECT nosuch(c) FROM t"),
      copies,
    );
    expect(outcome.kind === "failed" ? outcome.reason : "").toMatch(
      /^plain copy: SEMANTIC_ERROR/u,
    );
  });
});

describe("value keys", () => {
  it("are equal exactly when the values are", () => {
    expect(valueKey({ a: 1, b: [2, { c: 3, d: 4 }] })).toBe(
      valueKey({ b: [2, { d: 4, c: 3 }], a: 1 }),
    );
    expect(valueKey(-0)).toBe(valueKey(0));
    expect(valueKey(1)).not.toBe(valueKey(true));
    expect(valueKey(1)).not.toBe(valueKey("1"));
    expect(valueKey(null)).not.toBe(valueKey("null"));
  });

  it("count a row as often as it comes", () => {
    const sides = ["a", "b"] as const;
    expect(multisetDifference([[1], [1]], [[1], [1]], sides)).toBeNull();
    expect(multisetDifference([[1], [1]], [[1]], sides)).toMatch(
      /^2 rows in a against 1 in b\n {2}only in a/u,
    );
  });
});
