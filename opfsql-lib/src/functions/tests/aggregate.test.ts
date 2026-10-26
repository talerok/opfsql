import { describe, expect, it } from "vitest";
import {
  DatatypeMismatchError,
  IntegerOverflowError,
  SemanticError,
} from "../../errors.js";
import { aggregate } from "./helpers.js";

const MAX = Number.MAX_SAFE_INTEGER;

describe("aggregates", () => {
  it("COUNT counts rows, or non-NULL values", () => {
    expect(aggregate("count", [[], [], []], { types: [] })).toBe(3);
    expect(aggregate("count", [[1], [null], [2]])).toBe(2);
    expect(aggregate("count", [], { types: ["INTEGER"] })).toBe(0);
  });

  it("COUNT DISTINCT counts equal values once", () => {
    expect(
      aggregate("count", [[1], [1], [2], [null], ["1"]], {
        types: ["ANY"],
        distinct: true,
      }),
    ).toBe(3);
  });

  it("SUM is NULL over no rows and skips NULLs", () => {
    expect(aggregate("sum", [], { types: ["INTEGER"] })).toBeNull();
    expect(aggregate("sum", [[null]], { types: ["INTEGER"] })).toBeNull();
    expect(aggregate("sum", [[1], [null], [2]])).toBe(3);
    expect(aggregate("sum", [[1.5], [2]], { types: ["REAL"] })).toBe(3.5);
  });

  it("SUM over integers fails with INTEGER_OVERFLOW", () => {
    expect(() => aggregate("sum", [[MAX], [1]])).toThrow(IntegerOverflowError);
    expect(() => aggregate("sum", [[MAX], [1]], { types: ["ANY"] })).toThrow(
      IntegerOverflowError,
    );
    expect(aggregate("sum", [[MAX], [1]], { types: ["REAL"] })).toBe(MAX + 1);
  });

  it("SUM and AVG add REALs with compensation, as SQLite does", () => {
    // sqlite3: ten times 0.1 sums to exactly 1.0 (naively 0.9999999999999999)
    const tenths = Array.from({ length: 10 }, () => [0.1]);
    expect(aggregate("sum", tenths, { types: ["REAL"] })).toBe(1);
    expect(aggregate("avg", tenths, { types: ["REAL"] })).toBeCloseTo(0.1, 15);
    expect(aggregate("sum", [[Infinity], [1]], { types: ["REAL"] })).toBe(
      Infinity,
    );
    expect(
      aggregate("sum", [[Infinity], [-Infinity]], { types: ["REAL"] }),
    ).toBeNull();
  });

  it("SUM and AVG refuse values that are not numbers", () => {
    expect(() => aggregate("sum", [["x"]], { types: ["ANY"] })).toThrow(
      DatatypeMismatchError,
    );
    expect(() => aggregate("avg", [[true]], { types: ["ANY"] })).toThrow(
      DatatypeMismatchError,
    );
  });

  it("AVG is a REAL mean, NULL over no rows", () => {
    expect(aggregate("avg", [[1], [2]])).toBe(1.5);
    expect(aggregate("avg", [], { types: ["INTEGER"] })).toBeNull();
    expect(
      aggregate("avg", [[1], [1], [4]], { types: ["INTEGER"], distinct: true }),
    ).toBe(2.5);
  });

  it("MIN and MAX use the total order and skip NULLs", () => {
    const rows = [[3], [null], ["a"], [true], [10]];
    expect(aggregate("min", rows, { types: ["ANY"] })).toBe(true);
    expect(aggregate("max", rows, { types: ["ANY"] })).toBe("a");
    expect(aggregate("max", [[9], [10]])).toBe(10);
    expect(aggregate("min", [["9"], ["10"]])).toBe("10");
    expect(aggregate("max", [[null]], { types: ["INTEGER"] })).toBeNull();
  });

  it("TOTAL is SUM as a REAL: 0 over no rows, never an overflow", () => {
    expect(aggregate("total", [], { types: ["INTEGER"] })).toBe(0);
    expect(aggregate("total", [[null]], { types: ["INTEGER"] })).toBe(0);
    expect(aggregate("total", [[1], [null], [2]])).toBe(3);
    expect(aggregate("total", [[MAX], [MAX]])).toBe(MAX * 2);
    const tenths = Array.from({ length: 10 }, () => [0.1]);
    expect(aggregate("total", tenths, { types: ["REAL"] })).toBe(1);
    expect(() => aggregate("total", [["x"]], { types: ["ANY"] })).toThrow(
      DatatypeMismatchError,
    );
  });

  it("GROUP_CONCAT joins the values' text in the order they come, NULLs skipped", () => {
    expect(aggregate("group_concat", [["a"], [null], ["b"], ["c"]])).toBe(
      "a,b,c",
    );
    expect(
      aggregate("group_concat", [[1], [2.5], [true]], { types: ["ANY"] }),
    ).toBe("1,2.5,true");
    expect(aggregate("group_concat", [], { types: ["TEXT"] })).toBeNull();
    expect(aggregate("group_concat", [[null]], { types: ["TEXT"] })).toBeNull();
    expect(
      aggregate("group_concat", [["a"], ["a"], ["b"]], { distinct: true }),
    ).toBe("a,b");
  });

  it("GROUP_CONCAT puts each later row's own separator before its value", () => {
    const rows = [
      ["a", "-"],
      ["b", "+"],
      ["c", null],
      [null, "!"],
    ];
    expect(aggregate("group_concat", rows, { types: ["TEXT", "TEXT"] })).toBe(
      "a+bc",
    );
  });

  it("GROUP_CONCAT takes what || joins: JSON as its text, not a BLOB", () => {
    expect(
      aggregate("group_concat", [[{ b: 1, a: 2 }], [[1]]], { types: ["JSON"] }),
    ).toBe('{"a":2,"b":1},[1]');
    expect(() =>
      aggregate("group_concat", [[Uint8Array.of(1)]], { types: ["BLOB"] }),
    ).toThrow(SemanticError);
  });
});
