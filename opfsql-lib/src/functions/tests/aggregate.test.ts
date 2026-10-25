import { describe, expect, it } from "vitest";
import { DatatypeMismatchError, IntegerOverflowError } from "../../errors.js";
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
});
