import { describe, expect, it } from "vitest";
import type { Truth } from "../logic.js";
import { and, compareSql, foldTruth, membership, not, or } from "../logic.js";
import { areEqual } from "../order.js";
import type { Value } from "../value.js";

const TRUTHS: Truth[] = [true, false, null];

describe("three-valued logic", () => {
  it("AND is false if either side is false, else unknown if either is", () => {
    const table = TRUTHS.map((a) => TRUTHS.map((b) => and(a, b)));
    expect(table).toEqual([
      [true, false, null],
      [false, false, false],
      [null, false, null],
    ]);
  });

  it("OR is true if either side is true, else unknown if either is", () => {
    const table = TRUTHS.map((a) => TRUTHS.map((b) => or(a, b)));
    expect(table).toEqual([
      [true, true, true],
      [true, false, null],
      [true, null, null],
    ]);
  });

  it("NOT unknown is unknown", () => {
    expect(TRUTHS.map((a) => not(a))).toEqual([false, true, null]);
  });
});

describe("comparisons", () => {
  it("are NULL when either side is NULL", () => {
    expect(compareSql(null, 1)).toBeNull();
    expect(compareSql("a", null)).toBeNull();
    expect(compareSql(1, 2)).toBeLessThan(0);
  });
});

/** IN as docs/semantics.md defines it, item by item: what membership must agree with. */
function isIn(needle: Value, items: readonly Value[]): boolean | null {
  if (items.length === 0) return false;
  if (needle === null) return null;
  const isFound = items.some((item) => item !== null && areEqual(needle, item));
  if (isFound) return true;
  return items.includes(null) ? null : false;
}

describe("IN", () => {
  it("is true when an item matches, even with NULLs around", () => {
    expect(membership([null, 1])(1)).toBe(true);
  });

  it("is NULL without a match when the needle or an item is NULL", () => {
    expect(membership([2, null])(1)).toBeNull();
    expect(membership([1, 2])(null)).toBeNull();
  });

  it("is false without a match or NULLs", () => {
    expect(membership([2, 3])(1)).toBe(false);
  });

  it("is false for an empty list, whatever the needle", () => {
    expect(membership([])(null)).toBe(false);
    expect(not(membership([])(null))).toBe(true);
  });
});

describe("membership", () => {
  const lists: Value[][] = [
    [],
    [1, 2],
    [1, null],
    [null],
    ["1", 1, true, new Uint8Array([1]), { a: [1] }],
  ];
  const needles: Value[] = [
    null,
    1,
    2,
    "1",
    true,
    new Uint8Array([1]),
    { a: [1] },
    3,
  ];

  it("answers as IN is defined, for every list and needle", () => {
    for (const list of lists) {
      const test = membership(list);
      for (const needle of needles) {
        expect(test(needle)).toBe(isIn(needle, list));
      }
    }
  });
});

describe("foldTruth", () => {
  it("folds as AND and OR, three-valued", () => {
    expect(foldTruth("and", [true, null])).toBeNull();
    expect(foldTruth("and", [null, false])).toBe(false);
    expect(foldTruth("or", [false, null])).toBeNull();
    expect(foldTruth("or", [null, true])).toBe(true);
    expect(foldTruth("and", [])).toBe(true);
    expect(foldTruth("or", [])).toBe(false);
  });

  it("stops at the first truth that decides", () => {
    const seen: Truth[] = [];
    function* truths(): Generator<Truth> {
      for (const truth of [null, false, true] as Truth[]) {
        seen.push(truth);
        yield truth;
      }
    }
    expect(foldTruth("and", truths())).toBe(false);
    expect(seen).toEqual([null, false]);
  });
});
