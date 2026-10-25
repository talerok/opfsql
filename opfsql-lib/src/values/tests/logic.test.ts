import { describe, expect, it } from "vitest";
import type { Truth } from "../logic.js";
import {
  and,
  compareSql,
  isIn,
  isSame,
  membership,
  not,
  or,
} from "../logic.js";
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

  it("IS treats two NULLs as the same and never returns NULL", () => {
    expect(isSame(null, null)).toBe(true);
    expect(isSame(null, 0)).toBe(false);
    expect(isSame(2, 4 / 2)).toBe(true);
  });
});

describe("IN", () => {
  it("is true when an item matches, even with NULLs around", () => {
    expect(isIn(1, [null, 1])).toBe(true);
  });

  it("is NULL without a match when the needle or an item is NULL", () => {
    expect(isIn(1, [2, null])).toBeNull();
    expect(isIn(null, [1, 2])).toBeNull();
  });

  it("is false without a match or NULLs", () => {
    expect(isIn(1, [2, 3])).toBe(false);
  });

  it("is false for an empty list, whatever the needle", () => {
    expect(isIn(null, [])).toBe(false);
    expect(not(isIn(null, []))).toBe(true);
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

  it("answers as isIn does, for every list and needle", () => {
    for (const list of lists) {
      const test = membership(list);
      for (const needle of needles) {
        expect(test(needle)).toBe(isIn(needle, list));
      }
    }
  });
});
