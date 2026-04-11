import { describe, expect, it } from "vitest";
import { compareIndexKeys, keyHasNull } from "../index-btree/compare.js";

describe("compareIndexKeys", () => {
  // --- Simple single-column keys ---

  it("equal numbers", () => {
    expect(compareIndexKeys([1], [1])).toBe(0);
  });

  it("less number", () => {
    expect(compareIndexKeys([1], [2])).toBeLessThan(0);
  });

  it("greater number", () => {
    expect(compareIndexKeys([5], [3])).toBeGreaterThan(0);
  });

  it("equal strings", () => {
    expect(compareIndexKeys(["abc"], ["abc"])).toBe(0);
  });

  it("less string", () => {
    expect(compareIndexKeys(["abc"], ["def"])).toBeLessThan(0);
  });

  it("greater string", () => {
    expect(compareIndexKeys(["xyz"], ["abc"])).toBeGreaterThan(0);
  });

  // --- NULL handling (NULL sorts LAST) ---

  it("null equals null", () => {
    expect(compareIndexKeys([null], [null])).toBe(0);
  });

  it("null greater than non-null", () => {
    expect(compareIndexKeys([null], [1])).toBeGreaterThan(0);
  });

  it("non-null less than null", () => {
    expect(compareIndexKeys([1], [null])).toBeLessThan(0);
  });

  it("string less than null", () => {
    expect(compareIndexKeys(["z"], [null])).toBeLessThan(0);
  });

  // --- Composite keys ---

  it("composite equal", () => {
    expect(compareIndexKeys([1, "a"], [1, "a"])).toBe(0);
  });

  it("composite differs on first element", () => {
    expect(compareIndexKeys([1, "z"], [2, "a"])).toBeLessThan(0);
  });

  it("composite differs on second element", () => {
    expect(compareIndexKeys([1, "a"], [1, "b"])).toBeLessThan(0);
  });

  it("composite with null in second position", () => {
    expect(compareIndexKeys([1, null], [1, "a"])).toBeGreaterThan(0);
  });

  it("composite both null in second position", () => {
    expect(compareIndexKeys([1, null], [1, null])).toBe(0);
  });

  // --- Different key lengths (prefix comparison) ---

  it("shorter key is less than longer key with same prefix", () => {
    expect(compareIndexKeys([1], [1, "a"])).toBeLessThan(0);
  });

  it("longer key is greater than shorter key with same prefix", () => {
    expect(compareIndexKeys([1, "a"], [1])).toBeGreaterThan(0);
  });

  // --- Boolean keys ---

  it("false < true", () => {
    expect(compareIndexKeys([false], [true])).toBeLessThan(0);
  });

  it("true > false", () => {
    expect(compareIndexKeys([true], [false])).toBeGreaterThan(0);
  });

  it("true == true", () => {
    expect(compareIndexKeys([true], [true])).toBe(0);
  });
});

describe("keyHasNull", () => {
  it("no nulls", () => {
    expect(keyHasNull([1, "a", true])).toBe(false);
  });

  it("null in first position", () => {
    expect(keyHasNull([null, "a"])).toBe(true);
  });

  it("null in last position", () => {
    expect(keyHasNull([1, null])).toBe(true);
  });

  it("all nulls", () => {
    expect(keyHasNull([null, null])).toBe(true);
  });

  it("empty key has no nulls", () => {
    expect(keyHasNull([])).toBe(false);
  });
});
