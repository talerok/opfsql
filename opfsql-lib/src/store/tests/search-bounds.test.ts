import { describe, expect, it } from "vitest";
import { computeBounds, type SearchPredicate } from "../index-btree/search-bounds.js";

describe("computeBounds", () => {
  // --- Point lookup (all columns covered by equality) ---

  describe("point lookup", () => {
    it("single equality predicate (totalColumns unknown)", () => {
      const preds: SearchPredicate[] = [
        { columnPosition: 0, comparisonType: "EQUAL", value: 42 },
      ];
      const b = computeBounds(preds);
      expect(b.exactKey).toEqual([42]);
      expect(b.lowerKey).toBeNull();
      expect(b.upperKey).toBeNull();
    });

    it("single equality with totalColumns=1 is point lookup", () => {
      const preds: SearchPredicate[] = [
        { columnPosition: 0, comparisonType: "EQUAL", value: "foo" },
      ];
      const b = computeBounds(preds, 1);
      expect(b.exactKey).toEqual(["foo"]);
    });

    it("two equalities covering all columns", () => {
      const preds: SearchPredicate[] = [
        { columnPosition: 0, comparisonType: "EQUAL", value: 1 },
        { columnPosition: 1, comparisonType: "EQUAL", value: "x" },
      ];
      const b = computeBounds(preds, 2);
      expect(b.exactKey).toEqual([1, "x"]);
    });
  });

  // --- Prefix scan ---

  describe("prefix scan", () => {
    it("equality on first column of composite index", () => {
      const preds: SearchPredicate[] = [
        { columnPosition: 0, comparisonType: "EQUAL", value: 10 },
      ];
      const b = computeBounds(preds, 3);
      expect(b.exactKey).toBeNull();
      expect(b.lowerKey).toEqual([10]);
      expect(b.upperKey).toEqual([10]);
      expect(b.lowerInclusive).toBe(true);
      expect(b.upperInclusive).toBe(true);
      expect(b.prefixScan).toBe(true);
    });

    it("two equalities on 3-column index", () => {
      const preds: SearchPredicate[] = [
        { columnPosition: 0, comparisonType: "EQUAL", value: "a" },
        { columnPosition: 1, comparisonType: "EQUAL", value: "b" },
      ];
      const b = computeBounds(preds, 3);
      expect(b.exactKey).toBeNull();
      expect(b.lowerKey).toEqual(["a", "b"]);
      expect(b.upperKey).toEqual(["a", "b"]);
      expect(b.prefixScan).toBe(true);
    });
  });

  // --- Range predicates (no equalities) ---

  describe("range only", () => {
    it("GREATER", () => {
      const preds: SearchPredicate[] = [
        { columnPosition: 0, comparisonType: "GREATER", value: 5 },
      ];
      const b = computeBounds(preds);
      expect(b.lowerKey).toEqual([5]);
      expect(b.lowerInclusive).toBe(false);
      expect(b.upperKey).toBeNull();
    });

    it("LESS", () => {
      const preds: SearchPredicate[] = [
        { columnPosition: 0, comparisonType: "LESS", value: 10 },
      ];
      const b = computeBounds(preds);
      expect(b.upperKey).toEqual([10]);
      expect(b.upperInclusive).toBe(false);
      expect(b.lowerKey).toBeNull();
    });

    it("GREATER_EQUAL and LESS_EQUAL", () => {
      const preds: SearchPredicate[] = [
        { columnPosition: 0, comparisonType: "GREATER_EQUAL", value: 1 },
        { columnPosition: 0, comparisonType: "LESS_EQUAL", value: 100 },
      ];
      const b = computeBounds(preds);
      expect(b.lowerKey).toEqual([1]);
      expect(b.lowerInclusive).toBe(true);
      expect(b.upperKey).toEqual([100]);
      expect(b.upperInclusive).toBe(true);
    });
  });

  // --- Equality + range (mixed) ---

  describe("equality + range", () => {
    it("equality on first column + GREATER on second", () => {
      const preds: SearchPredicate[] = [
        { columnPosition: 0, comparisonType: "EQUAL", value: "a" },
        { columnPosition: 1, comparisonType: "GREATER", value: 5 },
      ];
      const b = computeBounds(preds);
      expect(b.lowerKey).toEqual(["a", 5]);
      expect(b.lowerInclusive).toBe(false);
      expect(b.upperKey).toEqual(["a"]);
      expect(b.upperInclusive).toBe(true);
      expect(b.prefixScan).toBe(true);
    });

    it("equality on first column + LESS_EQUAL on second", () => {
      const preds: SearchPredicate[] = [
        { columnPosition: 0, comparisonType: "EQUAL", value: "x" },
        { columnPosition: 1, comparisonType: "LESS_EQUAL", value: 99 },
      ];
      const b = computeBounds(preds);
      expect(b.lowerKey).toEqual(["x"]);
      expect(b.lowerInclusive).toBe(true);
      expect(b.upperKey).toEqual(["x", 99]);
      expect(b.upperInclusive).toBe(true);
    });

    it("equality on first + bounded range on second", () => {
      const preds: SearchPredicate[] = [
        { columnPosition: 0, comparisonType: "EQUAL", value: "a" },
        { columnPosition: 1, comparisonType: "GREATER_EQUAL", value: 10 },
        { columnPosition: 1, comparisonType: "LESS", value: 20 },
      ];
      const b = computeBounds(preds);
      expect(b.lowerKey).toEqual(["a", 10]);
      expect(b.lowerInclusive).toBe(true);
      expect(b.upperKey).toEqual(["a", 20]);
      expect(b.upperInclusive).toBe(false);
    });
  });

  // --- NULL value in predicates ---

  describe("null values", () => {
    it("equality on null", () => {
      const preds: SearchPredicate[] = [
        { columnPosition: 0, comparisonType: "EQUAL", value: null },
      ];
      const b = computeBounds(preds);
      expect(b.exactKey).toEqual([null]);
    });
  });

  // --- Predicate ordering ---

  describe("predicate ordering", () => {
    it("equalities are sorted by column position", () => {
      const preds: SearchPredicate[] = [
        { columnPosition: 1, comparisonType: "EQUAL", value: "b" },
        { columnPosition: 0, comparisonType: "EQUAL", value: "a" },
      ];
      const b = computeBounds(preds, 2);
      expect(b.exactKey).toEqual(["a", "b"]);
    });
  });
});
