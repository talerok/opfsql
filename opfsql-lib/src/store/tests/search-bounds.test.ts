import { describe, expect, it } from 'vitest';
import { computeBounds, type SearchPredicate } from '../index-btree/search-bounds.js';

describe('computeBounds', () => {
  describe('equality-only (point lookup or prefix scan)', () => {
    it('single equality sets lower=upper (inclusive)', () => {
      const preds: SearchPredicate[] = [
        { columnPosition: 0, comparisonType: 'EQUAL', value: 42 },
      ];
      const b = computeBounds(preds);
      expect(b.lower).toEqual([42]);
      expect(b.upper).toEqual([42]);
      expect(b.lowerInclusive).toBe(true);
      expect(b.upperInclusive).toBe(true);
    });

    it('two equalities sorted by column', () => {
      const preds: SearchPredicate[] = [
        { columnPosition: 1, comparisonType: 'EQUAL', value: 'b' },
        { columnPosition: 0, comparisonType: 'EQUAL', value: 'a' },
      ];
      const b = computeBounds(preds);
      expect(b.lower).toEqual(['a', 'b']);
      expect(b.upper).toEqual(['a', 'b']);
    });
  });

  describe('range only', () => {
    it('GREATER', () => {
      const preds: SearchPredicate[] = [
        { columnPosition: 0, comparisonType: 'GREATER', value: 5 },
      ];
      const b = computeBounds(preds);
      expect(b.lower).toEqual([5]);
      expect(b.lowerInclusive).toBe(false);
      expect(b.upper).toBeUndefined();
    });

    it('LESS', () => {
      const preds: SearchPredicate[] = [
        { columnPosition: 0, comparisonType: 'LESS', value: 10 },
      ];
      const b = computeBounds(preds);
      expect(b.upper).toEqual([10]);
      expect(b.upperInclusive).toBe(false);
      expect(b.lower).toBeUndefined();
    });

    it('GREATER_EQUAL and LESS_EQUAL', () => {
      const preds: SearchPredicate[] = [
        { columnPosition: 0, comparisonType: 'GREATER_EQUAL', value: 1 },
        { columnPosition: 0, comparisonType: 'LESS_EQUAL', value: 100 },
      ];
      const b = computeBounds(preds);
      expect(b.lower).toEqual([1]);
      expect(b.lowerInclusive).toBe(true);
      expect(b.upper).toEqual([100]);
      expect(b.upperInclusive).toBe(true);
    });
  });

  describe('equality + range', () => {
    it('equality on first column + GREATER on second', () => {
      const preds: SearchPredicate[] = [
        { columnPosition: 0, comparisonType: 'EQUAL', value: 'a' },
        { columnPosition: 1, comparisonType: 'GREATER', value: 5 },
      ];
      const b = computeBounds(preds);
      expect(b.lower).toEqual(['a', 5]);
      expect(b.lowerInclusive).toBe(false);
      expect(b.upper).toEqual(['a']);
      expect(b.upperInclusive).toBe(true);
    });

    it('equality on first column + LESS_EQUAL on second', () => {
      const preds: SearchPredicate[] = [
        { columnPosition: 0, comparisonType: 'EQUAL', value: 'x' },
        { columnPosition: 1, comparisonType: 'LESS_EQUAL', value: 99 },
      ];
      const b = computeBounds(preds);
      expect(b.lower).toEqual(['x']);
      expect(b.lowerInclusive).toBe(true);
      expect(b.upper).toEqual(['x', 99]);
      expect(b.upperInclusive).toBe(true);
    });

    it('equality on first + bounded range on second', () => {
      const preds: SearchPredicate[] = [
        { columnPosition: 0, comparisonType: 'EQUAL', value: 'a' },
        { columnPosition: 1, comparisonType: 'GREATER_EQUAL', value: 10 },
        { columnPosition: 1, comparisonType: 'LESS', value: 20 },
      ];
      const b = computeBounds(preds);
      expect(b.lower).toEqual(['a', 10]);
      expect(b.lowerInclusive).toBe(true);
      expect(b.upper).toEqual(['a', 20]);
      expect(b.upperInclusive).toBe(false);
    });
  });

  describe('null values', () => {
    it('equality on null', () => {
      const preds: SearchPredicate[] = [
        { columnPosition: 0, comparisonType: 'EQUAL', value: null },
      ];
      const b = computeBounds(preds);
      expect(b.lower).toEqual([null]);
      expect(b.upper).toEqual([null]);
    });
  });
});
