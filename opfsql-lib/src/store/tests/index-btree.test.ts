import { beforeEach, describe, expect, it } from 'vitest';
import { SyncBTree } from '../index-btree/index-btree.js';
import type { IndexKey } from '../index-btree/types.js';
import { SyncPageManager } from '../page-manager.js';
import type { RowId } from '../types.js';
import { MemoryStorage } from '../memory-storage.js';

function rid(a: number, b: number): RowId {
  return a * 1000 + b;
}

describe('SyncBTree', () => {
  let pm: SyncPageManager;
  let tree: SyncBTree;

  beforeEach(() => {
    pm = new SyncPageManager(new MemoryStorage());
    tree = new SyncBTree('test_idx', pm, false);
  });

  describe('insert and search', () => {
    it('insert single key and find it', () => {
      tree.insert([1], rid(0, 0));
      pm.commit();

      expect(tree.search([{ columnPosition: 0, comparisonType: 'EQUAL', value: 1 }])).toEqual([rid(0, 0)]);
    });

    it('insert multiple keys and find each', () => {
      tree.insert([1], rid(0, 0));
      tree.insert([2], rid(0, 1));
      tree.insert([3], rid(0, 2));
      pm.commit();

      for (let i = 1; i <= 3; i++) {
        expect(tree.search([{ columnPosition: 0, comparisonType: 'EQUAL', value: i }])).toEqual([rid(0, i - 1)]);
      }
    });

    it('search for non-existent key returns empty', () => {
      tree.insert([1], rid(0, 0));
      pm.commit();
      expect(tree.search([{ columnPosition: 0, comparisonType: 'EQUAL', value: 999 }])).toEqual([]);
    });

    it('search on empty tree returns empty', () => {
      expect(tree.search([{ columnPosition: 0, comparisonType: 'EQUAL', value: 1 }])).toEqual([]);
    });

    it('duplicate keys in non-unique index accumulate rowIds', () => {
      tree.insert([5], rid(0, 0));
      tree.insert([5], rid(0, 1));
      tree.insert([5], rid(1, 0));
      pm.commit();

      const results = tree.search([{ columnPosition: 0, comparisonType: 'EQUAL', value: 5 }]);
      expect(results).toHaveLength(3);
      expect(results).toContainEqual(rid(0, 0));
      expect(results).toContainEqual(rid(0, 1));
      expect(results).toContainEqual(rid(1, 0));
    });

    it('keys are stored in sorted order', () => {
      tree.insert([30], rid(0, 2));
      tree.insert([10], rid(0, 0));
      tree.insert([20], rid(0, 1));
      pm.commit();

      const results = tree.search([{ columnPosition: 0, comparisonType: 'GREATER_EQUAL', value: 1 }]);
      expect(results).toEqual([rid(0, 0), rid(0, 1), rid(0, 2)]);
    });
  });

  describe('delete', () => {
    it('delete existing key', () => {
      tree.insert([1], rid(0, 0));
      tree.insert([2], rid(0, 1));
      pm.commit();

      tree.delete([1], rid(0, 0));
      pm.commit();

      expect(tree.search([{ columnPosition: 0, comparisonType: 'EQUAL', value: 1 }])).toEqual([]);
      expect(tree.search([{ columnPosition: 0, comparisonType: 'EQUAL', value: 2 }])).toEqual([rid(0, 1)]);
    });

    it('delete non-existent key is a no-op', () => {
      tree.insert([1], rid(0, 0));
      pm.commit();

      tree.delete([999], rid(0, 0));
      pm.commit();

      expect(tree.search([{ columnPosition: 0, comparisonType: 'EQUAL', value: 1 }])).toEqual([rid(0, 0)]);
    });

    it('delete one rowId from duplicate key preserves others', () => {
      tree.insert([5], rid(0, 0));
      tree.insert([5], rid(0, 1));
      pm.commit();

      tree.delete([5], rid(0, 0));
      pm.commit();

      expect(tree.search([{ columnPosition: 0, comparisonType: 'EQUAL', value: 5 }])).toEqual([rid(0, 1)]);
    });

    it('delete on empty tree is a no-op', () => {
      tree.delete([1], rid(0, 0)); // no error
    });

    it('delete wrong rowId for existing key is a no-op', () => {
      tree.insert([1], rid(0, 0));
      pm.commit();

      tree.delete([1], rid(9, 9));
      pm.commit();

      expect(tree.search([{ columnPosition: 0, comparisonType: 'EQUAL', value: 1 }])).toEqual([rid(0, 0)]);
    });
  });

  describe('range scans', () => {
    beforeEach(() => {
      for (let i = 1; i <= 10; i++) tree.insert([i * 10], rid(0, i - 1));
      pm.commit();
    });

    it('GREATER', () => {
      expect(tree.search([{ columnPosition: 0, comparisonType: 'GREATER', value: 80 }])).toEqual([rid(0, 8), rid(0, 9)]);
    });

    it('GREATER_EQUAL', () => {
      expect(tree.search([{ columnPosition: 0, comparisonType: 'GREATER_EQUAL', value: 90 }])).toEqual([rid(0, 8), rid(0, 9)]);
    });

    it('LESS', () => {
      expect(tree.search([{ columnPosition: 0, comparisonType: 'LESS', value: 30 }])).toEqual([rid(0, 0), rid(0, 1)]);
    });

    it('LESS_EQUAL', () => {
      expect(tree.search([{ columnPosition: 0, comparisonType: 'LESS_EQUAL', value: 20 }])).toEqual([rid(0, 0), rid(0, 1)]);
    });

    it('bounded range (GREATER_EQUAL + LESS)', () => {
      expect(tree.search([
        { columnPosition: 0, comparisonType: 'GREATER_EQUAL', value: 30 },
        { columnPosition: 0, comparisonType: 'LESS', value: 60 },
      ])).toEqual([rid(0, 2), rid(0, 3), rid(0, 4)]);
    });
  });

  describe('composite keys', () => {
    it('search composite equality', () => {
      tree.insert(['a', 1], rid(0, 0));
      tree.insert(['a', 2], rid(0, 1));
      tree.insert(['b', 1], rid(0, 2));
      pm.commit();

      expect(tree.search([
        { columnPosition: 0, comparisonType: 'EQUAL', value: 'a' },
        { columnPosition: 1, comparisonType: 'EQUAL', value: 2 },
      ], 2)).toEqual([rid(0, 1)]);
    });

    it('prefix scan on composite index', () => {
      tree.insert(['a', 1], rid(0, 0));
      tree.insert(['a', 2], rid(0, 1));
      tree.insert(['b', 1], rid(0, 2));
      pm.commit();

      const results = tree.search([{ columnPosition: 0, comparisonType: 'EQUAL', value: 'a' }], 2);
      expect(results).toHaveLength(2);
      expect(results).toContainEqual(rid(0, 0));
      expect(results).toContainEqual(rid(0, 1));
    });
  });

  describe('unique constraint', () => {
    let uniqueTree: SyncBTree;

    beforeEach(() => {
      uniqueTree = new SyncBTree('unique_idx', pm, true);
    });

    it('throws on duplicate insert', () => {
      uniqueTree.insert([1], rid(0, 0));
      expect(() => uniqueTree.insert([1], rid(0, 1))).toThrow('UNIQUE constraint failed');
    });

    it('allows duplicate NULL in unique index', () => {
      uniqueTree.insert([null], rid(0, 0));
      uniqueTree.insert([null], rid(0, 1)); // no error
      pm.commit();

      expect(uniqueTree.search([{ columnPosition: 0, comparisonType: 'EQUAL', value: null }])).toHaveLength(2);
    });

    it('allows NULL in composite key even if other column matches', () => {
      uniqueTree.insert([1, null], rid(0, 0));
      uniqueTree.insert([1, null], rid(0, 1)); // no error
      pm.commit();
    });

    it('different keys are allowed in unique index', () => {
      uniqueTree.insert([1], rid(0, 0));
      uniqueTree.insert([2], rid(0, 1));
      pm.commit();

      expect(uniqueTree.search([{ columnPosition: 0, comparisonType: 'EQUAL', value: 1 }])).toEqual([rid(0, 0)]);
    });
  });

  describe('NULL keys', () => {
    it('NULL sorts last (after all non-null values)', () => {
      tree.insert([null], rid(0, 2));
      tree.insert([1], rid(0, 0));
      tree.insert([100], rid(0, 1));
      pm.commit();

      const results = tree.search([{ columnPosition: 0, comparisonType: 'GREATER_EQUAL', value: 1 }]);
      expect(results).toEqual([rid(0, 0), rid(0, 1), rid(0, 2)]);
    });

    it('exact search for NULL finds it', () => {
      tree.insert([null], rid(0, 0));
      tree.insert([1], rid(0, 1));
      pm.commit();

      expect(tree.search([{ columnPosition: 0, comparisonType: 'EQUAL', value: null }])).toEqual([rid(0, 0)]);
    });
  });

  describe('node splitting', () => {
    it('handles enough inserts to trigger leaf split', () => {
      for (let i = 0; i < 150; i++) tree.insert([i], rid(Math.floor(i / 50), i % 50));
      pm.commit();

      for (let i = 0; i < 150; i++) {
        expect(tree.search([{ columnPosition: 0, comparisonType: 'EQUAL', value: i }])).toHaveLength(1);
      }
    });

    it('range scan works across split nodes', () => {
      for (let i = 0; i < 150; i++) tree.insert([i], rid(0, i));
      pm.commit();

      expect(tree.search([
        { columnPosition: 0, comparisonType: 'GREATER_EQUAL', value: 140 },
        { columnPosition: 0, comparisonType: 'LESS_EQUAL', value: 149 },
      ])).toHaveLength(10);
    });

    it('handles reverse-order inserts (worst case for splits)', () => {
      for (let i = 200; i >= 0; i--) tree.insert([i], rid(0, i));
      pm.commit();

      for (const k of [0, 50, 100, 150, 200]) {
        expect(tree.search([{ columnPosition: 0, comparisonType: 'EQUAL', value: k }])).toEqual([rid(0, k)]);
      }
    });
  });

  describe('bulkLoad', () => {
    it('bulk load sorted entries', () => {
      const entries = Array.from({ length: 50 }, (_, i) => ({ key: [i] as IndexKey, rowId: rid(0, i) }));
      tree.bulkLoad(entries);
      pm.commit();

      for (let i = 0; i < 50; i++) {
        expect(tree.search([{ columnPosition: 0, comparisonType: 'EQUAL', value: i }])).toEqual([rid(0, i)]);
      }
    });

    it('bulk load empty entries creates empty tree', () => {
      tree.bulkLoad([]);
      pm.commit();
      expect(tree.search([{ columnPosition: 0, comparisonType: 'EQUAL', value: 1 }])).toEqual([]);
    });

    it('bulk load merges duplicate keys in non-unique index', () => {
      const entries = [
        { key: [1] as IndexKey, rowId: rid(0, 0) },
        { key: [1] as IndexKey, rowId: rid(0, 1) },
        { key: [2] as IndexKey, rowId: rid(0, 2) },
      ];
      tree.bulkLoad(entries);
      pm.commit();

      expect(tree.search([{ columnPosition: 0, comparisonType: 'EQUAL', value: 1 }])).toHaveLength(2);
    });

    it('bulk load unique rejects duplicates', () => {
      const uniqueTree = new SyncBTree('unique_idx', pm, true);
      const entries = [
        { key: [1] as IndexKey, rowId: rid(0, 0) },
        { key: [1] as IndexKey, rowId: rid(0, 1) },
      ];
      expect(() => uniqueTree.bulkLoad(entries)).toThrow('UNIQUE constraint failed');
    });

    it('bulk load with many entries triggers internal nodes', () => {
      const entries = Array.from({ length: 500 }, (_, i) => ({
        key: [i] as IndexKey,
        rowId: rid(Math.floor(i / 100), i % 100),
      }));
      tree.bulkLoad(entries);
      pm.commit();

      for (const k of [0, 99, 250, 499]) {
        expect(tree.search([{ columnPosition: 0, comparisonType: 'EQUAL', value: k }])).toHaveLength(1);
      }

      expect(tree.search([{ columnPosition: 0, comparisonType: 'GREATER_EQUAL', value: 490 }])).toHaveLength(10);
    });
  });

  describe('drop', () => {
    it('drop removes all btree data', () => {
      tree.insert([1], rid(0, 0));
      tree.insert([2], rid(0, 1));
      pm.commit();

      tree.drop();
      pm.commit();

      expect(tree.search([{ columnPosition: 0, comparisonType: 'EQUAL', value: 1 }])).toEqual([]);
    });
  });

  describe('string keys', () => {
    it('string keys sort lexicographically', () => {
      tree.insert(['banana'], rid(0, 0));
      tree.insert(['apple'], rid(0, 1));
      tree.insert(['cherry'], rid(0, 2));
      pm.commit();

      const results = tree.search([
        { columnPosition: 0, comparisonType: 'GREATER_EQUAL', value: 'a' },
        { columnPosition: 0, comparisonType: 'LESS_EQUAL', value: 'b' },
      ]);
      expect(results).toEqual([rid(0, 1)]);
    });
  });

  describe('persistence across commits', () => {
    it('data survives commit and is readable from storage', () => {
      tree.insert([1], rid(0, 0));
      pm.commit();

      tree.insert([2], rid(0, 1));
      pm.commit();

      expect(tree.search([{ columnPosition: 0, comparisonType: 'EQUAL', value: 1 }])).toEqual([rid(0, 0)]);
      expect(tree.search([{ columnPosition: 0, comparisonType: 'EQUAL', value: 2 }])).toEqual([rid(0, 1)]);
    });
  });
});
