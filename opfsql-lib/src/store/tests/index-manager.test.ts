import { beforeEach, describe, expect, it } from 'vitest';
import type { IndexKey } from '../index-btree/types.js';
import { SyncIndexManager } from '../index-manager.js';
import { SyncPageManager } from '../page-manager.js';
import type { RowId } from '../types.js';
import { MemoryStorage } from '../memory-storage.js';

function rid(a: number, b: number): RowId {
  return a * 1000 + b;
}

describe('SyncIndexManager', () => {
  let pm: SyncPageManager;
  let im: SyncIndexManager;

  beforeEach(() => {
    pm = new SyncPageManager(new MemoryStorage());
    im = new SyncIndexManager(pm);
  });

  describe('insert and search', () => {
    it('insert and find a single entry', () => {
      im.insert('idx1', [10], rid(0, 0));
      pm.commit();

      expect(im.search('idx1', [{ columnPosition: 0, comparisonType: 'EQUAL', value: 10 }])).toEqual([rid(0, 0)]);
    });

    it('insert with unique=true enforces uniqueness', () => {
      im.bulkLoad('idx1', [], true);
      im.insert('idx1', [1], rid(0, 0));
      expect(() => im.insert('idx1', [1], rid(0, 1))).toThrow('UNIQUE constraint failed');
    });

    it('different indexes are independent', () => {
      im.insert('idx_a', [1], rid(0, 0));
      im.insert('idx_b', [1], rid(0, 1));
      pm.commit();

      expect(im.search('idx_a', [{ columnPosition: 0, comparisonType: 'EQUAL', value: 1 }])).toEqual([rid(0, 0)]);
      expect(im.search('idx_b', [{ columnPosition: 0, comparisonType: 'EQUAL', value: 1 }])).toEqual([rid(0, 1)]);
    });
  });

  describe('delete', () => {
    it('delete removes entry from index', () => {
      im.insert('idx1', [5], rid(0, 0));
      pm.commit();

      im.delete('idx1', [5], rid(0, 0));
      pm.commit();

      expect(im.search('idx1', [{ columnPosition: 0, comparisonType: 'EQUAL', value: 5 }])).toEqual([]);
    });
  });

  describe('bulkLoad', () => {
    it('bulk load creates searchable index', () => {
      const entries = Array.from({ length: 20 }, (_, i) => ({ key: [i * 10] as IndexKey, rowId: rid(0, i) }));
      im.bulkLoad('idx1', entries, false);
      pm.commit();

      expect(im.search('idx1', [{ columnPosition: 0, comparisonType: 'EQUAL', value: 50 }])).toEqual([rid(0, 5)]);
    });

    it('bulk load with unique=true rejects duplicates', () => {
      const entries = [
        { key: [1] as IndexKey, rowId: rid(0, 0) },
        { key: [1] as IndexKey, rowId: rid(0, 1) },
      ];
      expect(() => im.bulkLoad('idx1', entries, true)).toThrow('UNIQUE constraint failed');
    });
  });

  describe('dropIndex', () => {
    it('drop removes all index data', () => {
      im.insert('idx1', [1], rid(0, 0));
      im.insert('idx1', [2], rid(0, 1));
      pm.commit();

      im.dropIndex('idx1');
      pm.commit();

      expect(im.search('idx1', [{ columnPosition: 0, comparisonType: 'EQUAL', value: 1 }])).toEqual([]);
    });

    it('drop one index does not affect another', () => {
      im.insert('idx_a', [1], rid(0, 0));
      im.insert('idx_b', [1], rid(0, 1));
      pm.commit();

      im.dropIndex('idx_a');
      pm.commit();

      expect(im.search('idx_a', [{ columnPosition: 0, comparisonType: 'EQUAL', value: 1 }])).toEqual([]);
      expect(im.search('idx_b', [{ columnPosition: 0, comparisonType: 'EQUAL', value: 1 }])).toEqual([rid(0, 1)]);
    });
  });

  describe('search with totalColumns', () => {
    it('prefix scan with totalColumns', () => {
      im.insert('idx1', ['a', 1], rid(0, 0));
      im.insert('idx1', ['a', 2], rid(0, 1));
      im.insert('idx1', ['b', 1], rid(0, 2));
      pm.commit();

      const results = im.search('idx1', [{ columnPosition: 0, comparisonType: 'EQUAL', value: 'a' }], 2);
      expect(results).toHaveLength(2);
      expect(results).toContainEqual(rid(0, 0));
      expect(results).toContainEqual(rid(0, 1));
    });

    it('point lookup with all columns covered', () => {
      im.insert('idx1', ['a', 1], rid(0, 0));
      im.insert('idx1', ['a', 2], rid(0, 1));
      pm.commit();

      expect(im.search('idx1', [
        { columnPosition: 0, comparisonType: 'EQUAL', value: 'a' },
        { columnPosition: 1, comparisonType: 'EQUAL', value: 1 },
      ], 2)).toEqual([rid(0, 0)]);
    });
  });

  describe('case insensitivity', () => {
    it('index names are lowercased', () => {
      im.insert('MyIndex', [1], rid(0, 0));
      pm.commit();

      expect(im.search('myindex', [{ columnPosition: 0, comparisonType: 'EQUAL', value: 1 }])).toEqual([rid(0, 0)]);
    });
  });
});
