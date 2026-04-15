import { beforeEach, describe, expect, it } from 'vitest';
import type { IndexKey } from '../index-btree/types.js';
import { SyncIndexManager } from '../index-manager.js';
import { SyncPageStore } from '../page-manager.js';
import type { ICatalog, IndexDef, RowId } from '../types.js';
import { MemoryPageStorage } from '../backend/memory-storage.js';
import { Catalog } from '../catalog.js';

function rid(a: number, b: number): RowId {
  return a * 1000 + b;
}

function createStore(): SyncPageStore {
  const s = new MemoryPageStorage();
  return new SyncPageStore(s, s.getNextPageId(), s.readPage<number[]>(2) ?? []);
}

describe('SyncIndexManager', () => {
  let ps: SyncPageStore;
  let catalog: Catalog;
  let im: SyncIndexManager;

  beforeEach(() => {
    ps = createStore();
    catalog = new Catalog();
    im = new SyncIndexManager(ps, () => catalog);

    // Create a default non-unique index 'idx1' via bulkLoad
    const metaPageNo = im.bulkLoad('idx1', [], false);
    catalog.addIndex({
      name: 'idx1',
      tableName: 'test',
      columns: ['col1'],
      unique: false,
      metaPageNo,
    });
  });

  describe('insert and search', () => {
    it('insert and find a single entry', () => {
      im.insert('idx1', [10], rid(0, 0));
      ps.commit();

      expect(im.search('idx1', [{ columnPosition: 0, comparisonType: 'EQUAL', value: 10 }])).toEqual([rid(0, 0)]);
    });

    it('insert with unique=true enforces uniqueness', () => {
      const metaPageNo = im.bulkLoad('idx_unique', [], true);
      catalog.addIndex({
        name: 'idx_unique',
        tableName: 'test',
        columns: ['col1'],
        unique: true,
        metaPageNo,
      });

      im.insert('idx_unique', [1], rid(0, 0));
      expect(() => im.insert('idx_unique', [1], rid(0, 1))).toThrow('UNIQUE constraint failed');
    });

    it('different indexes are independent', () => {
      // Create second index
      const metaPageNo = im.bulkLoad('idx_b', [], false);
      catalog.addIndex({
        name: 'idx_b',
        tableName: 'test',
        columns: ['col1'],
        unique: false,
        metaPageNo,
      });

      im.insert('idx1', [1], rid(0, 0));
      im.insert('idx_b', [1], rid(0, 1));
      ps.commit();

      expect(im.search('idx1', [{ columnPosition: 0, comparisonType: 'EQUAL', value: 1 }])).toEqual([rid(0, 0)]);
      expect(im.search('idx_b', [{ columnPosition: 0, comparisonType: 'EQUAL', value: 1 }])).toEqual([rid(0, 1)]);
    });
  });

  describe('delete', () => {
    it('delete removes entry from index', () => {
      im.insert('idx1', [5], rid(0, 0));
      ps.commit();

      im.delete('idx1', [5], rid(0, 0));
      ps.commit();

      expect(im.search('idx1', [{ columnPosition: 0, comparisonType: 'EQUAL', value: 5 }])).toEqual([]);
    });
  });

  describe('bulkLoad', () => {
    it('bulk load creates searchable index', () => {
      const entries = Array.from({ length: 20 }, (_, i) => ({ key: [i * 10] as IndexKey, rowId: rid(0, i) }));
      const metaPageNo = im.bulkLoad('idx_bulk', entries, false);
      catalog.addIndex({
        name: 'idx_bulk',
        tableName: 'test',
        columns: ['col1'],
        unique: false,
        metaPageNo,
      });
      ps.commit();

      expect(im.search('idx_bulk', [{ columnPosition: 0, comparisonType: 'EQUAL', value: 50 }])).toEqual([rid(0, 5)]);
    });

    it('bulk load with unique=true rejects duplicates', () => {
      const entries = [
        { key: [1] as IndexKey, rowId: rid(0, 0) },
        { key: [1] as IndexKey, rowId: rid(0, 1) },
      ];
      expect(() => im.bulkLoad('idx_dup', entries, true)).toThrow('UNIQUE constraint failed');
    });
  });

  describe('dropIndex', () => {
    it('drop removes all index data', () => {
      im.insert('idx1', [1], rid(0, 0));
      im.insert('idx1', [2], rid(0, 1));
      ps.commit();

      im.dropIndex('idx1');
      ps.commit();
    });

    it('drop one index does not affect another', () => {
      const metaPageNo = im.bulkLoad('idx_b', [], false);
      catalog.addIndex({
        name: 'idx_b',
        tableName: 'test',
        columns: ['col1'],
        unique: false,
        metaPageNo,
      });

      im.insert('idx1', [1], rid(0, 0));
      im.insert('idx_b', [1], rid(0, 1));
      ps.commit();

      im.dropIndex('idx1');
      ps.commit();

      expect(im.search('idx_b', [{ columnPosition: 0, comparisonType: 'EQUAL', value: 1 }])).toEqual([rid(0, 1)]);
    });
  });

  describe('search with totalColumns', () => {
    it('prefix scan with totalColumns', () => {
      im.insert('idx1', ['a', 1], rid(0, 0));
      im.insert('idx1', ['a', 2], rid(0, 1));
      im.insert('idx1', ['b', 1], rid(0, 2));
      ps.commit();

      const results = im.search('idx1', [{ columnPosition: 0, comparisonType: 'EQUAL', value: 'a' }], 2);
      expect(results).toHaveLength(2);
      expect(results).toContainEqual(rid(0, 0));
      expect(results).toContainEqual(rid(0, 1));
    });

    it('point lookup with all columns covered', () => {
      im.insert('idx1', ['a', 1], rid(0, 0));
      im.insert('idx1', ['a', 2], rid(0, 1));
      ps.commit();

      expect(im.search('idx1', [
        { columnPosition: 0, comparisonType: 'EQUAL', value: 'a' },
        { columnPosition: 1, comparisonType: 'EQUAL', value: 1 },
      ], 2)).toEqual([rid(0, 0)]);
    });
  });

  describe('rollback — no stale cache', () => {
    it('index works correctly after rollback (no stale tree cache)', () => {
      im.insert('idx1', [1], rid(0, 0));
      im.insert('idx1', [2], rid(0, 1));
      ps.commit();

      // Start a new "transaction": insert, then rollback
      im.insert('idx1', [3], rid(0, 2));
      ps.rollback();

      // After rollback, key 3 should not exist
      expect(im.search('idx1', [{ columnPosition: 0, comparisonType: 'EQUAL', value: 3 }])).toEqual([]);

      // Existing keys should still work
      expect(im.search('idx1', [{ columnPosition: 0, comparisonType: 'EQUAL', value: 1 }])).toEqual([rid(0, 0)]);
      expect(im.search('idx1', [{ columnPosition: 0, comparisonType: 'EQUAL', value: 2 }])).toEqual([rid(0, 1)]);
    });

    it('new inserts work after rollback', () => {
      im.insert('idx1', [1], rid(0, 0));
      ps.commit();

      im.insert('idx1', [2], rid(0, 1));
      ps.rollback();

      // Should be able to insert key 2 again after rollback
      im.insert('idx1', [2], rid(0, 2));
      ps.commit();

      expect(im.search('idx1', [{ columnPosition: 0, comparisonType: 'EQUAL', value: 2 }])).toEqual([rid(0, 2)]);
    });
  });

  describe('case insensitivity', () => {
    it('index names are lowercased', () => {
      // idx1 already exists in lowercase
      im.insert('IDX1', [1], rid(0, 0));
      ps.commit();

      expect(im.search('idx1', [{ columnPosition: 0, comparisonType: 'EQUAL', value: 1 }])).toEqual([rid(0, 0)]);
    });
  });
});
