import { describe, it, expect, beforeEach } from 'vitest';
import { SyncTableBTree } from '../table-btree.js';
import { SyncPageManager } from '../page-manager.js';
import { MemoryStorage } from '../memory-storage.js';

describe('SyncTableBTree', () => {
  let storage: MemoryStorage;
  let pm: SyncPageManager;
  let tree: SyncTableBTree;

  beforeEach(() => {
    storage = new MemoryStorage();
    pm = new SyncPageManager(storage);
    tree = new SyncTableBTree('t1', pm);
  });

  function collectScan() {
    const rows = [];
    for (const r of tree.scan()) rows.push(r);
    return rows;
  }

  describe('insert & get', () => {
    it('inserts and reads back', () => {
      const id = tree.insert({ name: 'Alice' });
      expect(id).toBe(0);
      expect(tree.get(id)).toEqual({ name: 'Alice' });
    });

    it('sequential rowIds', () => {
      const id1 = tree.insert({ a: 1 });
      const id2 = tree.insert({ a: 2 });
      const id3 = tree.insert({ a: 3 });
      expect(id1).toBe(0);
      expect(id2).toBe(1);
      expect(id3).toBe(2);
    });

    it('get returns null for missing', () => {
      expect(tree.get(99)).toBeNull();
    });

    it('survives commit + reload', () => {
      const id = tree.insert({ val: 'persisted' });
      pm.commit();

      const tree2 = new SyncTableBTree('t1', pm);
      expect(tree2.get(id)).toEqual({ val: 'persisted' });
    });
  });

  describe('update', () => {
    it('replaces row data', () => {
      const id = tree.insert({ val: 'old' });
      tree.update(id, { val: 'new' });
      expect(tree.get(id)).toEqual({ val: 'new' });
    });

    it('throws for missing row', () => {
      expect(() => tree.update(99, { val: 'x' })).toThrow('not found');
    });
  });

  describe('delete', () => {
    it('removes row', () => {
      const id = tree.insert({ val: 1 });
      tree.delete(id);
      expect(tree.get(id)).toBeNull();
    });

    it('no-op for missing', () => {
      tree.delete(99); // should not throw
    });

    it('decrements size', () => {
      tree.insert({ a: 1 });
      tree.insert({ a: 2 });
      tree.delete(0);
      expect(collectScan()).toHaveLength(1);
    });
  });

  describe('scan', () => {
    it('empty tree yields nothing', () => {
      expect(collectScan()).toHaveLength(0);
    });

    it('yields all rows in rowId order', () => {
      tree.insert({ a: 1 });
      tree.insert({ a: 2 });
      tree.insert({ a: 3 });
      const rows = collectScan();
      expect(rows).toHaveLength(3);
      expect(rows.map((r) => r.rowId)).toEqual([0, 1, 2]);
      expect(rows.map((r) => r.row.a)).toEqual([1, 2, 3]);
    });

    it('skips deleted rows', () => {
      tree.insert({ a: 1 });
      tree.insert({ a: 2 });
      tree.insert({ a: 3 });
      tree.delete(1);
      const rows = collectScan();
      expect(rows).toHaveLength(2);
      expect(rows.map((r) => r.rowId)).toEqual([0, 2]);
    });
  });

  describe('split', () => {
    it('handles more than ORDER inserts', () => {
      const N = 1100; // > ORDER=128
      for (let i = 0; i < N; i++) tree.insert({ id: i });

      for (let i = 0; i < N; i++) {
        expect(tree.get(i)).toEqual({ id: i });
      }

      expect(collectScan()).toHaveLength(N);
    });
  });

  describe('drop', () => {
    it('removes all data', () => {
      tree.insert({ a: 1 });
      tree.insert({ a: 2 });
      pm.commit();

      tree.drop();
      pm.commit();

      const tree2 = new SyncTableBTree('t1', pm);
      expect(tree2.get(0)).toBeNull();
      const rows = [];
      for (const r of tree2.scan()) rows.push(r);
      expect(rows).toHaveLength(0);
    });
  });
});
