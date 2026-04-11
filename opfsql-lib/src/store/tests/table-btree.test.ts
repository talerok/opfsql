import { describe, it, expect, beforeEach } from 'vitest';
import { TableBTree } from '../btree/table-btree.js';
import { PageManager } from '../page-manager.js';
import { MemoryStorage } from './memory-storage.js';

describe('TableBTree', () => {
  let storage: MemoryStorage;
  let pm: PageManager;
  let tree: TableBTree;

  beforeEach(() => {
    storage = new MemoryStorage();
    pm = new PageManager(storage);
    tree = new TableBTree('t1', pm);
  });

  async function commit() {
    await pm.commit();
  }

  async function collectScan() {
    const rows = [];
    for await (const r of tree.scan()) rows.push(r);
    return rows;
  }

  // -- insert + get --
  describe('insert & get', () => {
    it('inserts and reads back', async () => {
      const id = await tree.insert({ name: 'Alice' });
      expect(id).toBe(0);
      const row = await tree.get(id);
      expect(row).toEqual({ name: 'Alice' });
    });

    it('sequential rowIds', async () => {
      const id1 = await tree.insert({ a: 1 });
      const id2 = await tree.insert({ a: 2 });
      const id3 = await tree.insert({ a: 3 });
      expect(id1).toBe(0);
      expect(id2).toBe(1);
      expect(id3).toBe(2);
    });

    it('get returns null for missing', async () => {
      expect(await tree.get(99)).toBeNull();
    });

    it('survives commit + reload', async () => {
      const id = await tree.insert({ val: 'persisted' });
      await commit();

      const tree2 = new TableBTree('t1', pm);
      expect(await tree2.get(id)).toEqual({ val: 'persisted' });
    });
  });

  // -- update --
  describe('update', () => {
    it('replaces row data', async () => {
      const id = await tree.insert({ val: 'old' });
      await tree.update(id, { val: 'new' });
      expect(await tree.get(id)).toEqual({ val: 'new' });
    });

    it('throws for missing row', async () => {
      await expect(tree.update(99, { val: 'x' })).rejects.toThrow('not found');
    });
  });

  // -- delete --
  describe('delete', () => {
    it('removes row', async () => {
      const id = await tree.insert({ val: 1 });
      await tree.delete(id);
      expect(await tree.get(id)).toBeNull();
    });

    it('no-op for missing', async () => {
      await tree.delete(99); // should not throw
    });

    it('decrements size', async () => {
      await tree.insert({ a: 1 });
      await tree.insert({ a: 2 });
      await tree.delete(0);
      const rows = await collectScan();
      expect(rows).toHaveLength(1);
    });
  });

  // -- scan --
  describe('scan', () => {
    it('empty tree yields nothing', async () => {
      expect(await collectScan()).toHaveLength(0);
    });

    it('yields all rows in rowId order', async () => {
      await tree.insert({ a: 1 });
      await tree.insert({ a: 2 });
      await tree.insert({ a: 3 });
      const rows = await collectScan();
      expect(rows).toHaveLength(3);
      expect(rows.map(r => r.rowId)).toEqual([0, 1, 2]);
      expect(rows.map(r => r.row.a)).toEqual([1, 2, 3]);
    });

    it('skips deleted rows', async () => {
      await tree.insert({ a: 1 });
      await tree.insert({ a: 2 });
      await tree.insert({ a: 3 });
      await tree.delete(1);
      const rows = await collectScan();
      expect(rows).toHaveLength(2);
      expect(rows.map(r => r.rowId)).toEqual([0, 2]);
    });
  });

  // -- split --
  describe('split', () => {
    it('handles more than ORDER inserts', async () => {
      const N = 1100; // > ORDER=1024
      for (let i = 0; i < N; i++) {
        await tree.insert({ id: i });
      }

      // All rows readable
      for (let i = 0; i < N; i++) {
        expect(await tree.get(i)).toEqual({ id: i });
      }

      // Scan returns all
      const rows = await collectScan();
      expect(rows).toHaveLength(N);
    });
  });

  // -- drop --
  describe('drop', () => {
    it('removes all data', async () => {
      await tree.insert({ a: 1 });
      await tree.insert({ a: 2 });
      await commit();

      await tree.drop();
      await commit();

      const tree2 = new TableBTree('t1', pm);
      expect(await tree2.get(0)).toBeNull();
      expect(await collectScan()).toHaveLength(0);
    });
  });
});
