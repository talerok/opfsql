import { describe, it, expect, beforeEach } from 'vitest';
import { TableManager } from '../table-manager.js';
import { PageManager } from '../page-manager.js';
import { MemoryStorage } from './memory-storage.js';

describe('TableManager (row operations via TableBTree)', () => {
  let storage: MemoryStorage;
  let kv: PageManager;
  let rm: TableManager;

  beforeEach(() => {
    storage = new MemoryStorage();
    kv = new PageManager(storage);
    rm = new TableManager(kv);
  });

  async function commitAndReset() {
    await kv.commit();
  }

  async function collectScan(tableId: string) {
    const rows = [];
    for await (const r of rm.scanTable(tableId)) rows.push(r);
    return rows;
  }

  // -- prepareInsert --
  describe('prepareInsert', () => {
    it('inserts and reads back via scanTable', async () => {
      await rm.prepareInsert('t1', { id: 1 });
      const rows = await collectScan('t1');
      expect(rows).toHaveLength(1);
      expect(rows[0].row).toEqual({ id: 1 });
    });

    it('assigns sequential rowIds', async () => {
      const r1 = await rm.prepareInsert('t1', { id: 1 });
      const r2 = await rm.prepareInsert('t1', { id: 2 });
      expect(r1).toBe(0);
      expect(r2).toBe(1);
    });

    it('inserts many rows (triggers B-tree splits)', async () => {
      for (let i = 0; i < 2000; i++) {
        await rm.prepareInsert('t1', { id: i });
      }
      const rows = await collectScan('t1');
      expect(rows).toHaveLength(2000);
    });
  });

  // -- prepareUpdate --
  describe('prepareUpdate', () => {
    let rowId: number;

    beforeEach(async () => {
      rowId = await rm.prepareInsert('t1', { id: 1, val: 'old' });
      await commitAndReset();
    });

    it('replaces row data', async () => {
      await rm.prepareUpdate('t1', rowId, { id: 1, val: 'new' });
      const rows = await collectScan('t1');
      expect(rows).toHaveLength(1);
      expect(rows[0].row).toEqual({ id: 1, val: 'new' });
    });

    it('returns same rowId (in-place update)', async () => {
      const newRowId = await rm.prepareUpdate('t1', rowId, { id: 1, val: 'new' });
      expect(newRowId).toBe(rowId);
    });

    it('old rowId still reads updated data', async () => {
      await rm.prepareUpdate('t1', rowId, { id: 1, val: 'new' });
      const row = await rm.readRow('t1', rowId);
      expect(row).toEqual({ id: 1, val: 'new' });
    });
  });

  // -- prepareDelete --
  describe('prepareDelete', () => {
    let rowId: number;

    beforeEach(async () => {
      rowId = await rm.prepareInsert('t1', { id: 1 });
      await commitAndReset();
    });

    it('removes row from scan', async () => {
      await rm.prepareDelete('t1', rowId);
      expect(await collectScan('t1')).toHaveLength(0);
    });
  });

  // -- scanTable --
  describe('scanTable', () => {
    it('empty table yields nothing', async () => {
      expect(await collectScan('t1')).toHaveLength(0);
    });

    it('yields inserted rows in rowId order', async () => {
      await rm.prepareInsert('t1', { id: 1 });
      await rm.prepareInsert('t1', { id: 2 });
      const rows = await collectScan('t1');
      expect(rows).toHaveLength(2);
      expect(rows[0].row).toEqual({ id: 1 });
      expect(rows[1].row).toEqual({ id: 2 });
    });

    it('skips deleted rows', async () => {
      const r1 = await rm.prepareInsert('t1', { id: 1 });
      await rm.prepareInsert('t1', { id: 2 });
      await rm.prepareDelete('t1', r1);
      const rows = await collectScan('t1');
      expect(rows).toHaveLength(1);
      expect(rows[0].row).toEqual({ id: 2 });
    });
  });

  // -- readRow --
  describe('readRow', () => {
    it('reads inserted row by rowId', async () => {
      const rowId = await rm.prepareInsert('t1', { id: 1, val: 'hello' });
      const row = await rm.readRow('t1', rowId);
      expect(row).toEqual({ id: 1, val: 'hello' });
    });

    it('returns null for deleted row', async () => {
      const rowId = await rm.prepareInsert('t1', { id: 1 });
      await rm.prepareDelete('t1', rowId);
      expect(await rm.readRow('t1', rowId)).toBeNull();
    });

    it('returns null for non-existent row', async () => {
      expect(await rm.readRow('t1', 99)).toBeNull();
    });
  });

  // -- batch DML via WAL --
  describe('batch DML via WAL', () => {
    it('multiple deletes all persist after commit', async () => {
      for (let i = 0; i < 3; i++) {
        await rm.prepareInsert('t1', { id: i });
      }
      await commitAndReset();

      const rows = await collectScan('t1');
      for (const { rowId } of rows) {
        await rm.prepareDelete('t1', rowId);
      }
      await commitAndReset();

      expect(await collectScan('t1')).toHaveLength(0);
    });

    it('multiple updates all persist after commit', async () => {
      for (let i = 0; i < 3; i++) {
        await rm.prepareInsert('t1', { id: i, val: 'old' });
      }
      await commitAndReset();

      const rows = await collectScan('t1');
      for (const { rowId } of rows) {
        await rm.prepareUpdate('t1', rowId, { id: rowId, val: 'new' });
      }
      await commitAndReset();

      const updated = await collectScan('t1');
      expect(updated).toHaveLength(3);
      expect(updated.every((r) => r.row.val === 'new')).toBe(true);
    });
  });

  // -- persistence --
  describe('persistence', () => {
    it('data survives commit and reload', async () => {
      const rowId = await rm.prepareInsert('t1', { id: 1, val: 'persisted' });
      await kv.commit();

      const kv2 = new PageManager(storage);
      const rm2 = new TableManager(kv2);
      const row = await rm2.readRow('t1', rowId);
      expect(row).toEqual({ id: 1, val: 'persisted' });
    });

    it('rollback discards uncommitted data', async () => {
      await rm.prepareInsert('t1', { id: 1 });
      await kv.commit();

      await rm.prepareInsert('t1', { id: 2 });
      kv.rollback();

      const rows = await collectScan('t1');
      expect(rows).toHaveLength(1);
      expect(rows[0].row).toEqual({ id: 1 });
    });
  });

  // -- deleteTableData --
  describe('deleteTableData', () => {
    it('removes all table data', async () => {
      await rm.prepareInsert('t1', { id: 1 });
      await rm.prepareInsert('t1', { id: 2 });
      await kv.commit();

      await rm.deleteTableData('t1');
      await kv.commit();

      expect(await collectScan('t1')).toHaveLength(0);
    });
  });
});
