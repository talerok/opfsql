import { describe, it, expect, beforeEach } from 'vitest';
import { PageManager } from '../page-manager.js';
import { MemoryStorage } from './memory-storage.js';
import { PAGE_SIZE } from '../types.js';

describe('PageManager row operations', () => {
  let storage: MemoryStorage;
  let pm: PageManager;

  beforeEach(() => {
    storage = new MemoryStorage();
    pm = new PageManager(storage);
  });

  async function commitAndReset() {
    await pm.commit();
  }

  async function collectScan(tableId: string) {
    const rows = [];
    for await (const r of pm.scanTable(tableId)) rows.push(r);
    return rows;
  }

  // -- prepareInsert --
  describe('prepareInsert', () => {
    it('inserts and reads back via scanTable', async () => {
      await pm.prepareInsert('t1', { id: 1 });
      const rows = await collectScan('t1');
      expect(rows).toHaveLength(1);
      expect(rows[0].row).toEqual({ id: 1 });
    });

    it('increments totalRowCount', async () => {
      await pm.prepareInsert('t1', { id: 1 });
      await pm.prepareInsert('t1', { id: 2 });
      const meta = await pm.getPageMeta('t1');
      expect(meta.totalRowCount).toBe(2);
    });

    it('assigns sequential rowIds', async () => {
      const r1 = await pm.prepareInsert('t1', { id: 1 });
      const r2 = await pm.prepareInsert('t1', { id: 2 });
      expect(r1).toBe(0);
      expect(r2).toBe(1);
    });

    it('creates new page when PAGE_SIZE exceeded', async () => {
      for (let i = 0; i < PAGE_SIZE + 1; i++) {
        await pm.prepareInsert('t1', { id: i });
      }
      const meta = await pm.getPageMeta('t1');
      expect(meta.lastPageId).toBe(1);
    });
  });

  // -- prepareUpdate --
  describe('prepareUpdate', () => {
    let rowId: number;

    beforeEach(async () => {
      rowId = await pm.prepareInsert('t1', { id: 1, val: 'old' });
      await commitAndReset();
    });

    it('replaces row data', async () => {
      await pm.prepareUpdate('t1', rowId, { id: 1, val: 'new' });
      const rows = await collectScan('t1');
      expect(rows).toHaveLength(1);
      expect(rows[0].row).toEqual({ id: 1, val: 'new' });
    });

    it('returns same rowId (in-place update)', async () => {
      const newRowId = await pm.prepareUpdate('t1', rowId, { id: 1, val: 'new' });
      expect(newRowId).toBe(rowId);
    });

    it('old rowId still reads updated data', async () => {
      await pm.prepareUpdate('t1', rowId, { id: 1, val: 'new' });
      const row = await pm.readRow('t1', rowId);
      expect(row).toEqual({ id: 1, val: 'new' });
    });
  });

  // -- prepareDelete --
  describe('prepareDelete', () => {
    let rowId: number;

    beforeEach(async () => {
      rowId = await pm.prepareInsert('t1', { id: 1 });
      await commitAndReset();
    });

    it('removes row from scan', async () => {
      await pm.prepareDelete('t1', rowId);
      expect(await collectScan('t1')).toHaveLength(0);
    });

    it('decrements totalRowCount', async () => {
      await pm.prepareDelete('t1', rowId);
      const meta = await pm.getPageMeta('t1');
      expect(meta.totalRowCount).toBe(0);
    });
  });

  // -- scanTable --
  describe('scanTable', () => {
    it('empty table yields nothing', async () => {
      expect(await collectScan('t1')).toHaveLength(0);
    });

    it('yields inserted rows', async () => {
      await pm.prepareInsert('t1', { id: 1 });
      await pm.prepareInsert('t1', { id: 2 });
      const rows = await collectScan('t1');
      expect(rows).toHaveLength(2);
      expect(rows[0].row).toEqual({ id: 1 });
      expect(rows[1].row).toEqual({ id: 2 });
    });

    it('skips deleted rows', async () => {
      const r1 = await pm.prepareInsert('t1', { id: 1 });
      await pm.prepareInsert('t1', { id: 2 });
      await pm.prepareDelete('t1', r1);
      const rows = await collectScan('t1');
      expect(rows).toHaveLength(1);
      expect(rows[0].row).toEqual({ id: 2 });
    });

    it('traverses multiple pages', async () => {
      for (let i = 0; i <= PAGE_SIZE; i++) {
        await pm.prepareInsert('t1', { id: i });
      }
      const rows = await collectScan('t1');
      expect(rows).toHaveLength(PAGE_SIZE + 1);
    });
  });

  // -- readRow --
  describe('readRow', () => {
    it('reads inserted row by rowId', async () => {
      const rowId = await pm.prepareInsert('t1', { id: 1, val: 'hello' });
      const row = await pm.readRow('t1', rowId);
      expect(row).toEqual({ id: 1, val: 'hello' });
    });

    it('returns null for deleted row', async () => {
      const rowId = await pm.prepareInsert('t1', { id: 1 });
      await pm.prepareDelete('t1', rowId);
      expect(await pm.readRow('t1', rowId)).toBeNull();
    });

    it('returns null for non-existent row', async () => {
      expect(await pm.readRow('t1', 99)).toBeNull();
    });
  });

  // -- batch DML via WAL --
  describe('batch DML via WAL', () => {
    it('multiple deletes on same page all persist after commit', async () => {
      for (let i = 0; i < 3; i++) {
        await pm.prepareInsert('t1', { id: i });
      }
      await commitAndReset();

      const rows = await collectScan('t1');
      for (const { rowId } of rows) {
        await pm.prepareDelete('t1', rowId);
      }
      await commitAndReset();

      expect(await collectScan('t1')).toHaveLength(0);
    });

    it('multiple updates on same page all persist after commit', async () => {
      for (let i = 0; i < 3; i++) {
        await pm.prepareInsert('t1', { id: i, val: 'old' });
      }
      await commitAndReset();

      const rows = await collectScan('t1');
      for (const { rowId } of rows) {
        await pm.prepareUpdate('t1', rowId, { id: rowId, val: 'new' });
      }
      await commitAndReset();

      const updated = await collectScan('t1');
      expect(updated).toHaveLength(3);
      expect(updated.every((r) => r.row.val === 'new')).toBe(true);
    });
  });

  // -- buffer pool --
  describe('buffer pool', () => {
    it('data survives commit in cache (no storage re-read)', async () => {
      const rowId = await pm.prepareInsert('t1', { id: 1, val: 'cached' });
      await pm.commit();

      const row = await pm.readRow('t1', rowId);
      expect(row).toEqual({ id: 1, val: 'cached' });
    });

    it('rollback discards uncommitted data but cache remains', async () => {
      await pm.prepareInsert('t1', { id: 1 });
      await pm.commit();

      await pm.prepareInsert('t1', { id: 2 });
      pm.rollback();

      const rows = await collectScan('t1');
      expect(rows).toHaveLength(1);
      expect(rows[0].row).toEqual({ id: 1 });
    });
  });

  // -- freePageIds --
  describe('freePageIds reuse', () => {
    it('reuses freed page after all rows deleted', async () => {
      // Fill pages 0 and 1 fully
      for (let i = 0; i < PAGE_SIZE * 2; i++) {
        await pm.prepareInsert('t1', { id: i });
      }
      await commitAndReset();

      // Delete all rows from page 0 (rowIds 0..PAGE_SIZE-1)
      for (let i = 0; i < PAGE_SIZE; i++) {
        await pm.prepareDelete('t1', i);
      }
      await commitAndReset();

      const meta = await pm.getPageMeta('t1');
      expect(meta.freePageIds).toContain(0);

      // lastPage (page 1) is full, so next insert must reuse page 0
      await pm.prepareInsert('t1', { id: 9999 });
      const metaAfter = await pm.getPageMeta('t1');
      expect(metaAfter.freePageIds).not.toContain(0);
    });
  });

  // -- row map persistence --
  describe('row map persistence', () => {
    it('row map survives commit and reload', async () => {
      const rowId = await pm.prepareInsert('t1', { id: 1, val: 'persisted' });
      await pm.commit();

      const pm2 = new PageManager(storage);
      const row = await pm2.readRow('t1', rowId);
      expect(row).toEqual({ id: 1, val: 'persisted' });
    });
  });
});
