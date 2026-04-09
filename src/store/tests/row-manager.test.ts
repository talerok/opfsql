import { describe, it, expect, beforeEach } from 'vitest';
import { PageManager } from '../page-manager.js';
import { RowManager } from '../row-manager.js';
import { MemoryStorage } from './memory-storage.js';
import { PAGE_SIZE } from '../types.js';

describe('RowManager', () => {
  let storage: MemoryStorage;
  let pm: PageManager;
  let rm: RowManager;

  beforeEach(() => {
    storage = new MemoryStorage();
    pm = new PageManager(storage);
    rm = new RowManager(pm);
  });

  async function commitAndReset() {
    await pm.commit();
  }

  async function collectScan(tableId: string) {
    const rows = [];
    for await (const r of rm.scanTable(tableId)) rows.push(r);
    return rows;
  }

  // -- prepareInsert --
  describe('prepareInsert', () => {
    it('creates page 0 on first insert', async () => {
      await rm.prepareInsert('t1', { id: 1 });
      const page = await pm.readPage('t1', 0);
      expect(page).not.toBeNull();
      expect(page!.rows).toHaveLength(1);
      expect(page!.rows[0].data).toEqual({ id: 1 });
    });

    it('increments totalRowCount', async () => {
      await rm.prepareInsert('t1', { id: 1 });
      await rm.prepareInsert('t1', { id: 2 });
      const meta = await pm.getPageMeta('t1');
      expect(meta.totalRowCount).toBe(2);
    });

    it('assigns sequential slotIds', async () => {
      await rm.prepareInsert('t1', { id: 1 });
      await rm.prepareInsert('t1', { id: 2 });
      const page = await pm.readPage('t1', 0);
      expect(page!.rows[0].slotId).toBe(0);
      expect(page!.rows[1].slotId).toBe(1);
    });

    it('creates new page when PAGE_SIZE exceeded', async () => {
      for (let i = 0; i < PAGE_SIZE + 1; i++) {
        await rm.prepareInsert('t1', { id: i });
      }
      const meta = await pm.getPageMeta('t1');
      expect(meta.lastPageId).toBe(1);
      const page1 = await pm.readPage('t1', 1);
      expect(page1!.rows).toHaveLength(1);
    });
  });

  // -- prepareUpdate --
  describe('prepareUpdate', () => {
    beforeEach(async () => {
      await rm.prepareInsert('t1', { id: 1, val: 'old' });
      await commitAndReset();
    });

    it('marks old row as deleted', async () => {
      await rm.prepareUpdate('t1', { pageId: 0, slotId: 0 }, { id: 1, val: 'new' });
      const page = await pm.readPage('t1', 0);
      expect(page!.rows[0].deleted).toBe(true);
    });

    it('appends new row', async () => {
      await rm.prepareUpdate('t1', { pageId: 0, slotId: 0 }, { id: 1, val: 'new' });
      const page = await pm.readPage('t1', 0);
      expect(page!.rows[1].data).toEqual({ id: 1, val: 'new' });
      expect(page!.rows[1].deleted).toBe(false);
    });

    it('increments deadRowCount', async () => {
      await rm.prepareUpdate('t1', { pageId: 0, slotId: 0 }, { id: 1, val: 'new' });
      const meta = await pm.getPageMeta('t1');
      expect(meta.deadRowCount).toBe(1);
    });
  });

  // -- prepareDelete --
  describe('prepareDelete', () => {
    beforeEach(async () => {
      await rm.prepareInsert('t1', { id: 1 });
      await commitAndReset();
    });

    it('marks row as deleted', async () => {
      await rm.prepareDelete('t1', { pageId: 0, slotId: 0 });
      const page = await pm.readPage('t1', 0);
      expect(page!.rows[0].deleted).toBe(true);
    });

    it('increments deadRowCount', async () => {
      await rm.prepareDelete('t1', { pageId: 0, slotId: 0 });
      const meta = await pm.getPageMeta('t1');
      expect(meta.deadRowCount).toBe(1);
    });
  });

  // -- scanTable --
  describe('scanTable', () => {
    it('empty table yields nothing', async () => {
      expect(await collectScan('t1')).toHaveLength(0);
    });

    it('yields inserted rows', async () => {
      await rm.prepareInsert('t1', { id: 1 });
      await rm.prepareInsert('t1', { id: 2 });
      const rows = await collectScan('t1');
      expect(rows).toHaveLength(2);
      expect(rows[0].row).toEqual({ id: 1 });
      expect(rows[1].row).toEqual({ id: 2 });
    });

    it('skips deleted rows', async () => {
      await rm.prepareInsert('t1', { id: 1 });
      await rm.prepareInsert('t1', { id: 2 });
      await rm.prepareDelete('t1', { pageId: 0, slotId: 0 });
      const rows = await collectScan('t1');
      expect(rows).toHaveLength(1);
      expect(rows[0].row).toEqual({ id: 2 });
    });

    it('traverses multiple pages', async () => {
      for (let i = 0; i <= PAGE_SIZE; i++) {
        await rm.prepareInsert('t1', { id: i });
      }
      const rows = await collectScan('t1');
      expect(rows).toHaveLength(PAGE_SIZE + 1);
    });
  });

  // -- batch DML (the bug DmlContext was fixing) --
  describe('batch DML via WAL', () => {
    it('multiple deletes on same page all persist after commit', async () => {
      for (let i = 0; i < 3; i++) {
        await rm.prepareInsert('t1', { id: i });
      }
      await commitAndReset();

      // Delete all rows without intermediate commits
      const rows = await collectScan('t1');
      for (const { rowId } of rows) {
        await rm.prepareDelete('t1', rowId);
      }
      await commitAndReset();

      expect(await collectScan('t1')).toHaveLength(0);
    });

    it('multiple updates on same page all persist after commit', async () => {
      for (let i = 0; i < 3; i++) {
        await rm.prepareInsert('t1', { id: i, val: 'old' });
      }
      await commitAndReset();

      const rows = await collectScan('t1');
      for (const { rowId } of rows) {
        await rm.prepareUpdate('t1', rowId, { id: rowId.slotId, val: 'new' });
      }
      await commitAndReset();

      const updated = await collectScan('t1');
      expect(updated).toHaveLength(3);
      expect(updated.every((r) => r.row.val === 'new')).toBe(true);
    });
  });
});
