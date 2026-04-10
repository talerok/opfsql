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

    it('assigns sequential slotIds', async () => {
      const r1 = await pm.prepareInsert('t1', { id: 1 });
      const r2 = await pm.prepareInsert('t1', { id: 2 });
      expect(r1.slotId).toBe(0);
      expect(r2.slotId).toBe(1);
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
    beforeEach(async () => {
      await pm.prepareInsert('t1', { id: 1, val: 'old' });
      await commitAndReset();
    });

    it('replaces row data', async () => {
      await pm.prepareUpdate('t1', { pageId: 0, slotId: 0 }, { id: 1, val: 'new' });
      const rows = await collectScan('t1');
      expect(rows).toHaveLength(1);
      expect(rows[0].row).toEqual({ id: 1, val: 'new' });
    });

    it('increments deadRowCount', async () => {
      await pm.prepareUpdate('t1', { pageId: 0, slotId: 0 }, { id: 1, val: 'new' });
      const meta = await pm.getPageMeta('t1');
      expect(meta.deadRowCount).toBe(1);
    });

    it('old rowId returns null via readRow', async () => {
      await pm.prepareUpdate('t1', { pageId: 0, slotId: 0 }, { id: 1, val: 'new' });
      const old = await pm.readRow('t1', { pageId: 0, slotId: 0 });
      expect(old).toBeNull();
    });
  });

  // -- prepareDelete --
  describe('prepareDelete', () => {
    beforeEach(async () => {
      await pm.prepareInsert('t1', { id: 1 });
      await commitAndReset();
    });

    it('removes row from scan', async () => {
      await pm.prepareDelete('t1', { pageId: 0, slotId: 0 });
      expect(await collectScan('t1')).toHaveLength(0);
    });

    it('increments deadRowCount', async () => {
      await pm.prepareDelete('t1', { pageId: 0, slotId: 0 });
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
      await pm.prepareInsert('t1', { id: 1 });
      await pm.prepareInsert('t1', { id: 2 });
      const rows = await collectScan('t1');
      expect(rows).toHaveLength(2);
      expect(rows[0].row).toEqual({ id: 1 });
      expect(rows[1].row).toEqual({ id: 2 });
    });

    it('skips deleted rows', async () => {
      await pm.prepareInsert('t1', { id: 1 });
      await pm.prepareInsert('t1', { id: 2 });
      await pm.prepareDelete('t1', { pageId: 0, slotId: 0 });
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

    it('returns null for non-existent page', async () => {
      expect(await pm.readRow('t1', { pageId: 99, slotId: 0 })).toBeNull();
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
        await pm.prepareUpdate('t1', rowId, { id: rowId.slotId, val: 'new' });
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
      await pm.prepareInsert('t1', { id: 1, val: 'cached' });
      await pm.commit();

      // Read after commit — should come from cache
      const row = await pm.readRow('t1', { pageId: 0, slotId: 0 });
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
});
