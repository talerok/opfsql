import { describe, it, expect, beforeEach } from 'vitest';
import { PageManager } from '../page-manager.js';
import { RowManager } from '../row-manager.js';
import { Vacuum } from '../vacuum.js';
import { MemoryStorage } from './memory-storage.js';
import { PAGE_SIZE } from '../types.js';

describe('Vacuum', () => {
  let storage: MemoryStorage;
  let pm: PageManager;
  let rm: RowManager;
  let vacuum: Vacuum;

  beforeEach(() => {
    storage = new MemoryStorage();
    pm = new PageManager(storage);
    rm = new RowManager(pm);
    vacuum = new Vacuum(storage, pm);
  });

  async function collectScan(tableId: string) {
    const rows = [];
    for await (const r of rm.scanTable(tableId)) rows.push(r);
    return rows;
  }

  // -- shouldVacuum --
  describe('shouldVacuum', () => {
    it('returns false for empty table', async () => {
      expect(await vacuum.shouldVacuum('t1')).toBe(false);
    });

    it('returns false when no dead rows', async () => {
      await rm.prepareInsert('t1', { id: 1 });
      await rm.prepareInsert('t1', { id: 2 });
      await pm.commit();

      expect(await vacuum.shouldVacuum('t1')).toBe(false);
    });

    it('returns false when dead ratio <= 0.3', async () => {
      for (let i = 0; i < 10; i++) {
        await rm.prepareInsert('t1', { id: i });
      }
      await pm.commit();

      // Delete 3 out of 10 = 30% — threshold is >, not >=
      for (let i = 0; i < 3; i++) {
        await rm.prepareDelete('t1', { pageId: 0, slotId: i });
      }
      await pm.commit();

      expect(await vacuum.shouldVacuum('t1')).toBe(false);
    });

    it('returns true when dead ratio > 0.3', async () => {
      for (let i = 0; i < 10; i++) {
        await rm.prepareInsert('t1', { id: i });
      }
      await pm.commit();

      // Delete 4 out of 10 = 40%
      for (let i = 0; i < 4; i++) {
        await rm.prepareDelete('t1', { pageId: 0, slotId: i });
      }
      await pm.commit();

      expect(await vacuum.shouldVacuum('t1')).toBe(true);
    });
  });

  // -- vacuumTable --
  describe('vacuumTable', () => {
    it('compacts a table by removing dead rows', async () => {
      for (let i = 0; i < 5; i++) {
        await rm.prepareInsert('t1', { id: i });
      }
      await pm.commit();

      // Delete rows 0, 1, 2
      for (let i = 0; i < 3; i++) {
        await rm.prepareDelete('t1', { pageId: 0, slotId: i });
      }
      await pm.commit();

      await vacuum.vacuumTable('t1');

      // Re-create PM/RM to read from storage directly (vacuum writes to storage, not WAL)
      const pm2 = new PageManager(storage);
      const rm2 = new RowManager(pm2);
      const rows = [];
      for await (const r of rm2.scanTable('t1')) rows.push(r);

      expect(rows).toHaveLength(2);
      expect(rows[0].row).toEqual({ id: 3 });
      expect(rows[1].row).toEqual({ id: 4 });
    });

    it('resets meta after vacuum', async () => {
      for (let i = 0; i < 5; i++) {
        await rm.prepareInsert('t1', { id: i });
      }
      await pm.commit();

      for (let i = 0; i < 3; i++) {
        await rm.prepareDelete('t1', { pageId: 0, slotId: i });
      }
      await pm.commit();

      await vacuum.vacuumTable('t1');

      const pm2 = new PageManager(storage);
      const meta = await pm2.getPageMeta('t1');
      expect(meta.totalRowCount).toBe(2);
      expect(meta.deadRowCount).toBe(0);
      expect(meta.lastPageId).toBe(0);
    });

    it('handles all rows deleted', async () => {
      for (let i = 0; i < 3; i++) {
        await rm.prepareInsert('t1', { id: i });
      }
      await pm.commit();

      for (let i = 0; i < 3; i++) {
        await rm.prepareDelete('t1', { pageId: 0, slotId: i });
      }
      await pm.commit();

      await vacuum.vacuumTable('t1');

      const pm2 = new PageManager(storage);
      const meta = await pm2.getPageMeta('t1');
      expect(meta.totalRowCount).toBe(0);
      expect(meta.deadRowCount).toBe(0);
      expect(meta.lastPageId).toBe(-1);
    });

    it('reassigns slotIds after compaction', async () => {
      for (let i = 0; i < 5; i++) {
        await rm.prepareInsert('t1', { id: i });
      }
      await pm.commit();

      // Delete slot 1 and 3 (non-contiguous)
      await rm.prepareDelete('t1', { pageId: 0, slotId: 1 });
      await rm.prepareDelete('t1', { pageId: 0, slotId: 3 });
      await pm.commit();

      await vacuum.vacuumTable('t1');

      const pm2 = new PageManager(storage);
      const page = await pm2.readPage('t1', 0);
      expect(page).not.toBeNull();
      expect(page!.rows).toHaveLength(3);
      expect(page!.rows.map((r) => r.slotId)).toEqual([0, 1, 2]);
      expect(page!.rows.map((r) => r.data.id)).toEqual([0, 2, 4]);
    });
  });
});
