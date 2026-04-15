import { describe, it, expect, beforeEach } from 'vitest';
import { SyncTableManager } from '../table-manager.js';
import { SyncPageStore } from '../page-manager.js';
import { MemoryPageStorage } from '../memory-storage.js';
import { Catalog } from '../catalog.js';
import type { ICatalog, TableSchema } from '../types.js';

function createStore(storage?: MemoryPageStorage): SyncPageStore {
  const s = storage ?? new MemoryPageStorage();
  return new SyncPageStore(s, s.getNextPageId(), s.readPage<number[]>(2) ?? []);
}

describe('SyncTableManager (row operations via SyncTableBTree)', () => {
  let storage: MemoryPageStorage;
  let ps: SyncPageStore;
  let catalog: Catalog;
  let rm: SyncTableManager;

  beforeEach(() => {
    storage = new MemoryPageStorage();
    ps = createStore(storage);
    catalog = new Catalog();
    rm = new SyncTableManager(ps, () => catalog);

    // Create a table 't1' in the catalog
    const metaPageNo = rm.createTable();
    const schema: TableSchema = {
      name: 't1',
      columns: [],
      metaPageNo,
    };
    catalog.addTable(schema);
  });

  function collectScan(tableId: string) {
    const rows = [];
    for (const r of rm.scanTable(tableId)) rows.push(r);
    return rows;
  }

  describe('prepareInsert', () => {
    it('inserts and reads back via scanTable', () => {
      rm.prepareInsert('t1', { id: 1 });
      const rows = collectScan('t1');
      expect(rows).toHaveLength(1);
      expect(rows[0].row).toEqual({ id: 1 });
    });

    it('assigns sequential rowIds', () => {
      const r1 = rm.prepareInsert('t1', { id: 1 });
      const r2 = rm.prepareInsert('t1', { id: 2 });
      expect(r1).toBe(0);
      expect(r2).toBe(1);
    });

    it('inserts many rows (triggers B-tree splits)', () => {
      for (let i = 0; i < 2000; i++) rm.prepareInsert('t1', { id: i });
      expect(collectScan('t1')).toHaveLength(2000);
    });
  });

  describe('prepareUpdate', () => {
    let rowId: number;

    beforeEach(() => {
      rowId = rm.prepareInsert('t1', { id: 1, val: 'old' });
      ps.commit();
    });

    it('replaces row data', () => {
      rm.prepareUpdate('t1', rowId, { id: 1, val: 'new' });
      const rows = collectScan('t1');
      expect(rows).toHaveLength(1);
      expect(rows[0].row).toEqual({ id: 1, val: 'new' });
    });

    it('returns same rowId (in-place update)', () => {
      const newRowId = rm.prepareUpdate('t1', rowId, { id: 1, val: 'new' });
      expect(newRowId).toBe(rowId);
    });

    it('old rowId still reads updated data', () => {
      rm.prepareUpdate('t1', rowId, { id: 1, val: 'new' });
      expect(rm.readRow('t1', rowId)).toEqual({ id: 1, val: 'new' });
    });
  });

  describe('prepareDelete', () => {
    let rowId: number;

    beforeEach(() => {
      rowId = rm.prepareInsert('t1', { id: 1 });
      ps.commit();
    });

    it('removes row from scan', () => {
      rm.prepareDelete('t1', rowId);
      expect(collectScan('t1')).toHaveLength(0);
    });
  });

  describe('scanTable', () => {
    it('empty table yields nothing', () => {
      expect(collectScan('t1')).toHaveLength(0);
    });

    it('yields inserted rows in rowId order', () => {
      rm.prepareInsert('t1', { id: 1 });
      rm.prepareInsert('t1', { id: 2 });
      const rows = collectScan('t1');
      expect(rows).toHaveLength(2);
      expect(rows[0].row).toEqual({ id: 1 });
      expect(rows[1].row).toEqual({ id: 2 });
    });

    it('skips deleted rows', () => {
      const r1 = rm.prepareInsert('t1', { id: 1 });
      rm.prepareInsert('t1', { id: 2 });
      rm.prepareDelete('t1', r1);
      const rows = collectScan('t1');
      expect(rows).toHaveLength(1);
      expect(rows[0].row).toEqual({ id: 2 });
    });
  });

  describe('readRow', () => {
    it('reads inserted row by rowId', () => {
      const rowId = rm.prepareInsert('t1', { id: 1, val: 'hello' });
      expect(rm.readRow('t1', rowId)).toEqual({ id: 1, val: 'hello' });
    });

    it('returns null for deleted row', () => {
      const rowId = rm.prepareInsert('t1', { id: 1 });
      rm.prepareDelete('t1', rowId);
      expect(rm.readRow('t1', rowId)).toBeNull();
    });

    it('returns null for non-existent row', () => {
      expect(rm.readRow('t1', 99)).toBeNull();
    });
  });

  describe('batch DML via WAL', () => {
    it('multiple deletes all persist after commit', () => {
      for (let i = 0; i < 3; i++) rm.prepareInsert('t1', { id: i });
      ps.commit();

      for (const { rowId } of collectScan('t1')) rm.prepareDelete('t1', rowId);
      ps.commit();

      expect(collectScan('t1')).toHaveLength(0);
    });

    it('multiple updates all persist after commit', () => {
      for (let i = 0; i < 3; i++) rm.prepareInsert('t1', { id: i, val: 'old' });
      ps.commit();

      for (const { rowId } of collectScan('t1')) rm.prepareUpdate('t1', rowId, { id: rowId, val: 'new' });
      ps.commit();

      const updated = collectScan('t1');
      expect(updated).toHaveLength(3);
      expect(updated.every((r) => r.row.val === 'new')).toBe(true);
    });
  });

  describe('persistence', () => {
    it('data survives commit and is readable', () => {
      const rowId = rm.prepareInsert('t1', { id: 1, val: 'persisted' });
      ps.commit();

      // Create a fresh manager pointing to same storage
      const ps2 = createStore(storage);
      const rm2 = new SyncTableManager(ps2, () => catalog);
      expect(rm2.readRow('t1', rowId)).toEqual({ id: 1, val: 'persisted' });
    });

    it('rollback discards uncommitted data', () => {
      rm.prepareInsert('t1', { id: 1 });
      ps.commit();

      rm.prepareInsert('t1', { id: 2 });
      ps.rollback();

      const rows = collectScan('t1');
      expect(rows).toHaveLength(1);
      expect(rows[0].row).toEqual({ id: 1 });
    });
  });

  describe('deleteTableData', () => {
    it('removes all table data', () => {
      rm.prepareInsert('t1', { id: 1 });
      rm.prepareInsert('t1', { id: 2 });
      ps.commit();

      rm.deleteTableData('t1');
      ps.commit();
    });
  });
});
