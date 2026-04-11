import { describe, it, expect, beforeEach } from 'vitest';
import { PageManager } from '../page-manager.js';
import { MemoryStorage } from './memory-storage.js';

describe('PageManager', () => {
  let storage: MemoryStorage;
  let pm: PageManager;

  beforeEach(() => {
    storage = new MemoryStorage();
    pm = new PageManager(storage);
  });

  describe('read defaults', () => {
    it('readRow returns null for missing row', async () => {
      expect(await pm.readRow('t1', 0)).toBeNull();
    });

    it('getPageMeta returns defaults for missing table', async () => {
      const meta = await pm.getPageMeta('t1');
      expect(meta).toEqual({ lastPageId: -1, nextRowId: 0, totalRowCount: 0, freePageIds: [] });
    });
  });

  describe('KV operations (writeKey/readKey)', () => {
    it('writeKey → readKey returns value from WAL', async () => {
      pm.writeKey('custom:key', { data: 42 });
      const val = await pm.readKey<{ data: number }>('custom:key');
      expect(val).toEqual({ data: 42 });
    });

    it('writeKey → commit persists to storage', async () => {
      pm.writeKey('custom:key', { data: 42 });
      await pm.commit();
      const val = await storage.get<{ data: number }>('custom:key');
      expect(val).toEqual({ data: 42 });
    });

    it('deleteKey → commit removes from storage', async () => {
      await storage.put('page:t1:000000', { pageId: 0, tableId: 't1', rows: {} });
      pm.deleteKey('page:t1:000000');
      await pm.commit();
      expect(await storage.get('page:t1:000000')).toBeNull();
    });
  });

  describe('commit', () => {
    it('flushes WAL to storage', async () => {
      await pm.prepareInsert('t1', { id: 1 });
      await pm.commit();

      const page = await storage.get<any>('page:t1:000000');
      expect(page).not.toBeNull();
      expect(Object.keys(page.rows)).toHaveLength(1);
    });

    it('data survives in cache after commit', async () => {
      const rowId = await pm.prepareInsert('t1', { id: 1 });
      await pm.commit();

      const row = await pm.readRow('t1', rowId);
      expect(row).toEqual({ id: 1 });
    });

    it('noop when WAL is empty', async () => {
      await pm.commit(); // should not throw
    });
  });

  describe('rollback', () => {
    it('discards uncommitted data', async () => {
      const rowId = await pm.prepareInsert('t1', { id: 1 });
      pm.rollback();
      expect(await pm.readRow('t1', rowId)).toBeNull();
    });

    it('does not affect committed data in cache', async () => {
      const rowId = await pm.prepareInsert('t1', { id: 1 });
      await pm.commit();

      await pm.prepareInsert('t1', { id: 2 });
      pm.rollback();

      expect(await pm.readRow('t1', rowId)).toEqual({ id: 1 });
    });
  });

  describe('getAllPageKeys', () => {
    it('returns keys from storage', async () => {
      await storage.put('page:t1:000000', {});
      await storage.put('page:t1:000001', {});
      await storage.put('page:t2:000000', {});
      const keys = await pm.getAllPageKeys('t1');
      expect(keys).toEqual(['page:t1:000000', 'page:t1:000001']);
    });
  });

  describe('deleteTableData', () => {
    it('removes all pages, meta, and rowmap for a table', async () => {
      await pm.prepareInsert('t1', { id: 1 });
      await pm.prepareInsert('t1', { id: 2 });
      await pm.commit();

      await pm.deleteTableData('t1');
      await pm.commit();

      const pm2 = new PageManager(storage);
      const meta = await pm2.getPageMeta('t1');
      expect(meta.lastPageId).toBe(-1);
    });
  });

  describe('getAllKeys', () => {
    it('merges WAL and storage keys', async () => {
      await storage.put('idx:test:a', 'val');
      pm.writeKey('idx:test:b', 'val');
      const keys = await pm.getAllKeys('idx:test:');
      expect(keys).toEqual(['idx:test:a', 'idx:test:b']);
    });

    it('WAL deletes remove storage keys', async () => {
      await storage.put('idx:test:a', 'val');
      pm.deleteKey('idx:test:a');
      const keys = await pm.getAllKeys('idx:test:');
      expect(keys).toEqual([]);
    });
  });

});
