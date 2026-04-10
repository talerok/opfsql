import { describe, it, expect, beforeEach } from 'vitest';
import { PageManager } from '../page-manager.js';
import { MemoryStorage } from './memory-storage.js';
import type { Page, PageMeta } from '../types.js';

describe('PageManager', () => {
  let storage: MemoryStorage;
  let pm: PageManager;

  beforeEach(() => {
    storage = new MemoryStorage();
    pm = new PageManager(storage);
  });

  describe('read defaults', () => {
    it('readPage returns null for missing page', async () => {
      expect(await pm.readPage('t1', 0)).toBeNull();
    });

    it('getPageMeta returns defaults for missing table', async () => {
      const meta = await pm.getPageMeta('t1');
      expect(meta).toEqual({ lastPageId: -1, totalRowCount: 0, deadRowCount: 0 });
    });
  });

  describe('key formatting', () => {
    it('getPageKey pads pageId', () => {
      expect(pm.getPageKey('t1', 0)).toBe('page:t1:000000');
      expect(pm.getPageKey('t1', 42)).toBe('page:t1:000042');
    });

    it('getMetaKey', () => {
      expect(pm.getMetaKey('t1')).toBe('meta:pages:t1');
    });
  });

  describe('createEmptyPage', () => {
    it('creates page with empty rows', () => {
      const page = pm.createEmptyPage('t1', 3);
      expect(page).toEqual({ pageId: 3, tableId: 't1', rows: [] });
    });
  });

  describe('WAL — write then read', () => {
    it('writePage → readPage returns from WAL', async () => {
      const page: Page = { pageId: 0, tableId: 't1', rows: [] };
      pm.writePage('t1', page);
      const read = await pm.readPage('t1', 0);
      expect(read).toBe(page); // same reference
    });

    it('writeMeta → getPageMeta returns from WAL', async () => {
      const meta: PageMeta = { lastPageId: 2, totalRowCount: 10, deadRowCount: 1 };
      pm.writeMeta('t1', meta);
      const read = await pm.getPageMeta('t1');
      expect(read).toBe(meta);
    });

    it('writeKey → commit persists to storage', async () => {
      pm.writeKey('custom:key', { data: 42 });
      await pm.commit();
      const val = await storage.get<{ data: number }>('custom:key');
      expect(val).toEqual({ data: 42 });
    });

    it('deleteKey → commit removes from storage', async () => {
      await storage.put('page:t1:000000', { pageId: 0, tableId: 't1', rows: [] });
      pm.deleteKey('page:t1:000000');
      await pm.commit();
      expect(await storage.get('page:t1:000000')).toBeNull();
    });
  });

  describe('commit', () => {
    it('flushes WAL to storage', async () => {
      const page: Page = { pageId: 0, tableId: 't1', rows: [] };
      const meta: PageMeta = { lastPageId: 0, totalRowCount: 0, deadRowCount: 0 };
      pm.writePage('t1', page);
      pm.writeMeta('t1', meta);
      await pm.commit();

      expect(await storage.get('page:t1:000000')).toEqual(page);
      expect(await storage.get('meta:pages:t1')).toEqual(meta);
    });

    it('clears WAL after commit', async () => {
      pm.writeKey('k', 'v');
      await pm.commit();
      // After commit, WAL is empty — next read goes to storage
      await storage.delete('k');
      expect(await storage.get('k')).toBeNull();
    });

    it('noop when WAL is empty', async () => {
      await pm.commit(); // should not throw
    });
  });

  describe('rollback', () => {
    it('discards WAL changes', async () => {
      pm.writePage('t1', { pageId: 0, tableId: 't1', rows: [] });
      pm.rollback();
      expect(await pm.readPage('t1', 0)).toBeNull();
    });

    it('does not affect committed data', async () => {
      const page: Page = { pageId: 0, tableId: 't1', rows: [] };
      await storage.put('page:t1:000000', page);
      pm.writePage('t1', { ...page, rows: [{ slotId: 0, deleted: false, data: { id: 1 } }] });
      pm.rollback();
      const read = await pm.readPage('t1', 0);
      expect(read!.rows).toHaveLength(0);
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
});
