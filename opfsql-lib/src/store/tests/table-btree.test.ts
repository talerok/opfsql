import { resetMockOPFS } from 'opfs-mock';
import { describe, it, expect, beforeEach } from 'vitest';
import { SyncTableBTree } from '../table-btree.js';
import type { TableBTreeMeta, TableLeafNode } from '../table-btree.js';
import { Storage } from '../storage.js';
import type { SessionStore } from '../session-store.js';
import { OPFSSyncStorage } from '../backend/opfs-storage.js';
import type { SyncIPageStore } from '../types.js';
import { decodeCbor, encodeCbor } from '../page-codec.js';

let seq = 0;

/** The tree stores opaque row cells; these tests use CBOR-encoded objects. */
const cell = (value: unknown) => encodeCbor(value);
const read = (bytes: Uint8Array | null) =>
  bytes ? decodeCbor<Record<string, unknown>>(bytes) : null;

async function createStore(): Promise<SessionStore> {
  const s = new OPFSSyncStorage(`tbt-test-${seq++}`);
  const storage = new Storage(s);
  await storage.open();
  return storage.createSession();
}

/** Allocate meta + root leaf pages and return a ready-to-use tree. */
function createTree(ps: SyncIPageStore): { tree: SyncTableBTree; metaPageNo: number } {
  const metaPageNo = ps.allocPage();
  const rootPageNo = ps.allocPage();
  const leaf: TableLeafNode = { kind: 'leaf', nodeId: rootPageNo, keys: [], cells: [], nextLeafId: null };
  const meta: TableBTreeMeta = { rootNodeId: rootPageNo, height: 1, nextRowId: 0, size: 0, lastAutoIncrement: 0 };
  ps.writePage(rootPageNo, leaf);
  ps.writePage(metaPageNo, meta);
  return { tree: new SyncTableBTree(metaPageNo, ps), metaPageNo };
}

describe('SyncTableBTree', () => {
  let ps: SessionStore;
  let tree: SyncTableBTree;
  let metaPageNo: number;

  beforeEach(async () => {
    resetMockOPFS();
    ps = await createStore();
    ({ tree, metaPageNo } = createTree(ps));
  });

  function collectScan() {
    const rows = [];
    for (const r of tree.scan()) rows.push(r);
    return rows;
  }

  describe('insert & get', () => {
    it('inserts and reads back', () => {
      const id = tree.insert(cell({ name: 'Alice' }));
      expect(id).toBe(0);
      expect(read(tree.get(id))).toEqual({ name: 'Alice' });
    });

    it('sequential rowIds', () => {
      const id1 = tree.insert(cell({ a: 1 }));
      const id2 = tree.insert(cell({ a: 2 }));
      const id3 = tree.insert(cell({ a: 3 }));
      expect(id1).toBe(0);
      expect(id2).toBe(1);
      expect(id3).toBe(2);
    });

    it('get returns null for missing', () => {
      expect(read(tree.get(99))).toBeNull();
    });

    it('survives commit + reload', () => {
      const id = tree.insert(cell({ val: 'persisted' }));
      ps.commit();

      const tree2 = new SyncTableBTree(metaPageNo, ps);
      expect(read(tree2.get(id))).toEqual({ val: 'persisted' });
    });
  });

  describe('update', () => {
    it('replaces row data', () => {
      const id = tree.insert(cell({ val: 'old' }));
      tree.update(id, cell({ val: 'new' }));
      expect(read(tree.get(id))).toEqual({ val: 'new' });
    });

    it('throws for missing row', () => {
      expect(() => tree.update(99, cell({ val: 'x' }))).toThrow('not found');
    });
  });

  describe('delete', () => {
    it('removes row', () => {
      const id = tree.insert(cell({ val: 1 }));
      tree.delete(id);
      expect(read(tree.get(id))).toBeNull();
    });

    it('no-op for missing', () => {
      tree.delete(99); // should not throw
    });

    it('decrements size', () => {
      tree.insert(cell({ a: 1 }));
      tree.insert(cell({ a: 2 }));
      tree.delete(0);
      expect(collectScan()).toHaveLength(1);
    });
  });

  describe('scan', () => {
    it('empty tree yields nothing', () => {
      expect(collectScan()).toHaveLength(0);
    });

    it('yields all rows in rowId order', () => {
      tree.insert(cell({ a: 1 }));
      tree.insert(cell({ a: 2 }));
      tree.insert(cell({ a: 3 }));
      const rows = collectScan();
      expect(rows).toHaveLength(3);
      expect(rows.map((r) => r.rowId)).toEqual([0, 1, 2]);
      expect(rows.map((r) => read(r.cell)!.a)).toEqual([1, 2, 3]);
    });

    it('skips deleted rows', () => {
      tree.insert(cell({ a: 1 }));
      tree.insert(cell({ a: 2 }));
      tree.insert(cell({ a: 3 }));
      tree.delete(1);
      const rows = collectScan();
      expect(rows).toHaveLength(2);
      expect(rows.map((r) => r.rowId)).toEqual([0, 2]);
    });
  });

  describe('split', () => {
    it('handles more than ORDER inserts', () => {
      const N = 1100;
      for (let i = 0; i < N; i++) tree.insert(cell({ id: i }));

      for (let i = 0; i < N; i++) {
        expect(read(tree.get(i))).toEqual({ id: i });
      }

      expect(collectScan()).toHaveLength(N);
    });
  });

  describe('readMeta — explicit error', () => {
    it('throws descriptive error for invalid metaPageNo', () => {
      const badTree = new SyncTableBTree(999, ps);
      expect(() => badTree.scan().next()).toThrow(/page 999/);
    });
  });

  describe('drop', () => {
    it('removes all data', () => {
      tree.insert(cell({ a: 1 }));
      tree.insert(cell({ a: 2 }));
      ps.commit();

      tree.drop();
      ps.commit();
    });
  });
});
