import { resetMockOPFS } from 'opfs-mock';
import { describe, it, expect, beforeEach } from 'vitest';
import { SyncTableBTree } from '../table-btree.js';
import { BTree, type Node } from '../btree.js';
import { RowTooLargeError } from '../errors.js';
import { Storage } from '../storage.js';
import type { SessionStore } from '../session-store.js';
import { OPFSSyncStorage } from '../backend/opfs-storage.js';
import { decodeCbor, encodeCbor } from '../page-codec.js';

let seq = 0;

/** The tree stores opaque row cells; these tests use CBOR-encoded objects. */
const cell = (value: unknown) => encodeCbor(value);
const read = (bytes: Uint8Array | null) =>
  bytes ? decodeCbor<Record<string, unknown>>(bytes) : null;

/** Small pages, so that a few hundred rows split nodes. */
async function createStore(): Promise<SessionStore> {
  const s = new OPFSSyncStorage(`tbt-test-${seq++}`, { pageSize: 4096 });
  const storage = new Storage(s);
  await storage.open();
  return storage.createSession();
}

describe('SyncTableBTree', () => {
  let ps: SessionStore;
  let tree: SyncTableBTree;
  let rootPageNo: number;

  beforeEach(async () => {
    resetMockOPFS();
    ps = await createStore();
    rootPageNo = BTree.create(ps);
    tree = new SyncTableBTree(ps, rootPageNo, 't');
  });

  function collectScan() {
    return [...tree.scan()].map(([rowId, cell]) => ({ rowId, cell }));
  }

  function root() {
    return ps.readPage<Node<number, unknown>>(rootPageNo)!;
  }

  describe('insert & get', () => {
    it('inserts and reads back', () => {
      const id = tree.insert(cell({ name: 'Alice' }));
      expect(id).toBe(1);
      expect(read(tree.get(id))).toEqual({ name: 'Alice' });
    });

    it('sequential rowIds', () => {
      const id1 = tree.insert(cell({ a: 1 }));
      const id2 = tree.insert(cell({ a: 2 }));
      const id3 = tree.insert(cell({ a: 3 }));
      expect(id1).toBe(1);
      expect(id2).toBe(2);
      expect(id3).toBe(3);
    });

    it('the next rowId follows the largest in use', () => {
      for (let i = 0; i < 500; i++) tree.insert(cell({ i }));
      for (let rowId = 101; rowId <= 500; rowId++) tree.delete(rowId);

      // The rightmost leaves are empty now; the largest rowId is further left.
      expect(tree.insert(cell({ i: 'next' }))).toBe(101);
    });

    it('get returns null for missing', () => {
      expect(read(tree.get(99))).toBeNull();
    });

    it('survives commit + reload', () => {
      const id = tree.insert(cell({ val: 'persisted' }));
      ps.commit();

      const tree2 = new SyncTableBTree(ps, rootPageNo, 't');
      expect(read(tree2.get(id))).toEqual({ val: 'persisted' });
    });

    it('rejects a row over half a page, changing nothing', () => {
      tree.insert(cell('small'));
      expect(() => tree.insert(new Uint8Array(2100))).toThrow(RowTooLargeError);
      expect(() => tree.insert(new Uint8Array(2100))).toThrow(
        /row of table "t" is \d+ bytes, exceeds the limit of \d+ bytes/,
      );
      expect(tree.insert(new Uint8Array(1900))).toBe(2);
      expect(collectScan().map((r) => r.rowId)).toEqual([1, 2]);
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

    it('splits a leaf that a growing row overflows', () => {
      for (let i = 0; i < 30; i++) tree.insert(cell('x'.repeat(100)));
      expect(root().kind).toBe('leaf');

      tree.update(15, cell('y'.repeat(1500)));
      expect(root().kind).toBe('internal');
      expect(read(tree.get(15))).toBe('y'.repeat(1500));
      expect(collectScan()).toHaveLength(30);
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
      expect(rows.map((r) => r.rowId)).toEqual([1, 2, 3]);
      expect(rows.map((r) => read(r.cell)!.a)).toEqual([1, 2, 3]);
    });

    it('skips deleted rows', () => {
      tree.insert(cell({ a: 1 }));
      tree.insert(cell({ a: 2 }));
      tree.insert(cell({ a: 3 }));
      tree.delete(2);
      const rows = collectScan();
      expect(rows).toHaveLength(2);
      expect(rows.map((r) => r.rowId)).toEqual([1, 3]);
    });
  });

  describe('split', () => {
    it('handles inserts over several levels', () => {
      const N = 3000;
      for (let i = 0; i < N; i++) tree.insert(cell({ id: i }));

      expect(root().kind).toBe('internal');
      for (let i = 0; i < N; i++) {
        expect(read(tree.get(i + 1))).toEqual({ id: i });
      }
      expect(collectScan()).toHaveLength(N);
    });
  });

  describe('missing root', () => {
    it('throws a descriptive error', () => {
      const badTree = new SyncTableBTree(ps, 999, 't');
      expect(() => [...badTree.scan()]).toThrow(/page 999/);
    });
  });
});
