import { compareIndexKeys, keyHasNull } from "./index-btree/compare.js";
import type { BTreeMeta, IndexKey } from "./index-btree/types.js";
import type { TableBTreeMeta } from "./table-btree.js";
import type { ICatalog } from "./types.js";

// ---------------------------------------------------------------------------
// Structural integrity check of everything stored in pages.
//
// Every table and index B-tree must be well-formed (uniform depth, sorted keys
// within separator bounds, intact leaf chain, counters matching contents), and
// every page from FIRST_DATA_PAGE to nextPageId must be owned by exactly one
// tree or be on the freelist — no page shared, none leaked.
// ---------------------------------------------------------------------------

const FIRST_DATA_PAGE = 3; // 0 header, 1 catalog, 2 freelist

export interface StoreSnapshot {
  readPage<T>(pageNo: number): T | null;
  catalog: ICatalog;
  nextPageId: number;
  freeList: readonly number[];
}

interface NodeLike<K> {
  kind: "leaf" | "internal";
  nodeId: number;
  keys: K[];
  children?: number[];
  values?: unknown[];
  nextLeafId?: number | null;
}

/** Returns the problems found; an empty list means the store is consistent. */
export function checkStoreIntegrity(store: StoreSnapshot): string[] {
  return new IntegrityChecker(store).run();
}

class IntegrityChecker {
  private readonly problems: string[] = [];
  private readonly owners = new Map<number, string>();

  constructor(private readonly store: StoreSnapshot) {}

  run(): string[] {
    const { catalog } = this.store;
    for (const table of catalog.getAllTables()) {
      const rows = this.checkTable(table.name, table.metaPageNo);
      for (const index of catalog.getTableIndexes(table.name)) {
        const entries = this.checkIndex(
          index.name,
          index.metaPageNo,
          index.unique,
        );
        if (rows !== null && entries !== null && rows !== entries) {
          this.problems.push(
            `index "${index.name}" has ${entries} entries for ${rows} rows of "${table.name}"`,
          );
        }
      }
    }
    this.checkFreeList();
    this.checkAllPagesAccounted();
    return this.problems;
  }

  // --- Trees ----------------------------------------------------------------

  /** Returns the number of rows, or null if the tree could not be read. */
  private checkTable(name: string, metaPageNo?: number): number | null {
    const owner = `table "${name}"`;
    const meta = this.readMeta<TableBTreeMeta>(owner, metaPageNo);
    if (!meta) return null;

    const leaves = this.checkTree<number>(owner, meta, (a, b) => a - b);
    let rows = 0;
    for (const leaf of leaves) {
      rows += leaf.keys.length;
      if (leaf.values?.length !== leaf.keys.length) {
        this.problems.push(
          `${owner}: leaf ${leaf.nodeId} has ${leaf.keys.length} keys but ${leaf.values?.length} values`,
        );
      }
      for (const rowId of leaf.keys) {
        if (rowId >= meta.nextRowId) {
          this.problems.push(
            `${owner}: row ${rowId} is not below nextRowId ${meta.nextRowId}`,
          );
        }
      }
    }
    this.checkSize(owner, meta.size, rows);
    return rows;
  }

  /** Returns the number of entries, or null if the tree could not be read. */
  private checkIndex(
    name: string,
    metaPageNo: number | undefined,
    unique: boolean,
  ): number | null {
    const owner = `index "${name}"`;
    const meta = this.readMeta<BTreeMeta>(owner, metaPageNo);
    if (!meta) return null;

    const leaves = this.checkTree<IndexKey>(owner, meta, compareIndexKeys);
    let entries = 0;
    let previous: IndexKey | null = null;
    for (const leaf of leaves) {
      entries += leaf.keys.length;
      for (const stored of leaf.keys) {
        const userKey = stored.slice(0, -1);
        if (
          unique &&
          previous !== null &&
          !keyHasNull(userKey) &&
          compareIndexKeys(previous, userKey) === 0
        ) {
          this.problems.push(
            `${owner}: unique key ${JSON.stringify(userKey)} appears more than once`,
          );
        }
        previous = userKey;
      }
    }
    this.checkSize(owner, meta.size, entries);
    return entries;
  }

  private readMeta<M>(owner: string, metaPageNo?: number): M | null {
    if (metaPageNo === undefined) {
      this.problems.push(`${owner}: catalog has no meta page`);
      return null;
    }
    if (!this.claim(metaPageNo, `${owner} meta`)) return null;
    const meta = this.store.readPage<M>(metaPageNo);
    if (!meta) this.problems.push(`${owner}: meta page ${metaPageNo} is empty`);
    return meta;
  }

  /** Walks the tree depth-first and returns its leaves in key order. */
  private checkTree<K>(
    owner: string,
    meta: { rootNodeId: number; height: number },
    compare: (a: K, b: K) => number,
  ): NodeLike<K>[] {
    const leaves: NodeLike<K>[] = [];
    const describe = (key: K) => JSON.stringify(key);

    const visit = (pageNo: number, depth: number, lo?: K, hi?: K): void => {
      if (!this.claim(pageNo, owner)) return;
      const node = this.store.readPage<NodeLike<K>>(pageNo);
      if (!node) {
        this.problems.push(`${owner}: node page ${pageNo} is empty`);
        return;
      }
      if (node.nodeId !== pageNo) {
        this.problems.push(
          `${owner}: page ${pageNo} holds node ${node.nodeId}`,
        );
      }
      const expected = depth === meta.height ? "leaf" : "internal";
      if (node.kind !== expected) {
        this.problems.push(
          `${owner}: page ${pageNo} at depth ${depth} of ${meta.height} is ${node.kind}, expected ${expected}`,
        );
        return;
      }

      node.keys.forEach((key, i) => {
        if (i > 0 && compare(node.keys[i - 1], key) >= 0) {
          this.problems.push(
            `${owner}: page ${pageNo} keys out of order at ${describe(key)}`,
          );
        }
        if (
          (lo !== undefined && compare(key, lo) < 0) ||
          (hi !== undefined && compare(key, hi) >= 0)
        ) {
          this.problems.push(
            `${owner}: page ${pageNo} key ${describe(key)} is outside its parent's separators`,
          );
        }
      });

      if (node.kind === "leaf") {
        leaves.push(node);
        return;
      }
      const children = node.children ?? [];
      if (children.length !== node.keys.length + 1) {
        this.problems.push(
          `${owner}: page ${pageNo} has ${node.keys.length} keys and ${children.length} children`,
        );
        return;
      }
      children.forEach((child, i) =>
        visit(
          child,
          depth + 1,
          i === 0 ? lo : node.keys[i - 1],
          i === node.keys.length ? hi : node.keys[i],
        ),
      );
    };

    visit(meta.rootNodeId, 1);
    this.checkLeafChain(owner, leaves);
    return leaves;
  }

  private checkLeafChain<K>(owner: string, leaves: NodeLike<K>[]): void {
    leaves.forEach((leaf, i) => {
      const expected = i + 1 < leaves.length ? leaves[i + 1].nodeId : null;
      if (leaf.nextLeafId !== expected) {
        this.problems.push(
          `${owner}: leaf ${leaf.nodeId} links to ${leaf.nextLeafId}, expected ${expected}`,
        );
      }
    });
  }

  private checkSize(owner: string, recorded: number, actual: number): void {
    if (recorded !== actual) {
      this.problems.push(
        `${owner}: meta records ${recorded} entries, tree holds ${actual}`,
      );
    }
  }

  // --- Page accounting ------------------------------------------------------

  private checkFreeList(): void {
    for (const pageNo of this.store.freeList) this.claim(pageNo, "freelist");
  }

  private checkAllPagesAccounted(): void {
    for (let p = FIRST_DATA_PAGE; p < this.store.nextPageId; p++) {
      if (!this.owners.has(p)) {
        this.problems.push(`page ${p} is neither used nor free (leaked)`);
      }
    }
  }

  /** Records `owner` for the page; false if the page is invalid or taken. */
  private claim(pageNo: number, owner: string): boolean {
    if (pageNo < FIRST_DATA_PAGE || pageNo >= this.store.nextPageId) {
      this.problems.push(
        `${owner} refers to page ${pageNo}, outside ${FIRST_DATA_PAGE}..${this.store.nextPageId - 1}`,
      );
      return false;
    }
    const current = this.owners.get(pageNo);
    if (current !== undefined) {
      this.problems.push(`page ${pageNo} is used by ${current} and ${owner}`);
      return false;
    }
    this.owners.set(pageNo, owner);
    return true;
  }
}
