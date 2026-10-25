import type { LeafNode, Node } from "./btree.js";
import { compareIndexKeys, keyHasNull } from "./index-btree/compare.js";
import type { IndexKey } from "./index-btree/types.js";
import { encodedSize } from "./page-codec.js";
import { decodeRow } from "./row-codec.js";
import type { TableEntry } from "./table-btree.js";
import type { ICatalog, IndexDef, TableSchema } from "./types.js";

// ---------------------------------------------------------------------------
// Structural integrity check of everything stored in pages.
//
// Every table and index B-tree must be well-formed (leaves at one depth, keys
// sorted within their separators, node sizes recorded exactly), every
// AUTOINCREMENT sequence must be ahead of the ids in use, and every page from
// FIRST_DATA_PAGE to nextPageId must be owned by exactly one tree or be on
// the freelist — no page shared, none leaked.
// ---------------------------------------------------------------------------

const FIRST_DATA_PAGE = 3; // 0 header, 1 catalog, 2 freelist

export interface StoreSnapshot {
  readPage<T>(pageNo: number): T | null;
  catalog: ICatalog;
  nextPageId: number;
  freeList: readonly number[];
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
      const rows = this.checkTable(table);
      for (const index of catalog.getTableIndexes(table.name)) {
        const entries = this.checkIndex(index);
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

  // --- Tables and indexes ---------------------------------------------------

  /** Returns the number of rows, or null if the tree could not be read. */
  private checkTable(table: TableSchema): number | null {
    const owner = `table "${table.name}"`;
    const leaves = this.checkTree<number, TableEntry>(
      owner,
      table.rootPageNo,
      ([rowId]) => rowId,
      (a, b) => a - b,
    );
    if (!leaves) return null;
    const rows = leaves.flatMap((leaf) => leaf.entries);
    this.checkSequence(owner, table, rows);
    return rows.length;
  }

  private checkSequence(
    owner: string,
    table: TableSchema,
    rows: TableEntry[],
  ): void {
    const column = table.columns.find((c) => c.autoIncrement)?.name;
    if (table.sequencePageNo === undefined) {
      if (column !== undefined) {
        this.problems.push(
          `${owner}: AUTOINCREMENT column "${column}" has no sequence page`,
        );
      }
      return;
    }
    if (!this.claim(table.sequencePageNo, `${owner} sequence`)) return;
    const last = this.store.readPage<unknown>(table.sequencePageNo);
    if (typeof last !== "number") {
      this.problems.push(
        `${owner}: sequence page ${table.sequencePageNo} holds no number`,
      );
      return;
    }
    if (column === undefined) return;

    let maxId = -Infinity;
    for (const [, cell] of rows) {
      const id = decodeRow(table, cell)[column];
      if (typeof id === "number" && id > maxId) maxId = id;
    }
    if (maxId > last) {
      this.problems.push(
        `${owner}: AUTOINCREMENT sequence ${last} is behind id ${maxId}`,
      );
    }
  }

  /** Returns the number of entries, or null if the tree could not be read. */
  private checkIndex(index: IndexDef): number | null {
    const owner = `index "${index.name}"`;
    const leaves = this.checkTree<IndexKey, IndexKey>(
      owner,
      index.rootPageNo,
      (storedKey) => storedKey,
      compareIndexKeys,
    );
    if (!leaves) return null;

    let entries = 0;
    let previous: IndexKey | null = null;
    for (const leaf of leaves) {
      for (const stored of leaf.entries) {
        entries++;
        const userKey = stored.slice(0, -1);
        if (
          index.unique &&
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
    return entries;
  }

  // --- B-trees --------------------------------------------------------------

  /**
   * Walks a tree depth-first and returns its leaves in key order, or null
   * if the catalog gives it no root.
   */
  private checkTree<K, E>(
    owner: string,
    rootPageNo: number | undefined,
    keyOf: (entry: E) => K,
    compare: (a: K, b: K) => number,
  ): LeafNode<E>[] | null {
    if (rootPageNo === undefined) {
      this.problems.push(`${owner}: catalog has no root page`);
      return null;
    }
    const leaves: LeafNode<E>[] = [];
    let leafDepth: number | null = null;

    const visit = (pageNo: number, depth: number, lo?: K, hi?: K): void => {
      if (!this.claim(pageNo, owner)) return;
      const node = this.store.readPage<Node<K, E>>(pageNo);
      if (!node || (node.kind !== "leaf" && node.kind !== "internal")) {
        this.problems.push(`${owner}: page ${pageNo} holds no B-tree node`);
        return;
      }
      const keys = node.kind === "leaf" ? node.entries.map(keyOf) : node.keys;
      this.checkKeys(owner, pageNo, keys, compare, lo, hi);
      this.checkBytes(owner, pageNo, node);

      if (node.kind === "leaf") {
        leafDepth ??= depth;
        if (depth !== leafDepth) {
          this.problems.push(
            `${owner}: leaf ${pageNo} is at depth ${depth}, the first leaf at ${leafDepth}`,
          );
        }
        leaves.push(node);
        return;
      }
      if (
        node.keys.length === 0 ||
        node.children.length !== node.keys.length + 1
      ) {
        this.problems.push(
          `${owner}: page ${pageNo} has ${node.keys.length} keys and ${node.children.length} children`,
        );
        return;
      }
      node.children.forEach((child, i) =>
        visit(
          child,
          depth + 1,
          i === 0 ? lo : node.keys[i - 1],
          i === node.keys.length ? hi : node.keys[i],
        ),
      );
    };

    visit(rootPageNo, 1);
    return leaves;
  }

  /** Keys must ascend and stay within [lo, hi) set by the parent's separators. */
  private checkKeys<K>(
    owner: string,
    pageNo: number,
    keys: K[],
    compare: (a: K, b: K) => number,
    lo?: K,
    hi?: K,
  ): void {
    keys.forEach((key, i) => {
      if (i > 0 && compare(keys[i - 1], key) >= 0) {
        this.problems.push(
          `${owner}: page ${pageNo} keys out of order at ${JSON.stringify(key)}`,
        );
      }
      if (
        (lo !== undefined && compare(key, lo) < 0) ||
        (hi !== undefined && compare(key, hi) >= 0)
      ) {
        this.problems.push(
          `${owner}: page ${pageNo} key ${JSON.stringify(key)} is outside its parent's separators`,
        );
      }
    });
  }

  /** A node's recorded size must match its items: splits are decided by it. */
  private checkBytes<K, E>(
    owner: string,
    pageNo: number,
    node: Node<K, E>,
  ): void {
    const items: unknown[] =
      node.kind === "leaf" ? node.entries : [...node.keys, ...node.children];
    const actual = items.reduce<number>(
      (total, item) => total + encodedSize(item),
      0,
    );
    if (node.bytes !== actual) {
      this.problems.push(
        `${owner}: page ${pageNo} records ${node.bytes} bytes of items but holds ${actual}`,
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
