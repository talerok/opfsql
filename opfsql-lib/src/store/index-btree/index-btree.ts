import type { RowId, SyncIPageStore } from "../types.js";
import { compareIndexKeys, keyHasNull } from "./compare.js";
import type {
  BTreeInternalNode,
  BTreeLeafNode,
  BTreeMeta,
  BTreeNode,
  IndexKey,
  RangeOptions,
} from "./types.js";
import { ORDER } from "./types.js";

export type { RangeOptions } from "./types.js";

interface LeafPath {
  ancestors: BTreeInternalNode[];
  leaf: BTreeLeafNode;
}

// ---------------------------------------------------------------------------
// Stored keys in index B-tree are [...userKey, rowId] (SQLite-style).
// Every entry is unique by construction; duplicate user keys differ in the
// trailing rowId, so leaves split normally regardless of duplicate skew.
// The rowId is recovered from the last element of the stored key (no parallel
// rowIds[] array — see extractRowId). Range bounds are user-key-shaped;
// prefix semantics are built into isBelowLower / isAboveUpper via slicing to
// the bound length. compareIndexKeys' length tiebreaker makes
// [userKey] < [...userKey, rowId], so user-key bounds correctly frame every
// matching stored key.
// ---------------------------------------------------------------------------

function extractRowId(storedKey: IndexKey): RowId {
  return storedKey[storedKey.length - 1] as RowId;
}

export class SyncBTree {
  constructor(
    private readonly metaPageNo: number,
    private readonly pageStore: SyncIPageStore,
    private readonly unique: boolean = false,
  ) {}

  // --- Public API -----------------------------------------------------------

  insert(userKey: IndexKey, rowId: RowId): void {
    if (this.unique && !keyHasNull(userKey) && this.existsWithPrefix(userKey))
      throw new Error(`UNIQUE constraint failed: index`);
    const meta = { ...this.readMeta() };
    const storedKey: IndexKey = [...userKey, rowId];
    const { ancestors, leaf } = this.findLeafPath(meta, storedKey);
    const updated = this.insertIntoLeaf(leaf, storedKey);
    meta.size++;
    if (updated.keys.length >= ORDER) {
      this.splitLeaf(meta, updated, ancestors);
    } else {
      this.writeNode(updated);
    }
    this.writeMeta(meta);
  }

  delete(userKey: IndexKey, rowId: RowId): void {
    const meta = { ...this.readMeta() };
    const storedKey: IndexKey = [...userKey, rowId];
    const { leaf } = this.findLeafPath(meta, storedKey);
    const updated = this.removeFromLeaf(leaf, storedKey);
    if (!updated) return;
    meta.size--;
    this.writeNode(updated);
    this.writeMeta(meta);
  }

  lookup(userKey: IndexKey): RowId[] {
    return this.range({ lower: userKey, upper: userKey });
  }

  range(opts: RangeOptions = {}): RowId[] {
    const meta = this.readMeta();
    if (meta.size === 0) return [];
    const startLeaf = opts.lower
      ? this.findLeafPath(meta, opts.lower).leaf
      : this.findLeftmostLeaf(meta);
    return this.collectRange(startLeaf, opts);
  }

  bulkLoad(entries: Array<{ key: IndexKey; rowId: RowId }>): void {
    const meta: BTreeMeta = { rootNodeId: 0, height: 1, size: 0 };
    if (entries.length === 0) {
      meta.rootNodeId = this.createLeaf([], null);
      this.writeMeta(meta);
      return;
    }
    if (this.unique) this.checkUniqueSorted(entries);
    meta.size = entries.length;
    const leaves = this.buildLeaves(entries);
    this.linkAndWriteLeaves(leaves);
    meta.rootNodeId =
      leaves.length === 1
        ? leaves[0].nodeId
        : this.buildInternalLevels(meta, leaves);
    this.writeMeta(meta);
  }

  drop(): void {
    const meta = this.readMeta();
    this.freeSubtree(meta.rootNodeId, meta.height);
    this.pageStore.freePage(this.metaPageNo);
  }

  // --- Leaf operations ------------------------------------------------------

  private insertIntoLeaf(
    leaf: BTreeLeafNode,
    storedKey: IndexKey,
  ): BTreeLeafNode {
    const pos = this.bisectLeft(leaf.keys, storedKey);
    return {
      ...leaf,
      keys: [...leaf.keys.slice(0, pos), storedKey, ...leaf.keys.slice(pos)],
    };
  }

  private removeFromLeaf(
    leaf: BTreeLeafNode,
    storedKey: IndexKey,
  ): BTreeLeafNode | null {
    const pos = this.bisectLeft(leaf.keys, storedKey);
    if (
      pos >= leaf.keys.length ||
      compareIndexKeys(leaf.keys[pos], storedKey) !== 0
    )
      return null;
    return {
      ...leaf,
      keys: [...leaf.keys.slice(0, pos), ...leaf.keys.slice(pos + 1)],
    };
  }

  private existsWithPrefix(prefix: IndexKey): boolean {
    // Only called from insert() when this.unique is true, so at most one
    // entry can match — range() reads 1-2 leaves and returns 0 or 1 rowIds.
    // Using range correctly handles the separator-boundary case where
    // findLeafPath lands on the left sibling of the matching leaf.
    return this.range({ lower: prefix, upper: prefix }).length > 0;
  }

  // --- Range scan -----------------------------------------------------------

  private findLeftmostLeaf(meta: BTreeMeta): BTreeLeafNode {
    let node: BTreeNode = this.readNode(meta.rootNodeId);
    while (node.kind === "internal") node = this.readNode(node.children[0]);
    return node;
  }

  private collectRange(startLeaf: BTreeLeafNode, opts: RangeOptions): RowId[] {
    const results: RowId[] = [];
    let leaf: BTreeLeafNode | null = startLeaf;
    while (leaf) {
      for (let i = 0; i < leaf.keys.length; i++) {
        const key = leaf.keys[i];
        if (this.isBelowLower(key, opts)) continue;
        if (this.isAboveUpper(key, opts)) return results;
        results.push(extractRowId(key));
      }
      leaf = leaf.nextLeafId !== null
        ? (this.readNode(leaf.nextLeafId) as BTreeLeafNode)
        : null;
    }
    return results;
  }

  private isBelowLower(key: IndexKey, opts: RangeOptions): boolean {
    if (!opts.lower) return false;
    const prefix = key.slice(0, opts.lower.length);
    const cmp = compareIndexKeys(prefix, opts.lower);
    return opts.lowerInclusive !== false ? cmp < 0 : cmp <= 0;
  }

  private isAboveUpper(key: IndexKey, opts: RangeOptions): boolean {
    if (!opts.upper) return false;
    const prefix = key.slice(0, opts.upper.length);
    const cmp = compareIndexKeys(prefix, opts.upper);
    return opts.upperInclusive !== false ? cmp > 0 : cmp >= 0;
  }

  // --- Bulk load ------------------------------------------------------------

  private checkUniqueSorted(
    entries: Array<{ key: IndexKey; rowId: RowId }>,
  ): void {
    for (let i = 1; i < entries.length; i++) {
      if (
        compareIndexKeys(entries[i].key, entries[i - 1].key) === 0 &&
        !keyHasNull(entries[i].key)
      )
        throw new Error(`UNIQUE constraint failed: index`);
    }
  }

  private buildLeaves(
    entries: Array<{ key: IndexKey; rowId: RowId }>,
  ): BTreeLeafNode[] {
    const leaves: BTreeLeafNode[] = [];
    for (let i = 0; i < entries.length; i += ORDER - 1) {
      const chunk = entries.slice(i, i + ORDER - 1);
      const nodeId = this.pageStore.allocPage();
      leaves.push({
        kind: "leaf",
        nodeId,
        keys: chunk.map((e) => [...e.key, e.rowId]),
        nextLeafId: null,
      });
    }
    return leaves;
  }

  private linkAndWriteLeaves(leaves: BTreeLeafNode[]): void {
    for (let i = 0; i < leaves.length - 1; i++)
      leaves[i].nextLeafId = leaves[i + 1].nodeId;
    for (const leaf of leaves) this.writeNode(leaf);
  }

  private buildInternalLevels(
    meta: BTreeMeta,
    leaves: BTreeLeafNode[],
  ): number {
    let level = leaves.map((l) => ({ nodeId: l.nodeId, firstKey: l.keys[0] }));
    while (level.length > 1) {
      meta.height++;
      const next: Array<{ nodeId: number; firstKey: IndexKey }> = [];
      for (let i = 0; i < level.length; i += ORDER) {
        const chunk = level.slice(i, i + ORDER);
        const nodeId = this.pageStore.allocPage();
        this.writeNode({
          kind: "internal",
          nodeId,
          keys: chunk.slice(1).map((c) => c.firstKey),
          children: chunk.map((c) => c.nodeId),
        });
        next.push({ nodeId, firstKey: chunk[0].firstKey });
      }
      level = next;
    }
    return level[0].nodeId;
  }

  // --- Split ----------------------------------------------------------------

  private splitLeaf(
    meta: BTreeMeta,
    leaf: BTreeLeafNode,
    ancestors: BTreeInternalNode[],
  ): void {
    const mid = leaf.keys.length >>> 1;
    const rightLeaf: BTreeLeafNode = {
      kind: "leaf",
      nodeId: this.pageStore.allocPage(),
      keys: leaf.keys.slice(mid),
      nextLeafId: leaf.nextLeafId,
    };
    const leftLeaf: BTreeLeafNode = {
      ...leaf,
      keys: leaf.keys.slice(0, mid),
      nextLeafId: rightLeaf.nodeId,
    };
    this.writeNode(leftLeaf);
    this.writeNode(rightLeaf);
    this.propagateSplit(
      meta, ancestors, ancestors.length - 1,
      rightLeaf.keys[0], rightLeaf.nodeId,
    );
  }

  private propagateSplit(
    meta: BTreeMeta,
    ancestors: BTreeInternalNode[],
    parentIdx: number,
    key: IndexKey,
    rightChildId: number,
  ): void {
    if (parentIdx < 0) {
      this.createNewRoot(meta, key, rightChildId);
      return;
    }
    const parent = ancestors[parentIdx];
    const pos = this.bisectRight(parent.keys, key);
    const updated: BTreeInternalNode = {
      ...parent,
      keys: [...parent.keys.slice(0, pos), key, ...parent.keys.slice(pos)],
      children: [
        ...parent.children.slice(0, pos + 1),
        rightChildId,
        ...parent.children.slice(pos + 1),
      ],
    };
    this.writeNode(updated);
    if (updated.keys.length >= ORDER)
      this.splitInternal(meta, updated, ancestors, parentIdx);
  }

  private splitInternal(
    meta: BTreeMeta,
    node: BTreeInternalNode,
    ancestors: BTreeInternalNode[],
    nodeIdx: number,
  ): void {
    const mid = node.keys.length >>> 1;
    const promotedKey = node.keys[mid];
    const rightNode: BTreeInternalNode = {
      kind: "internal",
      nodeId: this.pageStore.allocPage(),
      keys: node.keys.slice(mid + 1),
      children: node.children.slice(mid + 1),
    };
    const leftNode: BTreeInternalNode = {
      ...node,
      keys: node.keys.slice(0, mid),
      children: node.children.slice(0, mid + 1),
    };
    this.writeNode(leftNode);
    this.writeNode(rightNode);
    this.propagateSplit(
      meta, ancestors, nodeIdx - 1,
      promotedKey, rightNode.nodeId,
    );
  }

  private createNewRoot(
    meta: BTreeMeta,
    key: IndexKey,
    rightChildId: number,
  ): void {
    const nodeId = this.pageStore.allocPage();
    this.writeNode({
      kind: "internal",
      nodeId,
      keys: [key],
      children: [meta.rootNodeId, rightChildId],
    });
    meta.rootNodeId = nodeId;
    meta.height++;
  }

  // --- Traversal ------------------------------------------------------------

  private findLeafPath(meta: BTreeMeta, key: IndexKey): LeafPath {
    const ancestors: BTreeInternalNode[] = [];
    let node: BTreeNode = this.readNode(meta.rootNodeId);
    while (node.kind === "internal") {
      ancestors.push(node);
      node = this.readNode(node.children[this.bisectRight(node.keys, key)]);
    }
    return { ancestors, leaf: node };
  }

  private bisectLeft(keys: IndexKey[], key: IndexKey): number {
    let lo = 0, hi = keys.length;
    while (lo < hi) {
      const mid = (lo + hi) >>> 1;
      if (compareIndexKeys(keys[mid], key) < 0) lo = mid + 1;
      else hi = mid;
    }
    return lo;
  }

  private bisectRight(keys: IndexKey[], key: IndexKey): number {
    let lo = 0, hi = keys.length;
    while (lo < hi) {
      const mid = (lo + hi) >>> 1;
      if (compareIndexKeys(key, keys[mid]) < 0) hi = mid;
      else lo = mid + 1;
    }
    return lo;
  }

  // --- Cleanup --------------------------------------------------------------

  private freeSubtree(pageNo: number, height: number): void {
    if (height === 1) {
      this.pageStore.freePage(pageNo);
      return;
    }
    const node = this.readNode(pageNo) as BTreeInternalNode;
    for (const childId of node.children)
      this.freeSubtree(childId, height - 1);
    this.pageStore.freePage(pageNo);
  }

  // --- Storage --------------------------------------------------------------

  private readMeta(): BTreeMeta {
    const meta = this.pageStore.readPage<BTreeMeta>(this.metaPageNo);
    if (!meta)
      throw new Error(`Index B-tree meta at page ${this.metaPageNo} not found`);
    return meta;
  }

  private writeMeta(meta: BTreeMeta): void {
    this.pageStore.writePage(this.metaPageNo, meta);
  }

  private readNode(pageNo: number): BTreeNode {
    const node = this.pageStore.readPage<BTreeNode>(pageNo);
    if (!node) throw new Error(`B-tree node at page ${pageNo} not found`);
    return node;
  }

  private writeNode(node: BTreeNode): void {
    this.pageStore.writePage(node.nodeId, node);
  }

  private createLeaf(
    keys: IndexKey[],
    nextLeafId: number | null,
  ): number {
    const nodeId = this.pageStore.allocPage();
    this.writeNode({ kind: "leaf", nodeId, keys, nextLeafId });
    return nodeId;
  }
}
