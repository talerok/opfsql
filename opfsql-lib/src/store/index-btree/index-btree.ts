import type { RowId, SyncIPageStore } from "../types.js";
import { compareIndexKeys, keyHasNull } from "./compare.js";
import {
  computeBounds,
  type ScanBounds,
  type SearchPredicate,
} from "./search-bounds.js";
import type {
  BTreeInternalNode,
  BTreeLeafNode,
  BTreeMeta,
  BTreeNode,
  IndexKey,
} from "./types.js";
import { ORDER } from "./types.js";
export type { SearchPredicate } from "./search-bounds.js";

export class SyncBTree {
  constructor(
    private readonly metaPageNo: number,
    private readonly ps: SyncIPageStore,
    private readonly unique: boolean = false,
  ) {}

  insert(key: IndexKey, rowId: RowId): void {
    const meta: BTreeMeta = { ...this.readMeta() };
    const path = this.findLeafPath(meta, key);
    const leaf = path.at(-1) as BTreeLeafNode;
    const newLeaf = this.insertIntoLeaf(leaf, key, rowId);
    meta.size++;
    path[path.length - 1] = newLeaf;
    if (newLeaf.keys.length >= ORDER) this.splitLeaf(meta, newLeaf, path);
    else this.writeNode(newLeaf);
    this.writeMeta(meta);
  }

  delete(key: IndexKey, rowId: RowId): void {
    const baseMeta = this.readMeta();
    const meta = { ...baseMeta };
    const leaf = this.findLeafPath(meta, key).at(-1) as BTreeLeafNode;
    const newLeaf = this.removeFromLeaf(leaf, key, rowId);
    if (!newLeaf) return;
    meta.size--;
    this.writeNode(newLeaf);
    this.writeMeta(meta);
  }

  search(predicates: SearchPredicate[], totalColumns?: number): RowId[] {
    const meta = this.readMeta();
    if (meta.size === 0) return [];
    return this.rangeScan(meta, computeBounds(predicates, totalColumns));
  }

  bulkLoad(entries: Array<{ key: IndexKey; rowId: RowId }>): void {
    const meta: BTreeMeta = {
      rootNodeId: 0,
      height: 1,
      size: 0,
      unique: this.unique,
    };
    if (entries.length === 0) {
      meta.rootNodeId = this.createLeaf([], [], null);
      this.writeMeta(meta);
      return;
    }
    const merged = this.mergeEntries(entries);
    meta.size = entries.length;
    const leaves = this.buildLeaves(merged);
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
    this.ps.freePage(this.metaPageNo);
  }

  // --- Drop (recursive free) ------------------------------------------------

  private freeSubtree(pageNo: number, height: number): void {
    if (height === 1) {
      this.ps.freePage(pageNo);
      return;
    }
    const node = this.readNode(pageNo) as BTreeInternalNode;
    for (const childId of node.children) {
      this.freeSubtree(childId, height - 1);
    }
    this.ps.freePage(pageNo);
  }

  // --- Leaf operations ------------------------------------------------------

  private insertIntoLeaf(
    leaf: BTreeLeafNode,
    key: IndexKey,
    rowId: RowId,
  ): BTreeLeafNode {
    const pos = this.bisectLeft(leaf.keys, key);
    const isExisting =
      pos < leaf.keys.length && compareIndexKeys(leaf.keys[pos], key) === 0;
    if (isExisting) {
      if (this.unique && !keyHasNull(key))
        throw new Error(`UNIQUE constraint failed: index`);
      return {
        ...leaf,
        rowIds: leaf.rowIds.map((b, i) => (i === pos ? [...b, rowId] : b)),
      };
    }
    return {
      ...leaf,
      keys: [...leaf.keys.slice(0, pos), key, ...leaf.keys.slice(pos)],
      rowIds: [
        ...leaf.rowIds.slice(0, pos),
        [rowId],
        ...leaf.rowIds.slice(pos),
      ],
    };
  }

  private removeFromLeaf(
    leaf: BTreeLeafNode,
    key: IndexKey,
    rowId: RowId,
  ): BTreeLeafNode | null {
    const pos = this.bisectLeft(leaf.keys, key);
    if (pos >= leaf.keys.length || compareIndexKeys(leaf.keys[pos], key) !== 0)
      return null;
    const bucket = leaf.rowIds[pos];
    const idx = bucket.indexOf(rowId);
    if (idx === -1) return null;
    const newBucket = [...bucket.slice(0, idx), ...bucket.slice(idx + 1)];
    if (newBucket.length === 0) {
      return {
        ...leaf,
        keys: [...leaf.keys.slice(0, pos), ...leaf.keys.slice(pos + 1)],
        rowIds: [...leaf.rowIds.slice(0, pos), ...leaf.rowIds.slice(pos + 1)],
      };
    }
    return {
      ...leaf,
      rowIds: leaf.rowIds.map((b, i) => (i === pos ? newBucket : b)),
    };
  }

  // --- Range scan -----------------------------------------------------------

  private rangeScan(meta: BTreeMeta, bounds: ScanBounds): RowId[] {
    if (bounds.exactKey) return this.pointLookup(meta, bounds.exactKey);
    const startLeaf = bounds.lowerKey
      ? (this.findLeafPath(meta, bounds.lowerKey).at(-1) as BTreeLeafNode)
      : this.findLeftmostLeaf(meta);
    return this.collectRange(startLeaf, bounds);
  }

  private pointLookup(meta: BTreeMeta, key: IndexKey): RowId[] {
    const leaf = this.findLeafPath(meta, key).at(-1) as BTreeLeafNode;
    const pos = this.bisectLeft(leaf.keys, key);
    if (pos < leaf.keys.length && compareIndexKeys(leaf.keys[pos], key) === 0)
      return [...leaf.rowIds[pos]];
    return [];
  }

  private findLeftmostLeaf(meta: BTreeMeta): BTreeLeafNode {
    let node = this.readNode(meta.rootNodeId);
    while (node.kind === "internal") node = this.readNode(node.children[0]);
    return node;
  }

  private collectRange(startLeaf: BTreeLeafNode, bounds: ScanBounds): RowId[] {
    const results: RowId[] = [];
    let leaf: BTreeLeafNode | null = startLeaf;
    while (leaf) {
      for (let i = 0; i < leaf.keys.length; i++) {
        const key = leaf.keys[i];
        if (this.isBelowLower(key, bounds)) continue;
        if (this.isAboveUpper(key, bounds)) return results;
        results.push(...leaf.rowIds[i]);
      }
      leaf =
        leaf.nextLeafId !== null
          ? (this.readNode(leaf.nextLeafId) as BTreeLeafNode)
          : null;
    }
    return results;
  }

  private isBelowLower(key: IndexKey, bounds: ScanBounds): boolean {
    if (!bounds.lowerKey) return false;
    const cmp = compareIndexKeys(key, bounds.lowerKey);
    return bounds.lowerInclusive ? cmp < 0 : cmp <= 0;
  }

  private isAboveUpper(key: IndexKey, bounds: ScanBounds): boolean {
    if (!bounds.upperKey) return false;
    if (bounds.prefixScan)
      return (
        compareIndexKeys(
          key.slice(0, bounds.upperKey.length),
          bounds.upperKey,
        ) > 0
      );
    const cmp = compareIndexKeys(key, bounds.upperKey);
    return bounds.upperInclusive ? cmp > 0 : cmp >= 0;
  }

  // --- Bulk load ------------------------------------------------------------

  private mergeEntries(
    entries: Array<{ key: IndexKey; rowId: RowId }>,
  ): Array<{ key: IndexKey; rowIds: RowId[] }> {
    const merged: Array<{ key: IndexKey; rowIds: RowId[] }> = [];
    for (const { key, rowId } of entries) {
      const last = merged.at(-1);
      if (last && compareIndexKeys(last.key, key) === 0) {
        if (this.unique && !keyHasNull(key))
          throw new Error(`UNIQUE constraint failed: index`);
        last.rowIds.push(rowId);
      } else {
        merged.push({ key, rowIds: [rowId] });
      }
    }
    return merged;
  }

  private buildLeaves(
    merged: Array<{ key: IndexKey; rowIds: RowId[] }>,
  ): BTreeLeafNode[] {
    const leaves: BTreeLeafNode[] = [];
    for (let i = 0; i < merged.length; i += ORDER - 1) {
      const chunk = merged.slice(i, i + ORDER - 1);
      const nodeId = this.ps.allocPage();
      leaves.push({
        kind: "leaf",
        nodeId,
        keys: chunk.map((e) => e.key),
        rowIds: chunk.map((e) => e.rowIds),
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
        const nodeId = this.ps.allocPage();
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
    path: BTreeNode[],
  ): void {
    const mid = leaf.keys.length >>> 1;
    const newPageNo = this.ps.allocPage();
    const newLeaf: BTreeLeafNode = {
      kind: "leaf",
      nodeId: newPageNo,
      keys: leaf.keys.slice(mid),
      rowIds: leaf.rowIds.slice(mid),
      nextLeafId: leaf.nextLeafId,
    };
    const updatedLeaf: BTreeLeafNode = {
      ...leaf,
      keys: leaf.keys.slice(0, mid),
      rowIds: leaf.rowIds.slice(0, mid),
      nextLeafId: newLeaf.nodeId,
    };
    this.writeNode(updatedLeaf);
    this.writeNode(newLeaf);
    this.propagateSplit(
      meta,
      path,
      path.length - 2,
      newLeaf.keys[0],
      newLeaf.nodeId,
    );
  }

  private propagateSplit(
    meta: BTreeMeta,
    path: BTreeNode[],
    parentIdx: number,
    key: IndexKey,
    rightChildId: number,
  ): void {
    if (parentIdx < 0) {
      this.createNewRoot(meta, key, rightChildId);
      return;
    }
    const parent = path[parentIdx] as BTreeInternalNode;
    const pos = this.bisectRight(parent.keys, key);
    const newParent: BTreeInternalNode = {
      ...parent,
      keys: [...parent.keys.slice(0, pos), key, ...parent.keys.slice(pos)],
      children: [
        ...parent.children.slice(0, pos + 1),
        rightChildId,
        ...parent.children.slice(pos + 1),
      ],
    };
    this.writeNode(newParent);
    if (newParent.keys.length >= ORDER)
      this.splitInternal(meta, newParent, path, parentIdx);
  }

  private splitInternal(
    meta: BTreeMeta,
    node: BTreeInternalNode,
    path: BTreeNode[],
    nodeIdx: number,
  ): void {
    const mid = node.keys.length >>> 1;
    const promotedKey = node.keys[mid];
    const newPageNo = this.ps.allocPage();
    const newNode: BTreeInternalNode = {
      kind: "internal",
      nodeId: newPageNo,
      keys: node.keys.slice(mid + 1),
      children: node.children.slice(mid + 1),
    };
    const updatedNode: BTreeInternalNode = {
      ...node,
      keys: node.keys.slice(0, mid),
      children: node.children.slice(0, mid + 1),
    };
    this.writeNode(updatedNode);
    this.writeNode(newNode);
    this.propagateSplit(meta, path, nodeIdx - 1, promotedKey, newNode.nodeId);
  }

  private createNewRoot(
    meta: BTreeMeta,
    key: IndexKey,
    rightChildId: number,
  ): void {
    const nodeId = this.ps.allocPage();
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

  private findLeafPath(meta: BTreeMeta, key: IndexKey): BTreeNode[] {
    const path: BTreeNode[] = [];
    let node = this.readNode(meta.rootNodeId);
    path.push(node);
    while (node.kind === "internal") {
      node = this.readNode(node.children[this.bisectRight(node.keys, key)]);
      path.push(node);
    }
    return path;
  }

  private bisectLeft(keys: IndexKey[], key: IndexKey): number {
    let lo = 0,
      hi = keys.length;
    while (lo < hi) {
      const mid = (lo + hi) >>> 1;
      if (compareIndexKeys(keys[mid], key) < 0) lo = mid + 1;
      else hi = mid;
    }
    return lo;
  }

  private bisectRight(keys: IndexKey[], key: IndexKey): number {
    let lo = 0,
      hi = keys.length;
    while (lo < hi) {
      const mid = (lo + hi) >>> 1;
      if (compareIndexKeys(key, keys[mid]) < 0) hi = mid;
      else lo = mid + 1;
    }
    return lo;
  }

  // --- Storage --------------------------------------------------------------

  private readMeta(): BTreeMeta {
    const meta = this.ps.readPage<BTreeMeta>(this.metaPageNo);
    if (!meta) throw new Error(`Index B-tree meta at page ${this.metaPageNo} not found`);
    return meta;
  }

  private writeMeta(meta: BTreeMeta): void {
    this.ps.writePage(this.metaPageNo, meta);
  }

  private readNode(pageNo: number): BTreeNode {
    const node = this.ps.readPage<BTreeNode>(pageNo);
    if (!node) throw new Error(`B-tree node at page ${pageNo} not found`);
    return node;
  }

  private writeNode(node: BTreeNode): void {
    this.ps.writePage(node.nodeId, node);
  }

  private createLeaf(
    keys: IndexKey[],
    rowIds: RowId[][],
    nextLeafId: number | null,
  ): number {
    const nodeId = this.ps.allocPage();
    this.writeNode({ kind: "leaf", nodeId, keys, rowIds, nextLeafId });
    return nodeId;
  }
}
