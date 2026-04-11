import type { IKVStore, RowId } from '../types.js';
import type {
  BTreeMeta,
  BTreeNode,
  BTreeLeafNode,
  BTreeInternalNode,
  IndexKey,
} from './types.js';
import { ORDER } from './types.js';
import { compareIndexKeys, keyHasNull } from './compare.js';
import { computeBounds, type ScanBounds, type SearchPredicate } from './search-bounds.js';
export type { SearchPredicate } from './search-bounds.js';

const NODE_ID_WIDTH = 8;

export class BTree {
  constructor(
    private readonly indexName: string,
    private readonly pm: IKVStore,
    private readonly unique: boolean,
  ) {}

  // --- Public API -----------------------------------------------------------

  async insert(key: IndexKey, rowId: RowId): Promise<void> {
    const meta = (await this.readMeta()) ?? this.initEmptyTree();
    const path = await this.findLeafPath(meta, key);
    const leaf = path.at(-1) as BTreeLeafNode;

    this.insertIntoLeaf(leaf, key, rowId);
    meta.size++;

    if (leaf.keys.length >= ORDER) {
      await this.splitLeaf(meta, leaf, path);
    } else {
      this.writeNode(leaf);
    }
    this.writeMeta(meta);
  }

  async delete(key: IndexKey, rowId: RowId): Promise<void> {
    const meta = await this.readMeta();
    if (!meta) return;

    const leaf = (await this.findLeafPath(meta, key)).at(-1) as BTreeLeafNode;
    if (!this.removeFromLeaf(leaf, key, rowId)) return;

    meta.size--;
    this.writeNode(leaf);
    this.writeMeta(meta);
  }

  async search(predicates: SearchPredicate[], totalColumns?: number): Promise<RowId[]> {
    const meta = await this.readMeta();
    if (!meta || meta.size === 0) return [];
    return this.rangeScan(meta, computeBounds(predicates, totalColumns));
  }

  async bulkLoad(entries: Array<{ key: IndexKey; rowId: RowId }>): Promise<void> {
    const meta: BTreeMeta = { rootNodeId: 0, height: 1, nextNodeId: 0, size: 0 };

    if (entries.length === 0) {
      meta.rootNodeId = this.createLeaf(meta, [], [], null);
      this.writeMeta(meta);
      return;
    }

    const merged = this.mergeEntries(entries);
    meta.size = entries.length;

    const leaves = this.buildLeaves(meta, merged);
    this.linkAndWriteLeaves(leaves);

    meta.rootNodeId = leaves.length === 1
      ? leaves[0].nodeId
      : this.buildInternalLevels(meta, leaves);
    this.writeMeta(meta);
  }

  async drop(): Promise<void> {
    const keys = await this.pm.getAllKeys(`btree:${this.indexName}:`);
    for (const key of keys) this.pm.deleteKey(key);
  }

  // --- Leaf operations ------------------------------------------------------

  private insertIntoLeaf(leaf: BTreeLeafNode, key: IndexKey, rowId: RowId): void {
    const pos = this.bisectLeft(leaf.keys, key);
    const isExisting = pos < leaf.keys.length && compareIndexKeys(leaf.keys[pos], key) === 0;

    if (isExisting) {
      if (this.unique && !keyHasNull(key)) {
        throw new Error(`UNIQUE constraint failed: index "${this.indexName}"`);
      }
      leaf.rowIds[pos].push(rowId);
    } else {
      leaf.keys.splice(pos, 0, key);
      leaf.rowIds.splice(pos, 0, [rowId]);
    }
  }

  private removeFromLeaf(leaf: BTreeLeafNode, key: IndexKey, rowId: RowId): boolean {
    const pos = this.bisectLeft(leaf.keys, key);
    if (pos >= leaf.keys.length || compareIndexKeys(leaf.keys[pos], key) !== 0) return false;

    const bucket = leaf.rowIds[pos];
    const idx = bucket.indexOf(rowId);
    if (idx === -1) return false;

    bucket.splice(idx, 1);
    if (bucket.length === 0) {
      leaf.keys.splice(pos, 1);
      leaf.rowIds.splice(pos, 1);
    }
    return true;
  }

  // --- Range scan -----------------------------------------------------------

  private async rangeScan(meta: BTreeMeta, bounds: ScanBounds): Promise<RowId[]> {
    if (bounds.exactKey) {
      return this.pointLookup(meta, bounds.exactKey);
    }

    const startLeaf = bounds.lowerKey
      ? (await this.findLeafPath(meta, bounds.lowerKey)).at(-1) as BTreeLeafNode
      : await this.findLeftmostLeaf(meta);

    return this.collectRange(startLeaf, bounds);
  }

  private async pointLookup(meta: BTreeMeta, key: IndexKey): Promise<RowId[]> {
    const leaf = (await this.findLeafPath(meta, key)).at(-1) as BTreeLeafNode;
    const pos = this.bisectLeft(leaf.keys, key);

    if (pos < leaf.keys.length && compareIndexKeys(leaf.keys[pos], key) === 0) {
      return [...leaf.rowIds[pos]];
    }
    return [];
  }

  private async findLeftmostLeaf(meta: BTreeMeta): Promise<BTreeLeafNode> {
    let node = await this.readNode(meta.rootNodeId);
    while (node.kind === 'internal') {
      node = await this.readNode(node.children[0]);
    }
    return node;
  }

  private async collectRange(startLeaf: BTreeLeafNode, bounds: ScanBounds): Promise<RowId[]> {
    const results: RowId[] = [];
    let leaf: BTreeLeafNode | null = startLeaf;

    while (leaf) {
      for (let i = 0; i < leaf.keys.length; i++) {
        const key = leaf.keys[i];
        if (this.isBelowLower(key, bounds)) continue;
        if (this.isAboveUpper(key, bounds)) return results;
        results.push(...leaf.rowIds[i]);
      }
      leaf = leaf.nextLeafId !== null
        ? await this.readNode(leaf.nextLeafId) as BTreeLeafNode
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
    if (bounds.prefixScan) {
      const prefix = key.slice(0, bounds.upperKey.length);
      return compareIndexKeys(prefix, bounds.upperKey) > 0;
    }
    const cmp = compareIndexKeys(key, bounds.upperKey);
    return bounds.upperInclusive ? cmp > 0 : cmp >= 0;
  }

  // --- Bulk load helpers ----------------------------------------------------

  private mergeEntries(entries: Array<{ key: IndexKey; rowId: RowId }>): Array<{ key: IndexKey; rowIds: RowId[] }> {
    const merged: Array<{ key: IndexKey; rowIds: RowId[] }> = [];

    for (const { key, rowId } of entries) {
      const last = merged.at(-1);
      if (last && compareIndexKeys(last.key, key) === 0) {
        if (this.unique && !keyHasNull(key)) {
          throw new Error(`UNIQUE constraint failed: index "${this.indexName}"`);
        }
        last.rowIds.push(rowId);
      } else {
        merged.push({ key, rowIds: [rowId] });
      }
    }
    return merged;
  }

  private buildLeaves(meta: BTreeMeta, merged: Array<{ key: IndexKey; rowIds: RowId[] }>): BTreeLeafNode[] {
    const leaves: BTreeLeafNode[] = [];
    for (let i = 0; i < merged.length; i += ORDER - 1) {
      const chunk = merged.slice(i, i + ORDER - 1);
      const nodeId = this.allocNodeId(meta);
      leaves.push({
        kind: 'leaf', nodeId,
        keys: chunk.map(e => e.key),
        rowIds: chunk.map(e => e.rowIds),
        nextLeafId: null,
      });
    }
    return leaves;
  }

  private linkAndWriteLeaves(leaves: BTreeLeafNode[]): void {
    for (let i = 0; i < leaves.length - 1; i++) {
      leaves[i].nextLeafId = leaves[i + 1].nodeId;
    }
    for (const leaf of leaves) this.writeNode(leaf);
  }

  private buildInternalLevels(meta: BTreeMeta, leaves: BTreeLeafNode[]): number {
    let level = leaves.map(l => ({ nodeId: l.nodeId, firstKey: l.keys[0] }));

    while (level.length > 1) {
      meta.height++;
      const next: Array<{ nodeId: number; firstKey: IndexKey }> = [];

      for (let i = 0; i < level.length; i += ORDER) {
        const chunk = level.slice(i, i + ORDER);
        const nodeId = this.allocNodeId(meta);
        this.writeNode({
          kind: 'internal', nodeId,
          keys: chunk.slice(1).map(c => c.firstKey),
          children: chunk.map(c => c.nodeId),
        });
        next.push({ nodeId, firstKey: chunk[0].firstKey });
      }
      level = next;
    }
    return level[0].nodeId;
  }

  // --- Split ----------------------------------------------------------------

  private async splitLeaf(meta: BTreeMeta, leaf: BTreeLeafNode, path: BTreeNode[]): Promise<void> {
    const mid = leaf.keys.length >>> 1;

    const newLeaf: BTreeLeafNode = {
      kind: 'leaf',
      nodeId: this.allocNodeId(meta),
      keys: leaf.keys.splice(mid),
      rowIds: leaf.rowIds.splice(mid),
      nextLeafId: leaf.nextLeafId,
    };
    leaf.nextLeafId = newLeaf.nodeId;

    this.writeNode(leaf);
    this.writeNode(newLeaf);
    await this.propagateSplit(meta, path, path.length - 2, newLeaf.keys[0], newLeaf.nodeId);
  }

  private async propagateSplit(
    meta: BTreeMeta, path: BTreeNode[], parentIdx: number,
    key: IndexKey, rightChildId: number,
  ): Promise<void> {
    if (parentIdx < 0) {
      this.createNewRoot(meta, key, rightChildId);
      return;
    }

    const parent = path[parentIdx] as BTreeInternalNode;
    const pos = this.bisectRight(parent.keys, key);
    parent.keys.splice(pos, 0, key);
    parent.children.splice(pos + 1, 0, rightChildId);
    this.writeNode(parent);

    if (parent.keys.length >= ORDER) {
      await this.splitInternal(meta, parent, path, parentIdx);
    }
  }

  private async splitInternal(
    meta: BTreeMeta, node: BTreeInternalNode,
    path: BTreeNode[], nodeIdx: number,
  ): Promise<void> {
    const mid = node.keys.length >>> 1;
    const promotedKey = node.keys[mid];

    const newNode: BTreeInternalNode = {
      kind: 'internal',
      nodeId: this.allocNodeId(meta),
      keys: node.keys.splice(mid + 1),
      children: node.children.splice(mid + 1),
    };
    node.keys.splice(mid, 1);

    this.writeNode(node);
    this.writeNode(newNode);
    await this.propagateSplit(meta, path, nodeIdx - 1, promotedKey, newNode.nodeId);
  }

  private createNewRoot(meta: BTreeMeta, key: IndexKey, rightChildId: number): void {
    const nodeId = this.allocNodeId(meta);
    this.writeNode({
      kind: 'internal', nodeId,
      keys: [key],
      children: [meta.rootNodeId, rightChildId],
    });
    meta.rootNodeId = nodeId;
    meta.height++;
  }

  // --- Tree traversal -------------------------------------------------------

  private async findLeafPath(meta: BTreeMeta, key: IndexKey): Promise<BTreeNode[]> {
    const path: BTreeNode[] = [];
    let node = await this.readNode(meta.rootNodeId);
    path.push(node);

    while (node.kind === 'internal') {
      const childIdx = this.bisectRight(node.keys, key);
      node = await this.readNode(node.children[childIdx]);
      path.push(node);
    }
    return path;
  }

  /** Find insertion point: first position where keys[pos] >= key. */
  private bisectLeft(keys: IndexKey[], key: IndexKey): number {
    let lo = 0, hi = keys.length;
    while (lo < hi) {
      const mid = (lo + hi) >>> 1;
      if (compareIndexKeys(keys[mid], key) < 0) lo = mid + 1;
      else hi = mid;
    }
    return lo;
  }

  /** Find child index: first position where key < keys[pos]. */
  private bisectRight(keys: IndexKey[], key: IndexKey): number {
    let lo = 0, hi = keys.length;
    while (lo < hi) {
      const mid = (lo + hi) >>> 1;
      if (compareIndexKeys(key, keys[mid]) < 0) hi = mid;
      else lo = mid + 1;
    }
    return lo;
  }

  // --- Storage access -------------------------------------------------------

  private metaKey(): string {
    return `btree:${this.indexName}:meta`;
  }

  private nodeKey(nodeId: number): string {
    return `btree:${this.indexName}:node:${String(nodeId).padStart(NODE_ID_WIDTH, '0')}`;
  }

  private async readMeta(): Promise<BTreeMeta | null> {
    return this.pm.readKey<BTreeMeta>(this.metaKey());
  }

  private writeMeta(meta: BTreeMeta): void {
    this.pm.writeKey(this.metaKey(), meta);
  }

  private async readNode(nodeId: number): Promise<BTreeNode> {
    const node = await this.pm.readKey<BTreeNode>(this.nodeKey(nodeId));
    if (!node) throw new Error(`B-tree node ${nodeId} not found in index "${this.indexName}"`);
    return node;
  }

  private writeNode(node: BTreeNode): void {
    this.pm.writeKey(this.nodeKey(node.nodeId), node);
  }

  private allocNodeId(meta: BTreeMeta): number {
    return meta.nextNodeId++;
  }

  private createLeaf(meta: BTreeMeta, keys: IndexKey[], rowIds: RowId[][], nextLeafId: number | null): number {
    const nodeId = this.allocNodeId(meta);
    this.writeNode({ kind: 'leaf', nodeId, keys, rowIds, nextLeafId });
    return nodeId;
  }

  private initEmptyTree(): BTreeMeta {
    const meta: BTreeMeta = { rootNodeId: 0, height: 1, nextNodeId: 0, size: 0 };
    meta.rootNodeId = this.createLeaf(meta, [], [], null);
    return meta;
  }
}
