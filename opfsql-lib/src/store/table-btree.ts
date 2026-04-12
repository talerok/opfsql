import type { SyncIKVStore, Row, RowId } from './types.js';

const NODE_ID_WIDTH = 8;
const ORDER = 128;

export interface TableLeafNode {
  kind: 'leaf';
  nodeId: number;
  keys: number[];
  values: Row[];
  nextLeafId: number | null;
}

export interface TableInternalNode {
  kind: 'internal';
  nodeId: number;
  keys: number[];
  children: number[];
}

export type TableNode = TableLeafNode | TableInternalNode;

export interface TableBTreeMeta {
  rootNodeId: number;
  height: number;
  nextNodeId: number;
  nextRowId: number;
  size: number;
}

export class SyncTableBTree {
  constructor(
    private readonly tableName: string,
    private readonly kv: SyncIKVStore,
  ) {}

  insert(row: Row): RowId {
    const baseMeta = this.readMeta();
    const meta: TableBTreeMeta = baseMeta ? { ...baseMeta } : this.initEmpty();
    const rowId = meta.nextRowId++;
    const path = this.findLeafPath(meta, rowId);
    const leaf = path.at(-1) as TableLeafNode;

    const pos = this.bisectLeft(leaf.keys, rowId);
    const newLeaf: TableLeafNode = {
      ...leaf,
      keys: [...leaf.keys.slice(0, pos), rowId, ...leaf.keys.slice(pos)],
      values: [...leaf.values.slice(0, pos), row, ...leaf.values.slice(pos)],
    };
    meta.size++;
    path[path.length - 1] = newLeaf;

    if (newLeaf.keys.length >= ORDER) this.splitLeaf(meta, newLeaf, path);
    else this.writeNode(newLeaf);
    this.writeMeta(meta);
    return rowId;
  }

  get(rowId: RowId): Row | null {
    const meta = this.readMeta();
    if (!meta || meta.size === 0) return null;
    const leaf = this.findLeafPath(meta, rowId).at(-1) as TableLeafNode;
    const pos = this.bisectLeft(leaf.keys, rowId);
    if (pos < leaf.keys.length && leaf.keys[pos] === rowId) return leaf.values[pos];
    return null;
  }

  update(rowId: RowId, row: Row): void {
    const meta = this.readMeta();
    if (!meta) throw new Error(`Row ${rowId} not found in table "${this.tableName}"`);
    const leaf = this.findLeafPath(meta, rowId).at(-1) as TableLeafNode;
    const pos = this.bisectLeft(leaf.keys, rowId);
    if (pos >= leaf.keys.length || leaf.keys[pos] !== rowId)
      throw new Error(`Row ${rowId} not found in table "${this.tableName}"`);
    this.writeNode({ ...leaf, values: leaf.values.map((v, i) => (i === pos ? row : v)) });
  }

  delete(rowId: RowId): void {
    const baseMeta = this.readMeta();
    if (!baseMeta) return;
    const meta = { ...baseMeta };
    const leaf = this.findLeafPath(meta, rowId).at(-1) as TableLeafNode;
    const pos = this.bisectLeft(leaf.keys, rowId);
    if (pos >= leaf.keys.length || leaf.keys[pos] !== rowId) return;
    this.writeNode({
      ...leaf,
      keys: [...leaf.keys.slice(0, pos), ...leaf.keys.slice(pos + 1)],
      values: [...leaf.values.slice(0, pos), ...leaf.values.slice(pos + 1)],
    });
    meta.size--;
    this.writeMeta(meta);
  }

  *scan(): Generator<{ rowId: RowId; row: Row }> {
    const meta = this.readMeta();
    if (!meta || meta.size === 0) return;
    let leaf: TableLeafNode | null = this.findLeftmostLeaf(meta);
    while (leaf) {
      for (let i = 0; i < leaf.keys.length; i++) yield { rowId: leaf.keys[i], row: leaf.values[i] };
      leaf = leaf.nextLeafId !== null ? (this.readNode(leaf.nextLeafId) as TableLeafNode) : null;
    }
  }

  drop(): void {
    for (const key of this.kv.getAllKeys(`table:${this.tableName}:`)) this.kv.deleteKey(key);
  }

  // --- Split ---------------------------------------------------------------

  private splitLeaf(meta: TableBTreeMeta, leaf: TableLeafNode, path: TableNode[]): void {
    const mid = leaf.keys.length >>> 1;
    const newLeaf: TableLeafNode = {
      kind: 'leaf', nodeId: this.allocNodeId(meta),
      keys: leaf.keys.slice(mid), values: leaf.values.slice(mid),
      nextLeafId: leaf.nextLeafId,
    };
    const updatedLeaf: TableLeafNode = {
      ...leaf, keys: leaf.keys.slice(0, mid), values: leaf.values.slice(0, mid),
      nextLeafId: newLeaf.nodeId,
    };
    this.writeNode(updatedLeaf);
    this.writeNode(newLeaf);
    this.propagateSplit(meta, path, path.length - 2, newLeaf.keys[0], newLeaf.nodeId);
  }

  private propagateSplit(
    meta: TableBTreeMeta, path: TableNode[], parentIdx: number,
    key: number, rightChildId: number,
  ): void {
    if (parentIdx < 0) { this.createNewRoot(meta, key, rightChildId); return; }
    const parent = path[parentIdx] as TableInternalNode;
    const pos = this.bisectRight(parent.keys, key);
    const newParent: TableInternalNode = {
      ...parent,
      keys: [...parent.keys.slice(0, pos), key, ...parent.keys.slice(pos)],
      children: [...parent.children.slice(0, pos + 1), rightChildId, ...parent.children.slice(pos + 1)],
    };
    this.writeNode(newParent);
    if (newParent.keys.length >= ORDER) this.splitInternal(meta, newParent, path, parentIdx);
  }

  private splitInternal(
    meta: TableBTreeMeta, node: TableInternalNode, path: TableNode[], nodeIdx: number,
  ): void {
    const mid = node.keys.length >>> 1;
    const promotedKey = node.keys[mid];
    const newNode: TableInternalNode = {
      kind: 'internal', nodeId: this.allocNodeId(meta),
      keys: node.keys.slice(mid + 1), children: node.children.slice(mid + 1),
    };
    const updatedNode: TableInternalNode = {
      ...node, keys: node.keys.slice(0, mid), children: node.children.slice(0, mid + 1),
    };
    this.writeNode(updatedNode);
    this.writeNode(newNode);
    this.propagateSplit(meta, path, nodeIdx - 1, promotedKey, newNode.nodeId);
  }

  private createNewRoot(meta: TableBTreeMeta, key: number, rightChildId: number): void {
    const nodeId = this.allocNodeId(meta);
    this.writeNode({ kind: 'internal', nodeId, keys: [key], children: [meta.rootNodeId, rightChildId] });
    meta.rootNodeId = nodeId;
    meta.height++;
  }

  // --- Traversal -----------------------------------------------------------

  private findLeafPath(meta: TableBTreeMeta, key: number): TableNode[] {
    const path: TableNode[] = [];
    let node = this.readNode(meta.rootNodeId);
    path.push(node);
    while (node.kind === 'internal') {
      node = this.readNode(node.children[this.bisectRight(node.keys, key)]);
      path.push(node);
    }
    return path;
  }

  private findLeftmostLeaf(meta: TableBTreeMeta): TableLeafNode {
    let node = this.readNode(meta.rootNodeId);
    while (node.kind === 'internal') node = this.readNode(node.children[0]);
    return node;
  }

  private bisectLeft(keys: number[], key: number): number {
    let lo = 0, hi = keys.length;
    while (lo < hi) { const mid = (lo + hi) >>> 1; if (keys[mid] < key) lo = mid + 1; else hi = mid; }
    return lo;
  }

  private bisectRight(keys: number[], key: number): number {
    let lo = 0, hi = keys.length;
    while (lo < hi) { const mid = (lo + hi) >>> 1; if (key < keys[mid]) hi = mid; else lo = mid + 1; }
    return lo;
  }

  // --- Storage -------------------------------------------------------------

  private metaKey = () => `table:${this.tableName}:meta`;
  private nodeKey = (id: number) => `table:${this.tableName}:node:${String(id).padStart(NODE_ID_WIDTH, '0')}`;

  private readMeta(): TableBTreeMeta | null { return this.kv.readKey<TableBTreeMeta>(this.metaKey()); }
  private writeMeta(meta: TableBTreeMeta): void { this.kv.writeKey(this.metaKey(), meta); }

  private readNode(nodeId: number): TableNode {
    const node = this.kv.readKey<TableNode>(this.nodeKey(nodeId));
    if (!node) throw new Error(`Table B-tree node ${nodeId} not found in "${this.tableName}"`);
    return node;
  }

  private writeNode(node: TableNode): void { this.kv.writeKey(this.nodeKey(node.nodeId), node); }
  private allocNodeId(meta: TableBTreeMeta): number { return meta.nextNodeId++; }

  private initEmpty(): TableBTreeMeta {
    const meta: TableBTreeMeta = { rootNodeId: 0, height: 1, nextNodeId: 0, nextRowId: 0, size: 0 };
    const nodeId = this.allocNodeId(meta);
    this.writeNode({ kind: 'leaf', nodeId, keys: [], values: [], nextLeafId: null });
    meta.rootNodeId = nodeId;
    return meta;
  }
}
