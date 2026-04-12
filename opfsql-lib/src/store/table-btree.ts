import type { IKVStore, Row, RowId } from "./types.js";
const NODE_ID_WIDTH = 8;
const ORDER = 128;

// ---------------------------------------------------------------------------
// Node types (separate from index B-tree)
// ---------------------------------------------------------------------------

export interface TableLeafNode {
  kind: "leaf";
  nodeId: number;
  keys: number[]; // rowIds, sorted ascending
  values: Row[]; // values[i] = row data for keys[i]
  nextLeafId: number | null;
}

export interface TableInternalNode {
  kind: "internal";
  nodeId: number;
  keys: number[]; // separator keys
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

// ---------------------------------------------------------------------------
// TableBTree
// ---------------------------------------------------------------------------

export class TableBTree {
  constructor(
    private readonly tableName: string,
    private readonly kv: IKVStore,
  ) {}

  // --- Public API ----------------------------------------------------------

  async insert(row: Row): Promise<RowId> {
    const baseMeta = await this.readMeta();
    const meta: TableBTreeMeta = baseMeta ? { ...baseMeta } : this.initEmpty();
    const rowId = meta.nextRowId++;
    const path = await this.findLeafPath(meta, rowId);
    const leaf = path.at(-1) as TableLeafNode;

    const pos = this.bisectLeft(leaf.keys, rowId);
    const newLeaf: TableLeafNode = {
      ...leaf,
      keys: [...leaf.keys.slice(0, pos), rowId, ...leaf.keys.slice(pos)],
      values: [...leaf.values.slice(0, pos), row, ...leaf.values.slice(pos)],
    };
    meta.size++;
    path[path.length - 1] = newLeaf;

    if (newLeaf.keys.length >= ORDER) {
      await this.splitLeaf(meta, newLeaf, path);
    } else {
      this.writeNode(newLeaf);
    }
    this.writeMeta(meta);
    return rowId;
  }

  async get(rowId: RowId): Promise<Row | null> {
    const meta = await this.readMeta();
    if (!meta || meta.size === 0) return null;

    const leaf = (await this.findLeafPath(meta, rowId)).at(-1) as TableLeafNode;
    const pos = this.bisectLeft(leaf.keys, rowId);
    if (pos < leaf.keys.length && leaf.keys[pos] === rowId) {
      return leaf.values[pos];
    }
    return null;
  }

  async update(rowId: RowId, row: Row): Promise<void> {
    const meta = await this.readMeta();
    if (!meta)
      throw new Error(`Row ${rowId} not found in table "${this.tableName}"`);

    const leaf = (await this.findLeafPath(meta, rowId)).at(-1) as TableLeafNode;
    const pos = this.bisectLeft(leaf.keys, rowId);
    if (pos >= leaf.keys.length || leaf.keys[pos] !== rowId) {
      throw new Error(`Row ${rowId} not found in table "${this.tableName}"`);
    }
    const newLeaf: TableLeafNode = {
      ...leaf,
      values: leaf.values.map((v, i) => (i === pos ? row : v)),
    };
    this.writeNode(newLeaf);
  }

  async delete(rowId: RowId): Promise<void> {
    const baseMeta = await this.readMeta();
    if (!baseMeta) return;
    const meta = { ...baseMeta };

    const leaf = (await this.findLeafPath(meta, rowId)).at(-1) as TableLeafNode;
    const pos = this.bisectLeft(leaf.keys, rowId);
    if (pos >= leaf.keys.length || leaf.keys[pos] !== rowId) return;

    const newLeaf: TableLeafNode = {
      ...leaf,
      keys: [...leaf.keys.slice(0, pos), ...leaf.keys.slice(pos + 1)],
      values: [...leaf.values.slice(0, pos), ...leaf.values.slice(pos + 1)],
    };
    meta.size--;

    this.writeNode(newLeaf);
    this.writeMeta(meta);
  }

  async *scan(): AsyncGenerator<{ rowId: RowId; row: Row }> {
    const meta = await this.readMeta();
    if (!meta || meta.size === 0) return;

    let leaf: TableLeafNode | null = await this.findLeftmostLeaf(meta);
    while (leaf) {
      for (let i = 0; i < leaf.keys.length; i++) {
        yield { rowId: leaf.keys[i], row: leaf.values[i] };
      }
      leaf =
        leaf.nextLeafId !== null
          ? ((await this.readNode(leaf.nextLeafId)) as TableLeafNode)
          : null;
    }
  }

  async drop(): Promise<void> {
    const keys = await this.kv.getAllKeys(`table:${this.tableName}:`);
    for (const key of keys) this.kv.deleteKey(key);
  }

  // --- Split ---------------------------------------------------------------

  private async splitLeaf(
    meta: TableBTreeMeta,
    leaf: TableLeafNode,
    path: TableNode[],
  ): Promise<void> {
    const mid = leaf.keys.length >>> 1;

    const newLeaf: TableLeafNode = {
      kind: "leaf",
      nodeId: this.allocNodeId(meta),
      keys: leaf.keys.slice(mid),
      values: leaf.values.slice(mid),
      nextLeafId: leaf.nextLeafId,
    };
    const updatedLeaf: TableLeafNode = {
      ...leaf,
      keys: leaf.keys.slice(0, mid),
      values: leaf.values.slice(0, mid),
      nextLeafId: newLeaf.nodeId,
    };

    this.writeNode(updatedLeaf);
    this.writeNode(newLeaf);
    await this.propagateSplit(
      meta,
      path,
      path.length - 2,
      newLeaf.keys[0],
      newLeaf.nodeId,
    );
  }

  private async propagateSplit(
    meta: TableBTreeMeta,
    path: TableNode[],
    parentIdx: number,
    key: number,
    rightChildId: number,
  ): Promise<void> {
    if (parentIdx < 0) {
      this.createNewRoot(meta, key, rightChildId);
      return;
    }

    const parent = path[parentIdx] as TableInternalNode;
    const pos = this.bisectRight(parent.keys, key);
    const newParent: TableInternalNode = {
      ...parent,
      keys: [...parent.keys.slice(0, pos), key, ...parent.keys.slice(pos)],
      children: [
        ...parent.children.slice(0, pos + 1),
        rightChildId,
        ...parent.children.slice(pos + 1),
      ],
    };
    this.writeNode(newParent);

    if (newParent.keys.length >= ORDER) {
      await this.splitInternal(meta, newParent, path, parentIdx);
    }
  }

  private async splitInternal(
    meta: TableBTreeMeta,
    node: TableInternalNode,
    path: TableNode[],
    nodeIdx: number,
  ): Promise<void> {
    const mid = node.keys.length >>> 1;
    const promotedKey = node.keys[mid];

    const newNode: TableInternalNode = {
      kind: "internal",
      nodeId: this.allocNodeId(meta),
      keys: node.keys.slice(mid + 1),
      children: node.children.slice(mid + 1),
    };
    const updatedNode: TableInternalNode = {
      ...node,
      keys: node.keys.slice(0, mid),
      children: node.children.slice(0, mid + 1),
    };

    this.writeNode(updatedNode);
    this.writeNode(newNode);
    await this.propagateSplit(
      meta,
      path,
      nodeIdx - 1,
      promotedKey,
      newNode.nodeId,
    );
  }

  private createNewRoot(
    meta: TableBTreeMeta,
    key: number,
    rightChildId: number,
  ): void {
    const nodeId = this.allocNodeId(meta);
    this.writeNode({
      kind: "internal",
      nodeId,
      keys: [key],
      children: [meta.rootNodeId, rightChildId],
    });
    meta.rootNodeId = nodeId;
    meta.height++;
  }

  // --- Traversal -----------------------------------------------------------

  private async findLeafPath(
    meta: TableBTreeMeta,
    key: number,
  ): Promise<TableNode[]> {
    const path: TableNode[] = [];
    let node = await this.readNode(meta.rootNodeId);
    path.push(node);

    while (node.kind === "internal") {
      const childIdx = this.bisectRight(node.keys, key);
      node = await this.readNode(node.children[childIdx]);
      path.push(node);
    }
    return path;
  }

  private async findLeftmostLeaf(meta: TableBTreeMeta): Promise<TableLeafNode> {
    let node = await this.readNode(meta.rootNodeId);
    while (node.kind === "internal") {
      node = await this.readNode(node.children[0]);
    }
    return node;
  }

  /** First position where keys[pos] >= key. */
  private bisectLeft(keys: number[], key: number): number {
    let lo = 0,
      hi = keys.length;
    while (lo < hi) {
      const mid = (lo + hi) >>> 1;
      if (keys[mid] < key) lo = mid + 1;
      else hi = mid;
    }
    return lo;
  }

  /** First position where key < keys[pos]. */
  private bisectRight(keys: number[], key: number): number {
    let lo = 0,
      hi = keys.length;
    while (lo < hi) {
      const mid = (lo + hi) >>> 1;
      if (key < keys[mid]) hi = mid;
      else lo = mid + 1;
    }
    return lo;
  }

  // --- Storage access ------------------------------------------------------

  private metaKey(): string {
    return `table:${this.tableName}:meta`;
  }

  private nodeKey(nodeId: number): string {
    return `table:${this.tableName}:node:${String(nodeId).padStart(
      NODE_ID_WIDTH,
      "0",
    )}`;
  }

  private async readMeta(): Promise<TableBTreeMeta | null> {
    return this.kv.readKey<TableBTreeMeta>(this.metaKey());
  }

  private writeMeta(meta: TableBTreeMeta): void {
    this.kv.writeKey(this.metaKey(), meta);
  }

  private async readNode(nodeId: number): Promise<TableNode> {
    const node = await this.kv.readKey<TableNode>(this.nodeKey(nodeId));
    if (!node)
      throw new Error(
        `Table B-tree node ${nodeId} not found in "${this.tableName}"`,
      );
    return node;
  }

  private writeNode(node: TableNode): void {
    this.kv.writeKey(this.nodeKey(node.nodeId), node);
  }

  private allocNodeId(meta: TableBTreeMeta): number {
    return meta.nextNodeId++;
  }

  private initEmpty(): TableBTreeMeta {
    const meta: TableBTreeMeta = {
      rootNodeId: 0,
      height: 1,
      nextNodeId: 0,
      nextRowId: 0,
      size: 0,
    };
    const nodeId = this.allocNodeId(meta);
    this.writeNode({
      kind: "leaf",
      nodeId,
      keys: [],
      values: [],
      nextLeafId: null,
    });
    meta.rootNodeId = nodeId;
    return meta;
  }
}
