import type { RowId } from "../types.js";

// ---------------------------------------------------------------------------
// Index key types
// ---------------------------------------------------------------------------

export type IndexKeyValue = string | number | boolean | null;

/** Composite key: one element per indexed column. */
export type IndexKey = IndexKeyValue[];

// ---------------------------------------------------------------------------
// B-tree node types
// ---------------------------------------------------------------------------

export interface BTreeLeafNode {
  kind: "leaf";
  nodeId: number;
  keys: IndexKey[];
  /** rowIds[i] = array of RowId for keys[i]. Non-unique indexes may have multiple. */
  rowIds: RowId[][];
  /** Pointer to the next leaf for efficient range scans. */
  nextLeafId: number | null;
}

export interface BTreeInternalNode {
  kind: "internal";
  nodeId: number;
  /** Separator keys. keys.length === children.length - 1. */
  keys: IndexKey[];
  /** Child node IDs. */
  children: number[];
}

export type BTreeNode = BTreeLeafNode | BTreeInternalNode;

// ---------------------------------------------------------------------------
// B-tree metadata (stored separately from nodes)
// ---------------------------------------------------------------------------

export interface BTreeMeta {
  rootNodeId: number;
  /** Tree height. 1 means root is a leaf. */
  height: number;
  /** Monotonically increasing node ID allocator. */
  nextNodeId: number;
  /** Total number of index entries. */
  size: number;
  /** Whether this index enforces uniqueness. */
  unique: boolean;
}

// ---------------------------------------------------------------------------
// Constants
// ---------------------------------------------------------------------------

/** Maximum keys per leaf node. Internal nodes hold ORDER-1 keys and ORDER children. */
export const ORDER = 128;
