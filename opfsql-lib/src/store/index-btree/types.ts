import type { RowId } from "../types.js";

export type IndexKeyValue = string | number | boolean | null;
export type IndexKey = IndexKeyValue[];

export interface BTreeLeafNode {
  kind: "leaf";
  nodeId: number;
  keys: IndexKey[];
  rowIds: RowId[][];
  nextLeafId: number | null;
}

export interface BTreeInternalNode {
  kind: "internal";
  nodeId: number;
  keys: IndexKey[];
  children: number[];
}

export type BTreeNode = BTreeLeafNode | BTreeInternalNode;

export interface BTreeMeta {
  rootNodeId: number;
  height: number;
  size: number;
  unique: boolean;
}

export const ORDER = 128;
