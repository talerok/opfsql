export type IndexKeyValue = string | number | boolean | null;
export type IndexKey = IndexKeyValue[];

export interface BTreeLeafNode {
  kind: "leaf";
  nodeId: number;
  // Each key is [...userKey, rowId]. The trailing rowId is also the entry's
  // rowId — no separate parallel array needed; see extractRowId helper.
  keys: IndexKey[];
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
}

export interface RangeOptions {
  lower?: IndexKey;
  upper?: IndexKey;
  lowerInclusive?: boolean;
  upperInclusive?: boolean;
}

export const ORDER = 128;
