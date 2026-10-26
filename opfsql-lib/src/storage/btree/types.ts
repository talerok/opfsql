import type { BTreePage, BTreePageType } from "../format/btree-page.js";

// ---------------------------------------------------------------------------
// What the B-tree modules share: the two kinds of tree, their keys, and where
// a cell or a walk over a leaf is.
// ---------------------------------------------------------------------------

export type TreeKind = "table" | "index";

/** A rowid in a table tree, an encoded key in an index tree. */
export type TreeKey = number | Uint8Array;

/** A cell in a leaf: read its rowid, record or key from the page. */
export interface Position {
  readonly page: BTreePage;
  readonly index: number;
}

/**
 * A leaf and the cell a walk over it starts at. The start may be one past
 * either end when the walk has no cell in this leaf: not a Position.
 */
export interface LeafStart {
  readonly leaf: BTreePage;
  readonly start: number;
}

export const LEAF_TYPE: Record<TreeKind, BTreePageType> = {
  table: "table-leaf",
  index: "index-leaf",
};

export const INTERIOR_TYPE: Record<TreeKind, BTreePageType> = {
  table: "table-interior",
  index: "index-interior",
};

/** The type a page of a `kind` tree has, as a leaf or an interior page. */
export function pageTypeOf(kind: TreeKind, isLeaf: boolean): BTreePageType {
  return isLeaf ? LEAF_TYPE[kind] : INTERIOR_TYPE[kind];
}
