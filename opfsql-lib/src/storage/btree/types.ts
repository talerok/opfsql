import type { BTreePage } from "../format/btree-page.js";

export type TreeKind = "table" | "index";

/** A rowid in a table tree, an encoded key in an index tree. */
export type TreeKey = number | Uint8Array;

/** A cell in a leaf: read its rowid, record or key from the page. */
export interface Position {
  readonly page: BTreePage;
  readonly index: number;
}
