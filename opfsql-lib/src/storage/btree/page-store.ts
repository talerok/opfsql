// ---------------------------------------------------------------------------
// What a B-tree needs from the pages under it. A reader sees one snapshot; a
// writer edits private copies that become visible when it commits.
// ---------------------------------------------------------------------------

export interface PageReader {
  readonly pageSize: number;
  /** The page as of the reader's snapshot. Shared: never modify it. */
  read(pageNo: number): Uint8Array;
  /**
   * A number that changes whenever the tree rooted at `root` changes or the
   * pages roll back: what a cursor or a row holds of a tree is current only
   * while it stays the same (docs/layers.md). Constant for a reader that
   * never changes.
   */
  treeVersion(root: number): number;
}

export interface PageWriter extends PageReader {
  /**
   * This transaction's writable copy of the page: the same one every call,
   * until the page is freed or a rollback puts back an earlier copy.
   */
  write(pageNo: number): Uint8Array;
  /** A new page, all zero, already writable. */
  allocate(): number;
  free(pageNo: number): void;
  /** Records that the tree rooted at `root` is changing. */
  noteChange(root: number): void;
}
