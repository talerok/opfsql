// What a B-tree needs from the pages under it. A reader sees one snapshot; a
// writer edits private copies that become visible when it commits.

export interface PageReader {
  readonly pageSize: number;
  /** The page as of the reader's snapshot. Shared: never modify it. */
  read(pageNo: number): Uint8Array;
}

export interface PageWriter extends PageReader {
  /** This transaction's writable copy of the page, the same one every call. */
  write(pageNo: number): Uint8Array;
  /** A new page, all zero, already writable. */
  allocate(): number;
  free(pageNo: number): void;
}
