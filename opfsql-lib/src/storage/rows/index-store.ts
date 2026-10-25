import { IndexKeyTooLargeError } from "../../errors.js";
import { BTree, BTreeWriter } from "../btree/btree.js";
import type { PageReader, PageWriter } from "../btree/page-store.js";
import { compareBytes } from "../../values/index.js";
import { indexLeafCell, maxIndexKeySize } from "../format/btree-page.js";
import { prefixEnd } from "../format/key.js";

// ---------------------------------------------------------------------------
// The entries of an index: encoded keys (format/key.ts), each ending with the
// rowid of its row, in byte order.
// ---------------------------------------------------------------------------

export class IndexReader {
  /** `tree` is for IndexWriter, which reads through the tree it writes. */
  constructor(
    pages: PageReader,
    readonly root: number,
    protected readonly tree: BTree = new BTree(pages, root, "index"),
  ) {}

  /** Keys from the first at least `from` (or the first) on, in order. */
  *keysFrom(from?: Uint8Array): Generator<Uint8Array> {
    for (const { page, index } of this.tree.ascending(from)) {
      yield page.key(index).slice();
    }
  }

  /** Keys below `before` (or all), in reverse order. */
  *keysBefore(before?: Uint8Array): Generator<Uint8Array> {
    for (const { page, index } of this.tree.descending(before)) {
      yield page.key(index).slice();
    }
  }

  /** Keys that start with `prefix`: entries whose first values are those `prefix` encodes. */
  *keysWithPrefix(prefix: Uint8Array): Generator<Uint8Array> {
    const end = prefixEnd(prefix);
    for (const key of this.keysFrom(prefix)) {
      if (compareBytes(key, end) >= 0) return;
      yield key;
    }
  }

  has(key: Uint8Array): boolean {
    return this.tree.find(key) !== null;
  }
}

export class IndexWriter extends IndexReader {
  /** The longest key the index accepts: about a quarter of a page. */
  readonly maxKeySize: number;
  // The reader's tree, typed for writing.
  declare protected readonly tree: BTreeWriter;

  /** `index` names the index in errors. */
  constructor(
    pages: PageWriter,
    root: number,
    private readonly index: string,
  ) {
    super(pages, root, new BTreeWriter(pages, root, "index"));
    this.maxKeySize = maxIndexKeySize(pages.pageSize);
  }

  /** Allocates the root page of a new, empty index. */
  static create(pages: PageWriter): number {
    return BTreeWriter.create(pages, "index");
  }

  /** Adds `key`. False, changing nothing, if it is present. */
  insert(key: Uint8Array): boolean {
    if (key.length > this.maxKeySize) {
      throw new IndexKeyTooLargeError(this.index, key.length, this.maxKeySize);
    }
    return this.tree.insert(key, indexLeafCell(key));
  }

  /** Removes `key`. False if it is absent. */
  delete(key: Uint8Array): boolean {
    return this.tree.delete(key);
  }

  drop(): void {
    this.tree.drop();
  }
}
