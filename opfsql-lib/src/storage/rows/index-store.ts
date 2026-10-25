import { IndexKeyTooLargeError } from "../../errors.js";
import { BTree, BTreeWriter } from "../btree/btree.js";
import type { PageReader, PageWriter } from "../btree/page-store.js";
import { indexLeafCell } from "../format/btree-page.js";
import { varintSize } from "../format/bytes.js";

// ---------------------------------------------------------------------------
// The entries of an index: encoded keys (format/key.ts), each ending with the
// rowid of its row, in byte order.
// ---------------------------------------------------------------------------

const CHILD_SIZE = 4;

export class IndexReader {
  protected readonly tree: BTree;

  constructor(
    pages: PageReader,
    readonly root: number,
  ) {
    this.tree = new BTree(pages, root, "index");
  }

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

  has(key: Uint8Array): boolean {
    return this.tree.find(key) !== null;
  }
}

export class IndexWriter extends IndexReader {
  private readonly writer: BTreeWriter;

  /** `index` names the index in errors. */
  constructor(
    pages: PageWriter,
    root: number,
    private readonly index: string,
  ) {
    super(pages, root);
    this.writer = new BTreeWriter(pages, root, "index");
  }

  /** Allocates the root page of a new, empty index. */
  static create(pages: PageWriter): number {
    return BTreeWriter.create(pages, "index");
  }

  /** The longest key the index accepts: one whose interior cell fits a quarter page. */
  get maxKeySize(): number {
    const cell = this.writer.maxInteriorCellSize();
    return cell - CHILD_SIZE - varintSize(cell);
  }

  /** Adds `key`. False, changing nothing, if it is present. */
  insert(key: Uint8Array): boolean {
    if (key.length > this.maxKeySize) {
      throw new IndexKeyTooLargeError(this.index, key.length, this.maxKeySize);
    }
    return this.writer.insert(key, indexLeafCell(key));
  }

  /** Removes `key`. False if it is absent. */
  delete(key: Uint8Array): boolean {
    return this.writer.delete(key);
  }

  drop(): void {
    this.writer.drop();
  }
}
