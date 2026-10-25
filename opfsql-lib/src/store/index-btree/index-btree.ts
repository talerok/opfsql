import { OpfsqlError } from "../../errors.js";
import { BTree, type BTreeLayout } from "../btree.js";
import { IndexKeyTooLargeError } from "../errors.js";
import type { RowId, SyncIPageStore } from "../types.js";
import { compareIndexKeys, keyHasNull } from "./compare.js";
import { isAboveUpper, isBelowLower } from "./search-bounds.js";
import type { IndexKey, RangeOptions } from "./types.js";

export type { RangeOptions } from "./types.js";

// ---------------------------------------------------------------------------
// Stored keys in index B-tree are [...userKey, rowId] (SQLite-style).
// Every entry is unique by construction; duplicate user keys differ in the
// trailing rowId, so leaves split normally regardless of duplicate skew.
// The rowId is recovered from the last element of the stored key (see
// extractRowId). Range bounds are user-key-shaped; prefix semantics are built
// into isBelowLower / isAboveUpper via slicing to the bound length.
// compareIndexKeys' length tiebreaker makes [userKey] < [...userKey, rowId],
// so user-key bounds correctly frame every matching stored key.
// ---------------------------------------------------------------------------

/** Below every key whose first column is NULL, above every other: NULLs sort last. */
const NULL_FIRST_COLUMN: IndexKey = [null];

function extractRowId(storedKey: IndexKey): RowId {
  return storedKey[storedKey.length - 1] as RowId;
}

function toEntry(storedKey: IndexKey): { key: IndexKey; rowId: RowId } {
  return { key: storedKey.slice(0, -1), rowId: extractRowId(storedKey) };
}

function indexLayout(index: string): BTreeLayout<IndexKey, IndexKey> {
  return {
    keyOf: (storedKey) => storedKey,
    compare: compareIndexKeys,
    tooLarge: (size, limit) => new IndexKeyTooLargeError(index, size, limit),
  };
}

export class SyncBTree {
  private readonly tree: BTree<IndexKey, IndexKey>;

  constructor(
    pages: SyncIPageStore,
    rootPageNo: number,
    private readonly index: { name: string; unique: boolean },
  ) {
    this.tree = new BTree(pages, rootPageNo, indexLayout(index.name));
  }

  insert(userKey: IndexKey, rowId: RowId): void {
    if (
      this.index.unique &&
      !keyHasNull(userKey) &&
      this.lookup(userKey).length > 0
    )
      throw uniqueViolation(this.index.name);
    this.tree.insert([...userKey, rowId]);
  }

  delete(userKey: IndexKey, rowId: RowId): void {
    this.tree.delete([...userKey, rowId]);
  }

  lookup(userKey: IndexKey): RowId[] {
    return this.range({ lower: userKey, upper: userKey });
  }

  range(opts: RangeOptions = {}): RowId[] {
    const results: RowId[] = [];
    for (const key of this.tree.ascending(opts.lower)) {
      if (isBelowLower(key, opts)) continue;
      if (isAboveUpper(key, opts)) break;
      results.push(extractRowId(key));
    }
    return results;
  }

  /** Fills the empty index with `entries`, sorted by key and then rowId. */
  bulkLoad(entries: Array<{ key: IndexKey; rowId: RowId }>): void {
    if (this.index.unique) checkUniqueSorted(this.index.name, entries);
    this.tree.build(entries.map(({ key, rowId }) => [...key, rowId]));
  }

  drop(): void {
    this.tree.drop();
  }

  /** The entry with the smallest non-NULL first column, or null. */
  first(): { key: IndexKey; rowId: RowId } | null {
    for (const storedKey of this.tree.ascending()) {
      return storedKey[0] === null ? null : toEntry(storedKey);
    }
    return null;
  }

  /** The entry with the largest non-NULL first column, or null. */
  last(): { key: IndexKey; rowId: RowId } | null {
    for (const storedKey of this.tree.descending(NULL_FIRST_COLUMN)) {
      return toEntry(storedKey);
    }
    return null;
  }
}

function checkUniqueSorted(
  index: string,
  entries: Array<{ key: IndexKey; rowId: RowId }>,
): void {
  for (let i = 1; i < entries.length; i++) {
    if (
      compareIndexKeys(entries[i].key, entries[i - 1].key) === 0 &&
      !keyHasNull(entries[i].key)
    )
      throw uniqueViolation(index);
  }
}

function uniqueViolation(index: string): OpfsqlError {
  return new OpfsqlError(
    "CONSTRAINT_VIOLATION",
    `UNIQUE constraint failed: index "${index}"`,
  );
}
