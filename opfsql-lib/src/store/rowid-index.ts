import { isAboveUpper, isBelowLower } from "./index-btree/search-bounds.js";
import type { IndexKey, RangeOptions } from "./index-btree/types.js";
import type { SyncTableBTree } from "./table-btree.js";
import type { RowId } from "./types.js";

/**
 * The primary key index of a table keyed by its INTEGER PRIMARY KEY. It is
 * the table's own tree: each row is its entry, so writing rows maintains it.
 */
export class RowidIndex {
  constructor(private readonly table: SyncTableBTree) {}

  insert(): void {
    // The row is the entry.
  }

  delete(): void {
    // The row is the entry.
  }

  range(opts: RangeOptions = {}): RowId[] {
    const lower = opts.lower?.[0];
    const results: RowId[] = [];
    for (const [rowId] of this.table.scan(
      typeof lower === "number" ? lower : undefined,
    )) {
      const key = [rowId];
      if (isBelowLower(key, opts)) continue;
      if (isAboveUpper(key, opts)) break;
      results.push(rowId);
    }
    return results;
  }

  first(): { key: IndexKey; rowId: RowId } | null {
    for (const [rowId] of this.table.scan()) return { key: [rowId], rowId };
    return null;
  }

  last(): { key: IndexKey; rowId: RowId } | null {
    const last = this.table.last();
    return last && { key: [last[0]], rowId: last[0] };
  }

  drop(): void {
    // The table's pages go with the table.
  }
}
