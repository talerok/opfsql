import { CorruptError } from "../../errors.js";
import type { Index } from "../../schema/index.js";
import type { OrderDirection, physical } from "../../sql/plan/index.js";
import type { TableRow, TableRows } from "../../table/index.js";
import { IndexReader, keyRowid } from "../../storage/index.js";
import type { Context } from "../context.js";
import { type Operator, WalkOperator } from "../operator.js";
import { compileKeyRange, type KeyBounds, keysIn } from "./key-range.js";
import { tableAccess } from "./table-access.js";

/** The index's entries within the key range, in key order or reverse, then their rows. */
export function indexScan(
  plan: physical.IndexScan,
  context: Context,
): Operator {
  const { rows, matching } = tableAccess(plan, context);
  const entries = new IndexReader(context.pages, plan.index.rootPage);
  const bounds = compileKeyRange(plan.keys, plan.index, plan.table, context);
  const read = (range: KeyBounds | null): Iterable<TableRow> =>
    indexedRows({ entries, rows, index: plan.index }, range, plan.direction);
  return new WalkOperator(() => matching(read(bounds())), []);
}

/** An index, and the table it indexes. */
interface IndexedTable {
  readonly entries: IndexReader;
  readonly rows: TableRows;
  readonly index: Index;
}

/** The rows of the entries within `bounds`; none for null bounds. */
function* indexedRows(
  { entries, rows, index }: IndexedTable,
  bounds: KeyBounds | null,
  direction: OrderDirection,
): Generator<TableRow> {
  if (bounds === null) return;
  for (const key of keysIn(entries, bounds, direction)) {
    const rowid = keyRowid(key);
    const row = rows.get(rowid);
    if (row === null) {
      throw new CorruptError(
        `index ${index.name} has an entry for a missing row ${String(rowid)}`,
      );
    }
    yield row;
  }
}
