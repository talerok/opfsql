import type { physical } from "../../sql/plan/index.js";
import type { TableRow, TableRows } from "../../table/index.js";
import { IndexReader, keyRowid } from "../../storage/index.js";
import type { Context } from "../context.js";
import { type Operator, WalkOperator } from "../operator.js";
import { compileKeyRange, type KeyBounds, keysIn } from "./key-range.js";
import {
  compileRowidSet,
  type RowidSelection,
  rowsSelected,
} from "./rowids.js";
import { tableAccess } from "./table-access.js";

/** A branch compiled: the rowids it finds in one run. */
type Branch = () => Iterable<number>;

/**
 * The rows any branch finds, each once, in rowid order: the rowids are
 * gathered first, then each row is read and checked against the residual,
 * which keeps the OR the branches came from.
 */
export function indexUnion(
  plan: physical.IndexUnion,
  context: Context,
): Operator {
  const { rows, matching } = tableAccess(plan, context);
  const branches = plan.branches.map((branch) =>
    compileBranch(branch, rows, context),
  );
  return new WalkOperator(
    () => matching(rowsWithRowids(unionOf(branches), rows)),
    [],
  );
}

function compileBranch(
  branch: physical.IndexBranch,
  rows: TableRows,
  context: Context,
): Branch {
  switch (branch.kind) {
    case "index": {
      const entries = new IndexReader(context.pages, branch.index.rootPage);
      const bounds = compileKeyRange(branch.keys, context);
      return () => rowidsOfEntries(entries, bounds());
    }
    case "rowids": {
      const selection = compileRowidSet(branch.rowids, context);
      return () => rowidsSelected(selection(), rows);
    }
  }
}

function* rowidsOfEntries(
  entries: IndexReader,
  bounds: KeyBounds | null,
): Generator<number> {
  if (bounds === null) return;
  for (const key of keysIn(entries, bounds, "ascending")) yield keyRowid(key);
}

function* rowidsSelected(
  selection: RowidSelection,
  rows: TableRows,
): Generator<number> {
  if (selection.kind === "list") {
    yield* selection.rowids;
    return;
  }
  for (const row of rowsSelected(selection, rows)) yield row.rowid;
}

/** Every branch's rowids, each once, in order. */
function unionOf(branches: readonly Branch[]): number[] {
  const rowids = new Set<number>();
  for (const branch of branches) addEach(rowids, branch());
  return [...rowids].toSorted((a, b) => a - b);
}

function addEach(rowids: Set<number>, found: Iterable<number>): void {
  for (const rowid of found) rowids.add(rowid);
}

/** The rows that exist among `rowids`: a listed rowid may name none. */
function* rowsWithRowids(
  rowids: readonly number[],
  rows: TableRows,
): Generator<TableRow> {
  for (const rowid of rowids) {
    const row = rows.get(rowid);
    if (row !== null) yield row;
  }
}
