import { CorruptError } from "../../errors.js";
import type { Tuple } from "../../expressions/index.js";
import { type physical, ROWID_COLUMN } from "../../sql/plan/index.js";
import type { ColumnReader, TableRows } from "../../table/index.js";
import type { Context } from "../context.js";
import { leafOperator, type Operator } from "../operator.js";
import { compileRowidSet, type RowidSelection } from "./rowids.js";
import { compileKeySeek } from "./seeks.js";
import { tableAccess } from "./table-access.js";

/** A branch compiled: the rowids it finds in one run, each of an existing row. */
type Branch = () => Iterable<number>;

/**
 * The rows any branch finds, each read once, in rowid order, then checked
 * against the residual, which keeps the OR the branches came from: the
 * branches give rowids, and each row is read once they are gathered.
 */
export function indexUnion(
  plan: physical.IndexUnion,
  context: Context,
): Operator {
  const { rows, reader, matching } = tableAccess(plan, context);
  const branches = plan.branches.map((branch) =>
    compileBranch(branch, rows, context),
  );
  return leafOperator(
    () => matching(rowsWithRowids(unionOf(branches), reader)),
    context.query.run,
  );
}

function compileBranch(
  branch: physical.IndexBranch,
  rows: TableRows,
  context: Context,
): Branch {
  switch (branch.kind) {
    case "index": {
      const index = context.tables.index(branch.index);
      const seek = compileKeySeek(branch.keys, context);
      return () => index.rowids(seek(), "ascending");
    }
    case "rowids": {
      const selection = compileRowidSet(branch.rowids, context);
      const rowidOnly = rows.columns([ROWID_COLUMN]);
      return () => existingRowids(selection(), rows, rowidOnly);
    }
  }
}

/** The rowids of the rows a selection finds: listed ones that exist, or a range's. */
function* existingRowids(
  selection: RowidSelection,
  rows: TableRows,
  rowidOnly: ColumnReader,
): Generator<number> {
  if (selection.kind === "list") {
    yield* selection.rowids.filter((rowid) => rows.has(rowid));
    return;
  }
  const inRange = rowidOnly.between(selection.first, selection.last);
  for (const [rowid] of inRange) yield rowid as number;
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

/** The rows of `rowids`, which must exist: an index named them, or they were found. */
function* rowsWithRowids(
  rowids: readonly number[],
  reader: ColumnReader,
): Generator<Tuple> {
  for (const rowid of rowids) {
    const row = reader.at(rowid);
    if (row === null) {
      throw new CorruptError(
        `an index has an entry for a missing row ${String(rowid)}`,
      );
    }
    yield row;
  }
}
