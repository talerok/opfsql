import { CorruptError } from "../../errors.js";
import type { physical } from "../../sql/plan/index.js";
import type { TableRow, TableRows } from "../../table/index.js";
import type { Context } from "../context.js";
import { type Operator, WalkOperator } from "../operator.js";
import { compileRowidSet, rowsSelected } from "./rowids.js";
import { compileKeySeek } from "./seeks.js";
import { tableAccess } from "./table-access.js";

/**
 * What the branches found, by rowid: a row a rowid branch read already, or
 * null for a rowid an index gave, whose row is still to read.
 */
type Found = Map<number, TableRow | null>;

/** A branch compiled: adds what it finds in one run. */
type Branch = (found: Found) => void;

/**
 * The rows any branch finds, each read once, in rowid order, then checked
 * against the residual, which keeps the OR the branches came from.
 */
export function indexUnion(
  plan: physical.IndexUnion,
  context: Context,
): Operator {
  const { rows, matching } = tableAccess(plan, context);
  const branches = plan.branches.map((branch) =>
    compileBranch(branch, rows, context),
  );
  return new WalkOperator(() => matching(foundRows(branches, rows)), []);
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
      return (found) => {
        for (const rowid of index.rowids(seek(), "ascending")) {
          if (!found.has(rowid)) found.set(rowid, null);
        }
      };
    }
    case "rowids": {
      const selection = compileRowidSet(branch.rowids, context);
      return (found) => {
        for (const row of rowsSelected(selection(), rows)) {
          found.set(row.rowid, row);
        }
      };
    }
  }
}

function* foundRows(
  branches: readonly Branch[],
  rows: TableRows,
): Generator<TableRow> {
  const found: Found = new Map();
  for (const branch of branches) branch(found);
  const rowids = [...found.keys()].toSorted((a, b) => a - b);
  for (const rowid of rowids) yield found.get(rowid) ?? indexedRow(rows, rowid);
}

/** The row an index entry names, which must exist. */
function indexedRow(rows: TableRows, rowid: number): TableRow {
  const row = rows.get(rowid);
  if (row === null) {
    throw new CorruptError(
      `an index of ${rows.table.name} has an entry for a missing row ${String(rowid)}`,
    );
  }
  return row;
}
