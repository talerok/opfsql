import type {
  BoundOnConflict,
  ColumnBinding,
  LogicalInsert,
} from "../../sql/binder/types.js";
import type {
  ICatalog,
  Row,
  RowId,
  SyncIIndexManager,
  SyncIRowManager,
  TableSchema,
} from "../../store/types.js";
import { getIndexColumns } from "../../store/index-expression.js";
import { compareSql } from "../../values/index.js";
import {
  type CompiledExpression,
  compileExpression,
  compilePredicate,
  type ExecutionContext,
} from "../evaluate/index.js";
import { drainOperator } from "../operators/shared.js";
import { createPhysicalPlan } from "../planner/index.js";
import { buildResolver } from "../resolve.js";
import type { CTECacheEntry, ExecuteResult, Tuple, Value } from "../types.js";
import { coerceJsonIfNeeded } from "./utils/coerce.js";
import {
  buildIndexKey,
  maintainIndexesDelete,
  maintainIndexesInsert,
} from "./utils/index-maintenance.js";

/** Shared context passed through all insert helpers. */
interface InsertCtx {
  op: LogicalInsert;
  rowManager: SyncIRowManager;
  context: ExecutionContext;
  catalog?: ICatalog;
  indexManager?: SyncIIndexManager;
  /** ON CONFLICT DO UPDATE, compiled once for the statement. */
  conflictUpdate: ConflictUpdate | null;
}

/** DO UPDATE's WHERE and SET, over rows of [existing..., excluded...]. */
interface ConflictUpdate {
  where: ((row: Tuple) => boolean) | null;
  values: CompiledExpression[];
}

// ---------------------------------------------------------------------------
// INSERT entry point
// ---------------------------------------------------------------------------

export function executeInsert(
  op: LogicalInsert,
  rowManager: SyncIRowManager,
  context: ExecutionContext,
  catalog?: ICatalog,
  indexManager?: SyncIIndexManager,
): ExecuteResult {
  const ic: InsertCtx = {
    op,
    rowManager,
    context,
    catalog,
    indexManager,
    conflictUpdate: compileConflictUpdate(op, context),
  };
  const rows =
    op.children.length > 0 ? buildRowsFromSelect(ic) : buildRowsFromValues(ic);
  return insertRows(ic, rows);
}

// ---------------------------------------------------------------------------
// Row building
// ---------------------------------------------------------------------------

function buildRowsFromValues(ic: InsertCtx): Row[] {
  const { op, context } = ic;
  const colsPerRow = op.columns.length;
  const rowCount = op.expressions.length / colsPerRow;
  const emptyResolver = buildResolver([]);
  const colNames = op.columns.map((i) => op.schema.columns[i].name);
  const defaults = buildDefaults(op.schema);

  const rows: Row[] = [];
  for (let r = 0; r < rowCount; r++) {
    const row: Row = { ...defaults };
    for (let c = 0; c < colsPerRow; c++) {
      const expr = op.expressions[r * colsPerRow + c];
      const val = compileExpression(expr, emptyResolver, context)([]);
      row[colNames[c]] = coerceJsonIfNeeded(val, op.schema, op.columns[c]);
    }
    rows.push(row);
  }
  return rows;
}

function buildRowsFromSelect(ic: InsertCtx): Row[] {
  const { op, rowManager, context } = ic;
  const cteCache = new Map<number, CTECacheEntry>();
  const plan = createPhysicalPlan(
    op.children[0],
    rowManager,
    cteCache,
    context,
  );
  const tuples = drainOperator(plan);
  const defaults = buildDefaults(op.schema);

  return tuples.map((tuple) => {
    const row: Row = { ...defaults };
    for (let c = 0; c < op.columns.length; c++) {
      const val = (tuple[c] ?? null) as Value;
      row[op.schema.columns[op.columns[c]].name] = coerceJsonIfNeeded(
        val,
        op.schema,
        op.columns[c],
      );
    }
    return row;
  });
}

// ---------------------------------------------------------------------------
// Insert loop (shared by VALUES and SELECT)
// ---------------------------------------------------------------------------

function insertRows(ic: InsertCtx, rows: Row[]): ExecuteResult {
  let affected = 0;
  for (const row of rows) {
    if (ic.catalog) fillAutoIncrement(ic, row, ic.catalog);
    if (insertOrUpsertRow(ic, row)) {
      affected++;
    }
  }
  return { rows: [], rowsAffected: affected, catalogChanges: [] };
}

// ---------------------------------------------------------------------------
// AUTOINCREMENT
// ---------------------------------------------------------------------------

function fillAutoIncrement(ic: InsertCtx, row: Row, catalog: ICatalog): void {
  const tableName = ic.op.tableName;
  const schema = catalog.getTable(tableName);
  const col = schema?.columns.find((c) => c.autoIncrement);
  if (!col) return;

  const val = row[col.name];
  if (val === null || val === undefined) {
    row[col.name] = ic.rowManager.nextAutoIncrement(tableName);
  } else if (typeof val === "number") {
    ic.rowManager.observeAutoIncrement(tableName, val);
  }
}

// ---------------------------------------------------------------------------
// UPSERT (ON CONFLICT)
// ---------------------------------------------------------------------------

function doInsert(ic: InsertCtx, row: Row): void {
  const rowId = ic.rowManager.prepareInsert(ic.op.tableName, row);
  maintainIndexesInsert(
    ic.op.tableName,
    row,
    rowId,
    ic.catalog,
    ic.indexManager,
  );
}

function insertOrUpsertRow(ic: InsertCtx, newRow: Row): boolean {
  if (!ic.op.onConflict) {
    doInsert(ic, newRow);
    return true;
  }

  const oc = ic.op.onConflict;
  const conflictColNames = oc.conflictColumns.map(
    (i) => ic.op.schema.columns[i].name,
  );
  const conflict = findConflictingRow(ic, newRow, conflictColNames);

  if (!conflict) {
    doInsert(ic, newRow);
    return true;
  }
  if (oc.action === "NOTHING") {
    return false;
  }
  return executeConflictUpdate(ic, oc, conflict.rowId, conflict.row, newRow);
}

// ---------------------------------------------------------------------------
// Conflict detection
// ---------------------------------------------------------------------------

function findConflictingRow(
  ic: InsertCtx,
  newRow: Row,
  conflictColNames: string[],
): { rowId: RowId; row: Row } | null {
  if (ic.catalog && ic.indexManager) {
    const result = findConflictViaIndex(
      ic.op.tableName,
      newRow,
      conflictColNames,
      ic.catalog,
      ic.indexManager,
      ic.rowManager,
    );
    if (result !== undefined) return result;
  }
  return findConflictViaScan(
    ic.op.tableName,
    newRow,
    conflictColNames,
    ic.rowManager,
  );
}

function findConflictViaIndex(
  tableName: string,
  newRow: Row,
  conflictColNames: string[],
  catalog: ICatalog,
  indexManager: SyncIIndexManager,
  rowManager: SyncIRowManager,
): { rowId: RowId; row: Row } | null | undefined {
  const schema = catalog.getTable(tableName);
  if (!schema) return undefined;

  for (const idx of catalog.getTableIndexes(tableName)) {
    if (!idx.unique) continue;

    // Extract column names from index expressions — only simple column
    // expressions can match ON CONFLICT column lists
    const allSimpleColumns = idx.expressions.every((e) => e.type === "column");
    if (!allSimpleColumns) continue;

    const idxColNames = idx.expressions.flatMap(getIndexColumns);
    const idxColsSorted = [...idxColNames].map((c) => c.toLowerCase()).sort();
    const targetSorted = [...conflictColNames]
      .map((c) => c.toLowerCase())
      .sort();
    if (
      idxColsSorted.length !== targetSorted.length ||
      !idxColsSorted.every((c, i) => c === targetSorted[i])
    )
      continue;

    const key = buildIndexKey(newRow, schema, idx);
    if (key.some((v) => v === null)) return null;

    const rowIds = indexManager.search(
      idx.name,
      key.map((val, pos) => ({
        columnPosition: pos,
        comparisonType: "EQUAL" as const,
        value: val as string | number | boolean | null,
      })),
    );
    if (rowIds.length > 0) {
      const row = rowManager.readRow(tableName, rowIds[0]);
      if (row) return { rowId: rowIds[0], row };
    }
    return null;
  }
  return undefined;
}

function findConflictViaScan(
  tableName: string,
  newRow: Row,
  conflictColNames: string[],
  rowManager: SyncIRowManager,
): { rowId: RowId; row: Row } | null {
  for (const { rowId, row } of rowManager.scanTable(tableName)) {
    const matches = conflictColNames.every((col) => {
      const existingVal = row[col] ?? null;
      const newVal = newRow[col] ?? null;
      // Equal as `=` compares: NULLs never conflict.
      return compareSql(existingVal, newVal) === 0;
    });
    if (matches) return { rowId, row };
  }
  return null;
}

// ---------------------------------------------------------------------------
// Conflict UPDATE
// ---------------------------------------------------------------------------

function executeConflictUpdate(
  ic: InsertCtx,
  oc: BoundOnConflict,
  existingRowId: RowId,
  existingRow: Row,
  excludedRow: Row,
): boolean {
  const schema = ic.op.schema;
  const update = ic.conflictUpdate;
  if (update === null) return false;

  // The row the expressions see: [existing columns..., excluded columns...]
  const combinedTuple: Tuple = [
    ...schema.columns.map((column) => existingRow[column.name] ?? null),
    ...schema.columns.map((column) => excludedRow[column.name] ?? null),
  ];
  if (update.where !== null && !update.where(combinedTuple)) return false;

  const updatedRow: Row = { ...existingRow };
  for (let i = 0; i < oc.updateColumns.length; i++) {
    const colIdx = oc.updateColumns[i];
    updatedRow[schema.columns[colIdx].name] = coerceJsonIfNeeded(
      update.values[i](combinedTuple),
      schema,
      colIdx,
    );
  }

  maintainIndexesDelete(
    ic.op.tableName,
    existingRow,
    existingRowId,
    ic.catalog,
    ic.indexManager,
  );
  const newRowId = ic.rowManager.prepareUpdate(
    ic.op.tableName,
    existingRowId,
    updatedRow,
  );
  maintainIndexesInsert(
    ic.op.tableName,
    updatedRow,
    newRowId,
    ic.catalog,
    ic.indexManager,
  );

  return true;
}

/** Compiles DO UPDATE once: its row layout is fixed by the table. */
function compileConflictUpdate(
  op: LogicalInsert,
  context: ExecutionContext,
): ConflictUpdate | null {
  const oc = op.onConflict;
  if (oc?.action !== "UPDATE") return null;
  const columns = op.schema.columns.map((_column, columnIndex) => columnIndex);
  const layout: ColumnBinding[] = [
    ...columns.map((columnIndex) => ({
      tableIndex: oc.targetTableIndex,
      columnIndex,
    })),
    ...columns.map((columnIndex) => ({
      tableIndex: oc.excludedTableIndex,
      columnIndex,
    })),
  ];
  const resolver = buildResolver(layout);
  return {
    where:
      oc.whereExpression === null
        ? null
        : compilePredicate(oc.whereExpression, resolver, context),
    values: oc.updateExpressions.map((expression) =>
      compileExpression(expression, resolver, context),
    ),
  };
}

// ---------------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------------

function buildDefaults(schema: TableSchema): Row {
  const defaults: Row = {};
  for (const col of schema.columns) defaults[col.name] = col.defaultValue;
  return defaults;
}
