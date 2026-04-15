import type {
  BoundOnConflict,
  ColumnBinding,
  LogicalInsert,
} from "../../binder/types.js";
import type {
  ICatalog,
  Row,
  RowId,
  SyncIIndexManager,
  SyncIRowManager,
  TableSchema,
} from "../../store/types.js";
import type { SyncEvalContext } from "../evaluate/context.js";
import { evaluateExpression } from "../evaluate/index.js";
import { isTruthy } from "../evaluate/utils/compare.js";
import { drainOperator } from "../operators/utils.js";
import { createPhysicalPlan } from "../planner.js";
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
  ctx: SyncEvalContext;
  catalog?: ICatalog;
  indexManager?: SyncIIndexManager;
}

// ---------------------------------------------------------------------------
// INSERT entry point
// ---------------------------------------------------------------------------

export function executeInsert(
  op: LogicalInsert,
  rowManager: SyncIRowManager,
  ctx: SyncEvalContext,
  catalog?: ICatalog,
  indexManager?: SyncIIndexManager,
): ExecuteResult {
  const ic: InsertCtx = { op, rowManager, ctx, catalog, indexManager };
  const rows =
    op.children.length > 0 ? buildRowsFromSelect(ic) : buildRowsFromValues(ic);
  return insertRows(ic, rows);
}

// ---------------------------------------------------------------------------
// Row building
// ---------------------------------------------------------------------------

function buildRowsFromValues(ic: InsertCtx): Row[] {
  const { op, ctx } = ic;
  const colsPerRow = op.columns.length;
  const rowCount = op.expressions.length / colsPerRow;
  const emptyTuple: Tuple = [];
  const emptyResolver = buildResolver([]);
  const colNames = op.columns.map((i) => op.schema.columns[i].name);
  const defaults = buildDefaults(op.schema);

  const rows: Row[] = [];
  for (let r = 0; r < rowCount; r++) {
    const row: Row = { ...defaults };
    for (let c = 0; c < colsPerRow; c++) {
      const expr = op.expressions[r * colsPerRow + c];
      const val = evaluateExpression(expr, emptyTuple, emptyResolver, ctx);
      row[colNames[c]] = coerceJsonIfNeeded(val, op.schema, op.columns[c]);
    }
    rows.push(row);
  }
  return rows;
}

function buildRowsFromSelect(ic: InsertCtx): Row[] {
  const { op, rowManager, ctx } = ic;
  const cteCache = new Map<number, CTECacheEntry>();
  const plan = createPhysicalPlan(op.children[0], rowManager, cteCache, ctx);
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
  let seqDirty = false;
  for (const row of rows) {
    if (ic.catalog && fillAutoIncrement(row, ic.op.tableName, ic.catalog)) {
      seqDirty = true;
    }
    if (insertOrUpsertRow(ic, row)) {
      affected++;
    }
  }
  return {
    rows: [],
    rowsAffected: affected,
    catalogChanges: [],
    catalogDirty: seqDirty,
  };
}

// ---------------------------------------------------------------------------
// AUTOINCREMENT
// ---------------------------------------------------------------------------

function fillAutoIncrement(
  row: Row,
  tableName: string,
  catalog: ICatalog,
): boolean {
  const schema = catalog.getTable(tableName);
  if (!schema) return false;
  const col = schema.columns.find((c) => c.autoIncrement);
  if (!col) return false;

  const seq = schema.autoIncrementSeq ?? 0;
  const val = row[col.name];

  if (val === null || val === undefined) {
    const next = seq + 1;
    row[col.name] = next;
    catalog.updateTable({ ...schema, autoIncrementSeq: next });
    return true;
  }
  if (typeof val === "number" && val > seq) {
    catalog.updateTable({ ...schema, autoIncrementSeq: val });
    return true;
  }
  return false;
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
  for (const idx of catalog.getTableIndexes(tableName)) {
    if (!idx.unique) continue;
    const idxColsSorted = [...idx.columns].sort();
    const targetSorted = [...conflictColNames].sort();
    if (
      idxColsSorted.length !== targetSorted.length ||
      !idxColsSorted.every(
        (c, i) => c.toLowerCase() === targetSorted[i].toLowerCase(),
      )
    )
      continue;

    const key = buildIndexKey(newRow, idx.columns);
    if (key.some((v) => v === null)) return null;

    const rowIds = indexManager.search(
      idx.name,
      idx.columns.map((col, pos) => ({
        columnPosition: pos,
        comparisonType: "EQUAL" as const,
        value: (newRow[col] ?? null) as string | number | boolean | null,
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
      if (existingVal === null || newVal === null) return false;
      return existingVal === newVal;
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
  const colCount = schema.columns.length;

  // Build combined tuple: [existing cols..., excluded cols...]
  const combinedTuple: Tuple = new Array(colCount * 2);
  for (let i = 0; i < colCount; i++) {
    const colName = schema.columns[i].name;
    combinedTuple[i] = existingRow[colName] ?? null;
    combinedTuple[colCount + i] = excludedRow[colName] ?? null;
  }

  const layout: ColumnBinding[] = [];
  for (let i = 0; i < colCount; i++) {
    layout.push({ tableIndex: oc.targetTableIndex, columnIndex: i });
  }
  for (let i = 0; i < colCount; i++) {
    layout.push({ tableIndex: oc.excludedTableIndex, columnIndex: i });
  }
  const resolver = buildResolver(layout);

  if (oc.whereExpression) {
    const val = evaluateExpression(
      oc.whereExpression,
      combinedTuple,
      resolver,
      ic.ctx,
    );
    if (!isTruthy(val)) return false;
  }

  const updatedRow: Row = { ...existingRow };
  for (let i = 0; i < oc.updateColumns.length; i++) {
    const colIdx = oc.updateColumns[i];
    const val = evaluateExpression(
      oc.updateExpressions[i],
      combinedTuple,
      resolver,
      ic.ctx,
    );
    updatedRow[schema.columns[colIdx].name] = coerceJsonIfNeeded(
      val,
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

// ---------------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------------

function buildDefaults(schema: TableSchema): Row {
  const defaults: Row = {};
  for (const col of schema.columns) defaults[col.name] = col.defaultValue;
  return defaults;
}
