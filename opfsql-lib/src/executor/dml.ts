import type {
  BoundExpression,
  ColumnBinding,
  LogicalDelete,
  LogicalFilter,
  LogicalGet,
  LogicalInsert,
  LogicalOperator,
  LogicalUpdate,
} from "../binder/types.js";
import { BoundExpressionClass, LogicalOperatorType } from "../binder/types.js";
import { applyComparison, isTruthy } from "./evaluate/helpers.js";
import type { IndexKey } from "../store/index-btree/types.js";
import type {
  SyncIIndexManager,
  SyncIRowManager,
} from "../store/types.js";
import type { ICatalog, Row, RowId } from "../store/types.js";
import { ExecutorError } from "./errors.js";
import type { SyncEvalContext } from "./evaluate/context.js";
import { evaluateExpression } from "./evaluate/index.js";
import { drainOperator, resolveFilterValue } from "./operators/utils.js";
import { createPhysicalPlan } from "./planner.js";
import { buildResolver, type Resolver } from "./resolve.js";
import type { CTECacheEntry, ExecuteResult, Tuple } from "./types.js";

// ---------------------------------------------------------------------------
// DML scan extraction
// ---------------------------------------------------------------------------

interface DmlScanInfo {
  get: LogicalGet;
  condition: BoundExpression | null;
  layout: ColumnBinding[];
  resolver: Resolver;
}

function extractDmlScan(child: LogicalOperator): DmlScanInfo {
  const { get, condition } = extractConditions(child);
  const layout = get.schema.columns.map((_, i) => ({
    tableIndex: get.tableIndex,
    columnIndex: i,
  }));
  return { get, condition, layout, resolver: buildResolver(layout) };
}

function extractConditions(child: LogicalOperator): {
  get: LogicalGet;
  condition: BoundExpression | null;
} {
  if (child.type === LogicalOperatorType.LOGICAL_FILTER) {
    const filter = child as LogicalFilter;
    const inner = extractConditions(filter.children[0]);
    if (inner.condition) {
      const combined: BoundExpression = {
        expressionClass: BoundExpressionClass.BOUND_CONJUNCTION,
        conjunctionType: "AND" as const,
        children: [filter.expressions[0], inner.condition],
        returnType: "BOOLEAN" as const,
      };
      return { get: inner.get, condition: combined };
    }
    return { get: inner.get, condition: filter.expressions[0] };
  }
  if (child.type === LogicalOperatorType.LOGICAL_GET) {
    return { get: child as LogicalGet, condition: null };
  }
  throw new ExecutorError(`Unexpected node ${child.type} in DML scan tree`);
}

function rowToTuple(row: Row, get: LogicalGet): Tuple {
  return get.schema.columns.map((col) => row[col.name] ?? null);
}

function passesFilter(
  tuple: Tuple,
  scan: DmlScanInfo,
  ctx: SyncEvalContext,
): boolean {
  for (const tf of scan.get.tableFilters) {
    const val = resolveFilterValue(tf.constant, ctx.params);
    if (applyComparison(tuple[tf.columnIndex], val, tf.comparisonType) !== true)
      return false;
  }
  if (scan.condition) {
    const val = evaluateExpression(scan.condition, tuple, scan.resolver, ctx);
    if (!isTruthy(val)) return false;
  }
  return true;
}

// ---------------------------------------------------------------------------
// Index maintenance
// ---------------------------------------------------------------------------

function buildIndexKey(row: Row, columns: string[]): IndexKey {
  return columns.map((col) => (row[col] ?? null) as IndexKey[number]);
}

function maintainIndexesInsert(
  tableName: string,
  row: Row,
  rowId: RowId,
  catalog?: ICatalog,
  indexManager?: SyncIIndexManager,
): void {
  if (!catalog || !indexManager) return;
  for (const idx of catalog.getTableIndexes(tableName)) {
    indexManager.insert(idx.name, buildIndexKey(row, idx.columns), rowId);
  }
}

function maintainIndexesDelete(
  tableName: string,
  row: Row,
  rowId: RowId,
  catalog?: ICatalog,
  indexManager?: SyncIIndexManager,
): void {
  if (!catalog || !indexManager) return;
  for (const idx of catalog.getTableIndexes(tableName)) {
    indexManager.delete(idx.name, buildIndexKey(row, idx.columns), rowId);
  }
}

// ---------------------------------------------------------------------------
// INSERT
// ---------------------------------------------------------------------------

export function executeInsert(
  op: LogicalInsert,
  rowManager: SyncIRowManager,
  ctx: SyncEvalContext,
  catalog?: ICatalog,
  indexManager?: SyncIIndexManager,
): ExecuteResult {
  if (op.children.length > 0)
    return executeInsertSelect(op, rowManager, ctx, catalog, indexManager);
  return executeInsertValues(op, rowManager, ctx, catalog, indexManager);
}

function executeInsertValues(
  op: LogicalInsert,
  rowManager: SyncIRowManager,
  ctx: SyncEvalContext,
  catalog?: ICatalog,
  indexManager?: SyncIIndexManager,
): ExecuteResult {
  const colsPerRow = op.columns.length;
  const rowCount = op.expressions.length / colsPerRow;
  const emptyTuple: Tuple = [];
  const emptyResolver = buildResolver([]);
  const colNames = op.columns.map((i) => op.schema.columns[i].name);
  const defaults: Row = {};
  for (const col of op.schema.columns) defaults[col.name] = col.defaultValue;
  const indexes = catalog ? catalog.getTableIndexes(op.tableName) : [];

  for (let r = 0; r < rowCount; r++) {
    const row: Row = { ...defaults };
    for (let c = 0; c < colsPerRow; c++) {
      const expr = op.expressions[r * colsPerRow + c];
      row[colNames[c]] = evaluateExpression(
        expr,
        emptyTuple,
        emptyResolver,
        ctx,
      );
    }
    const rowId = rowManager.prepareInsert(op.tableName, row);
    if (indexManager) {
      for (const idx of indexes) {
        indexManager.insert(idx.name, buildIndexKey(row, idx.columns), rowId);
      }
    }
  }

  return { rows: [], rowsAffected: rowCount, catalogChanges: [] };
}

function executeInsertSelect(
  op: LogicalInsert,
  rowManager: SyncIRowManager,
  ctx: SyncEvalContext,
  catalog?: ICatalog,
  indexManager?: SyncIIndexManager,
): ExecuteResult {
  const cteCache = new Map<number, CTECacheEntry>();
  const plan = createPhysicalPlan(op.children[0], rowManager, cteCache, ctx);
  const tuples = drainOperator(plan);

  const defaults: Row = {};
  for (const col of op.schema.columns) defaults[col.name] = col.defaultValue;

  for (const tuple of tuples) {
    const row: Row = { ...defaults };
    for (let c = 0; c < op.columns.length; c++) {
      row[op.schema.columns[op.columns[c]].name] = tuple[c] ?? null;
    }
    const rowId = rowManager.prepareInsert(op.tableName, row);
    maintainIndexesInsert(op.tableName, row, rowId, catalog, indexManager);
  }

  return { rows: [], rowsAffected: tuples.length, catalogChanges: [] };
}

// ---------------------------------------------------------------------------
// UPDATE
// ---------------------------------------------------------------------------

export function executeUpdate(
  op: LogicalUpdate,
  rowManager: SyncIRowManager,
  ctx: SyncEvalContext,
  catalog?: ICatalog,
  indexManager?: SyncIIndexManager,
): ExecuteResult {
  const scan = extractDmlScan(op.children[0]);

  const targets: Array<{ rowId: RowId; oldRow: Row; newRow: Row }> = [];
  for (const { rowId, row } of rowManager.scanTable(op.tableName)) {
    const tuple = rowToTuple(row, scan.get);
    if (!passesFilter(tuple, scan, ctx)) continue;

    const newRow: Row = { ...row };
    for (let i = 0; i < op.updateColumns.length; i++) {
      const colIdx = op.updateColumns[i];
      newRow[op.schema.columns[colIdx].name] = evaluateExpression(
        op.expressions[i],
        tuple,
        scan.resolver,
        ctx,
      );
    }
    targets.push({ rowId, oldRow: row, newRow });
  }

  for (const { rowId, oldRow, newRow } of targets) {
    maintainIndexesDelete(op.tableName, oldRow, rowId, catalog, indexManager);
    const newRowId = rowManager.prepareUpdate(op.tableName, rowId, newRow);
    maintainIndexesInsert(
      op.tableName,
      newRow,
      newRowId,
      catalog,
      indexManager,
    );
  }

  return { rows: [], rowsAffected: targets.length, catalogChanges: [] };
}

// ---------------------------------------------------------------------------
// DELETE
// ---------------------------------------------------------------------------

export function executeDelete(
  op: LogicalDelete,
  rowManager: SyncIRowManager,
  ctx: SyncEvalContext,
  catalog?: ICatalog,
  indexManager?: SyncIIndexManager,
): ExecuteResult {
  const scan = extractDmlScan(op.children[0]);

  const targets: Array<{ rowId: RowId; row: Row }> = [];
  for (const { rowId, row } of rowManager.scanTable(op.tableName)) {
    const tuple = rowToTuple(row, scan.get);
    if (passesFilter(tuple, scan, ctx)) targets.push({ rowId, row });
  }

  for (const { rowId, row } of targets) {
    maintainIndexesDelete(op.tableName, row, rowId, catalog, indexManager);
    rowManager.prepareDelete(op.tableName, rowId);
  }

  return { rows: [], rowsAffected: targets.length, catalogChanges: [] };
}
