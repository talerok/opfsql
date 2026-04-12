import type {
  LogicalInsert,
  LogicalUpdate,
  LogicalDelete,
  LogicalOperator,
  LogicalGet,
  LogicalFilter,
  BoundExpression,
  ColumnBinding,
} from '../binder/types.js';
import { LogicalOperatorType, BoundExpressionClass } from '../binder/types.js';
import type { ICatalog, IRowManager, Row, RowId } from '../store/types.js';
import type { IIndexManager } from '../store/index-manager.js';
import type { IndexKey } from '../store/index-btree/types.js';
import type { ExecuteResult, Tuple, CTECacheEntry, Value } from './types.js';
import type { EvalContext } from './evaluate/context.js';
import { buildResolver, type Resolver } from './resolve.js';
import { evaluateExpression } from './evaluate/index.js';
import { applyComparison, isTruthy } from './evaluate/helpers.js';
import { createPhysicalPlan } from './planner.js';
import { drainOperator } from './operators/utils.js';
import { ExecutorError } from './errors.js';

// ---------------------------------------------------------------------------
// DML filter extraction from logical tree
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

function extractConditions(
  child: LogicalOperator,
): { get: LogicalGet; condition: BoundExpression | null } {
  if (child.type === LogicalOperatorType.LOGICAL_FILTER) {
    const filter = child as LogicalFilter;
    const inner = extractConditions(filter.children[0]);
    if (inner.condition) {
      const combined: BoundExpression = {
        expressionClass: BoundExpressionClass.BOUND_CONJUNCTION,
        conjunctionType: 'AND' as const,
        children: [filter.expressions[0], inner.condition],
        returnType: 'BOOLEAN' as const,
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

/** Convert a storage Row → Tuple using the full schema. */
function rowToTuple(row: Row, get: LogicalGet): Tuple {
  return get.schema.columns.map((col) => row[col.name] ?? null);
}

/** Apply tableFilters + WHERE condition to a tuple. */
async function passesFilter(
  tuple: Tuple,
  scan: DmlScanInfo,
  ctx: EvalContext,
): Promise<boolean> {
  for (const tf of scan.get.tableFilters) {
    if (applyComparison(tuple[tf.columnIndex], tf.constant.value, tf.comparisonType) !== true) {
      return false;
    }
  }
  if (scan.condition) {
    const val = await evaluateExpression(scan.condition, tuple, scan.resolver, ctx);
    if (!isTruthy(val)) return false;
  }
  return true;
}

// ---------------------------------------------------------------------------
// Index maintenance helpers
// ---------------------------------------------------------------------------

function buildIndexKey(row: Row, columns: string[]): IndexKey {
  return columns.map((col) => (row[col] ?? null) as IndexKey[number]);
}

async function maintainIndexesInsert(
  tableName: string,
  row: Row,
  rowId: RowId,
  catalog?: ICatalog,
  indexManager?: IIndexManager,
): Promise<void> {
  if (!catalog || !indexManager) return;
  for (const idx of catalog.getTableIndexes(tableName)) {
    await indexManager.insert(idx.name, buildIndexKey(row, idx.columns), rowId);
  }
}

async function maintainIndexesDelete(
  tableName: string,
  row: Row,
  rowId: RowId,
  catalog?: ICatalog,
  indexManager?: IIndexManager,
): Promise<void> {
  if (!catalog || !indexManager) return;
  for (const idx of catalog.getTableIndexes(tableName)) {
    await indexManager.delete(idx.name, buildIndexKey(row, idx.columns), rowId);
  }
}

// ---------------------------------------------------------------------------
// INSERT
// ---------------------------------------------------------------------------

export async function executeInsert(
  op: LogicalInsert,
  rowManager: IRowManager,
  ctx: EvalContext,
  catalog?: ICatalog,
  indexManager?: IIndexManager,
): Promise<ExecuteResult> {
  if (op.children.length > 0) {
    return executeInsertSelect(op, rowManager, ctx, catalog, indexManager);
  }
  return executeInsertValues(op, rowManager, ctx, catalog, indexManager);
}

async function executeInsertValues(
  op: LogicalInsert,
  rowManager: IRowManager,
  ctx: EvalContext,
  catalog?: ICatalog,
  indexManager?: IIndexManager,
): Promise<ExecuteResult> {
  const colsPerRow = op.columns.length;
  const rowCount = op.expressions.length / colsPerRow;

  // Hoist allocations out of the hot loop
  const emptyTuple: Tuple = [];
  const emptyResolver = buildResolver([]);
  const colNames = op.columns.map((i) => op.schema.columns[i].name);
  const defaults: Row = {};
  for (const col of op.schema.columns) {
    defaults[col.name] = col.defaultValue;
  }
  const indexes = catalog ? catalog.getTableIndexes(op.tableName) : [];

  for (let r = 0; r < rowCount; r++) {
    const row: Row = { ...defaults };

    for (let c = 0; c < colsPerRow; c++) {
      const expr = op.expressions[r * colsPerRow + c];
      row[colNames[c]] = await evaluateExpression(expr, emptyTuple, emptyResolver, ctx);
    }

    const rowId = await rowManager.prepareInsert(op.tableName, row);
    if (indexManager) {
      for (const idx of indexes) {
        await indexManager.insert(idx.name, buildIndexKey(row, idx.columns), rowId);
      }
    }
  }

  return { rows: [], rowsAffected: rowCount, catalogChanges: [] };
}

async function executeInsertSelect(
  op: LogicalInsert,
  rowManager: IRowManager,
  ctx: EvalContext,
  catalog?: ICatalog,
  indexManager?: IIndexManager,
): Promise<ExecuteResult> {
  const cteCache = new Map<number, CTECacheEntry>();
  const plan = createPhysicalPlan(op.children[0], rowManager, cteCache, ctx);
  const tuples = await drainOperator(plan);

  const defaults: Row = {};
  for (const col of op.schema.columns) {
    defaults[col.name] = col.defaultValue;
  }

  for (const tuple of tuples) {
    const row: Row = { ...defaults };
    for (let c = 0; c < op.columns.length; c++) {
      row[op.schema.columns[op.columns[c]].name] = tuple[c] ?? null;
    }

    const rowId = await rowManager.prepareInsert(op.tableName, row);
    await maintainIndexesInsert(op.tableName, row, rowId, catalog, indexManager);
  }

  return { rows: [], rowsAffected: tuples.length, catalogChanges: [] };
}

// ---------------------------------------------------------------------------
// UPDATE
// ---------------------------------------------------------------------------

export async function executeUpdate(
  op: LogicalUpdate,
  rowManager: IRowManager,
  ctx: EvalContext,
  catalog?: ICatalog,
  indexManager?: IIndexManager,
): Promise<ExecuteResult> {
  const scan = extractDmlScan(op.children[0]);

  // Collect phase — read all matching rows and compute new values before mutating
  const targets: Array<{ rowId: RowId; oldRow: Row; newRow: Row }> = [];
  for await (const { rowId, row } of rowManager.scanTable(op.tableName)) {
    const tuple = rowToTuple(row, scan.get);
    if (!(await passesFilter(tuple, scan, ctx))) continue;

    const newRow: Row = { ...row };
    for (let i = 0; i < op.updateColumns.length; i++) {
      const colIdx = op.updateColumns[i];
      newRow[op.schema.columns[colIdx].name] = await evaluateExpression(
        op.expressions[i], tuple, scan.resolver, ctx,
      );
    }
    targets.push({ rowId, oldRow: row, newRow });
  }

  // Mutation phase
  for (const { rowId, oldRow, newRow } of targets) {
    await maintainIndexesDelete(op.tableName, oldRow, rowId, catalog, indexManager);
    const newRowId = await rowManager.prepareUpdate(op.tableName, rowId, newRow);
    await maintainIndexesInsert(op.tableName, newRow, newRowId, catalog, indexManager);
  }

  return { rows: [], rowsAffected: targets.length, catalogChanges: [] };
}

// ---------------------------------------------------------------------------
// DELETE
// ---------------------------------------------------------------------------

export async function executeDelete(
  op: LogicalDelete,
  rowManager: IRowManager,
  ctx: EvalContext,
  catalog?: ICatalog,
  indexManager?: IIndexManager,
): Promise<ExecuteResult> {
  const scan = extractDmlScan(op.children[0]);

  // Collect phase — read all matching rows before mutating
  const targets: Array<{ rowId: RowId; row: Row }> = [];
  for await (const { rowId, row } of rowManager.scanTable(op.tableName)) {
    const tuple = rowToTuple(row, scan.get);
    if (await passesFilter(tuple, scan, ctx)) {
      targets.push({ rowId, row });
    }
  }

  // Mutation phase
  for (const { rowId, row } of targets) {
    await maintainIndexesDelete(op.tableName, row, rowId, catalog, indexManager);
    await rowManager.prepareDelete(op.tableName, rowId);
  }

  return { rows: [], rowsAffected: targets.length, catalogChanges: [] };
}
