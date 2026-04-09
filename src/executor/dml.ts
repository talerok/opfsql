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
import type { ICatalog, IRowManager, Row } from '../store/types.js';
import type { IIndexManager } from '../store/index-manager.js';
import type { IndexKey } from '../store/btree/types.js';
import type { ExecuteResult, Tuple, CTECacheEntry, Value } from './types.js';
import type { EvalContext } from './evaluate/context.js';
import { buildResolver } from './resolve.js';
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
}

function extractDmlFilter(child: LogicalOperator): DmlScanInfo {
  if (child.type === LogicalOperatorType.LOGICAL_FILTER) {
    const filter = child as LogicalFilter;
    const inner = extractDmlFilter(filter.children[0]);
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

/** Build layout and convert storage Row → Tuple for DML evaluation. */
function rowToTupleForDml(
  row: Row,
  get: LogicalGet,
): { tuple: Tuple; layout: ColumnBinding[] } {
  const layout = get.schema.columns.map((_, i) => ({
    tableIndex: get.tableIndex,
    columnIndex: i,
  }));
  const tuple: Tuple = get.schema.columns.map((col) => row[col.name] ?? null);
  return { tuple, layout };
}

/** Apply tableFilters + condition to a tuple. */
async function passesFilter(
  tuple: Tuple,
  layout: ColumnBinding[],
  get: LogicalGet,
  condition: BoundExpression | null,
  ctx: EvalContext,
): Promise<boolean> {
  for (const tf of get.tableFilters) {
    const pos = tf.columnIndex;
    const result = applyComparison(tuple[pos], tf.constant.value, tf.comparisonType);
    if (result !== true) return false;
  }

  if (condition) {
    const resolver = buildResolver(layout);
    const val = await evaluateExpression(condition, tuple, resolver, ctx);
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
  rowId: { pageId: number; slotId: number },
  catalog?: ICatalog,
  indexManager?: IIndexManager,
): Promise<void> {
  if (!catalog || !indexManager) return;
  const indexes = catalog.getTableIndexes(tableName);
  for (const idx of indexes) {
    const key = buildIndexKey(row, idx.columns);
    await indexManager.insert(idx.name, key, rowId, idx.unique);
  }
}

async function maintainIndexesDelete(
  tableName: string,
  row: Row,
  rowId: { pageId: number; slotId: number },
  catalog?: ICatalog,
  indexManager?: IIndexManager,
): Promise<void> {
  if (!catalog || !indexManager) return;
  const indexes = catalog.getTableIndexes(tableName);
  for (const idx of indexes) {
    const key = buildIndexKey(row, idx.columns);
    await indexManager.delete(idx.name, key, rowId);
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

  for (let r = 0; r < rowCount; r++) {
    const row: Row = {};

    for (const col of op.schema.columns) {
      row[col.name] = col.defaultValue;
    }

    for (let c = 0; c < colsPerRow; c++) {
      const expr = op.expressions[r * colsPerRow + c];
      const val = await evaluateExpression(expr, [], buildResolver([]), ctx);
      row[op.schema.columns[op.columns[c]].name] = val;
    }

    const rowId = await rowManager.prepareInsert(op.tableName, row);
    await maintainIndexesInsert(op.tableName, row, rowId, catalog, indexManager);
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

  for (const tuple of tuples) {
    const row: Row = {};

    for (const col of op.schema.columns) {
      row[col.name] = col.defaultValue;
    }

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
  const { get, condition } = extractDmlFilter(op.children[0]);
  let affected = 0;

  for await (const { rowId, row } of rowManager.scanTable(op.tableName)) {
    const { tuple, layout } = rowToTupleForDml(row, get);

    if (!(await passesFilter(tuple, layout, get, condition, ctx))) {
      continue;
    }

    const newRow: Row = { ...row };
    const resolver = buildResolver(layout);
    for (let i = 0; i < op.updateColumns.length; i++) {
      const colIdx = op.updateColumns[i];
      const val = await evaluateExpression(
        op.expressions[i],
        tuple,
        resolver,
        ctx,
      );
      newRow[op.schema.columns[colIdx].name] = val;
    }

    // Delete old index entries, insert new ones
    await maintainIndexesDelete(op.tableName, row, rowId, catalog, indexManager);
    const newRowId = await rowManager.prepareUpdate(op.tableName, rowId, newRow);
    await maintainIndexesInsert(op.tableName, newRow, newRowId, catalog, indexManager);
    affected++;
  }

  return { rows: [], rowsAffected: affected, catalogChanges: [] };
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
  const { get, condition } = extractDmlFilter(op.children[0]);
  let affected = 0;

  for await (const { rowId, row } of rowManager.scanTable(op.tableName)) {
    const { tuple, layout } = rowToTupleForDml(row, get);

    if (!(await passesFilter(tuple, layout, get, condition, ctx))) {
      continue;
    }

    await maintainIndexesDelete(op.tableName, row, rowId, catalog, indexManager);
    await rowManager.prepareDelete(op.tableName, rowId);
    affected++;
  }

  return { rows: [], rowsAffected: affected, catalogChanges: [] };
}
