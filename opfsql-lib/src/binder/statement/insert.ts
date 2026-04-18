import type { InsertStatement, OnConflictClause, OnConflictUpdate } from '../../parser/types.js';
import { getIndexColumns } from '../../store/index-expression.js';
import type * as BT from '../types.js';
import { LogicalOperatorType } from '../types.js';
import { BindError } from '../core/errors.js';
import type { BindContext } from '../core/context.js';
import { requireTable } from '../core/utils/require-table.js';
import { findColumnIndexOrThrow, getPrimaryKeyColumns } from '../core/utils/find-column.js';
import { bindExpression } from '../expression/index.js';
import { bindQueryNode } from './query-node.js';

export function bindInsert(
  ctx: BindContext,
  stmt: InsertStatement,
): BT.LogicalInsert {
  const schema = requireTable(ctx, stmt.table);
  const columnIndices = resolveColumns(stmt, schema);

  let result: BT.LogicalInsert;
  if (stmt.select_statement) {
    result = bindInsertSelect(ctx, stmt, schema, columnIndices);
  } else {
    result = bindInsertValues(ctx, stmt, schema, columnIndices);
  }

  if (stmt.onConflict) {
    result.onConflict = bindOnConflict(ctx, stmt.onConflict, schema);
  }

  return result;
}

function resolveColumns(stmt: InsertStatement, schema: BT.TableSchema): number[] {
  if (stmt.columns.length === 0) {
    return schema.columns.map((_, i) => i);
  }

  const seen = new Set<string>();
  return stmt.columns.map((colName) => {
    const lower = colName.toLowerCase();
    if (seen.has(lower)) {
      throw new BindError(`Duplicate column "${colName}" in INSERT`);
    }
    seen.add(lower);
    return findColumnIndexOrThrow(schema, colName);
  });
}

function bindInsertSelect(
  ctx: BindContext,
  stmt: InsertStatement,
  schema: BT.TableSchema,
  columnIndices: number[],
): BT.LogicalInsert {
  const scope = ctx.createScope();
  const selectPlan = bindQueryNode(ctx, stmt.select_statement!.node, scope);

  if (selectPlan.types.length !== columnIndices.length) {
    throw new BindError(
      `INSERT SELECT column count mismatch: expected ${columnIndices.length}, got ${selectPlan.types.length}`,
    );
  }

  return makeInsert(schema, columnIndices, [selectPlan], []);
}

function bindInsertValues(
  ctx: BindContext,
  stmt: InsertStatement,
  schema: BT.TableSchema,
  columnIndices: number[],
): BT.LogicalInsert {
  const scope = ctx.createScope();
  const boundExprs: BT.BoundExpression[] = [];

  for (const row of stmt.values) {
    if (row.length !== columnIndices.length) {
      throw new BindError(
        `INSERT VALUES column count mismatch: expected ${columnIndices.length}, got ${row.length}`,
      );
    }
    for (const val of row) {
      boundExprs.push(bindExpression(ctx, val, scope));
    }
  }

  return makeInsert(schema, columnIndices, [], boundExprs);
}

function bindOnConflict(
  ctx: BindContext,
  clause: OnConflictClause,
  schema: BT.TableSchema,
): BT.BoundOnConflict {
  const conflictColumns = resolveConflictColumns(ctx, clause, schema);
  validateConflictTarget(conflictColumns, schema, ctx);

  if (clause.action === 'NOTHING') {
    return bindDoNothing(conflictColumns);
  }
  return bindDoUpdate(ctx, clause.action, conflictColumns, schema);
}

function resolveConflictColumns(
  ctx: BindContext,
  clause: OnConflictClause,
  schema: BT.TableSchema,
): number[] {
  if (clause.conflictTarget) {
    return clause.conflictTarget.map((colName) => findColumnIndexOrThrow(schema, colName));
  }

  // No explicit target — infer from PK
  const pkCols = getPrimaryKeyColumns(schema);
  if (pkCols.length > 0) return pkCols;

  // Fall back to first inline unique column
  const uniqueIdx = schema.columns.findIndex((c) => c.unique);
  if (uniqueIdx !== -1) return [uniqueIdx];

  // Fall back to first unique index
  const uIdx = ctx.catalog.getTableIndexes(schema.name).find((idx) => idx.unique);
  if (uIdx) {
    return uIdx.expressions.flatMap(getIndexColumns).map((colName) => findColumnIndexOrThrow(schema, colName));
  }

  throw new BindError(
    `ON CONFLICT requires a conflict target or a PRIMARY KEY / UNIQUE constraint on table "${schema.name}"`,
  );
}

function bindDoNothing(conflictColumns: number[]): BT.BoundOnConflict {
  return {
    conflictColumns,
    action: 'NOTHING',
    updateColumns: [],
    updateExpressions: [],
    whereExpression: null,
    targetTableIndex: -1,
    excludedTableIndex: -1,
  };
}

function bindDoUpdate(
  ctx: BindContext,
  action: OnConflictUpdate,
  conflictColumns: number[],
  schema: BT.TableSchema,
): BT.BoundOnConflict {
  const scope = ctx.createScope();
  const tableEntry = scope.addTable(schema.name, schema.name, schema); // target table
  const excludedEntry = scope.addTable(schema.name, 'excluded', schema); // pseudo-table for new row values

  const updateColumns: number[] = [];
  const updateExpressions: BT.BoundExpression[] = [];
  for (const sc of action.setClauses) {
    updateColumns.push(findColumnIndexOrThrow(schema, sc.column));
    updateExpressions.push(bindExpression(ctx, sc.value, scope));
  }

  const whereExpression = action.whereClause
    ? bindExpression(ctx, action.whereClause, scope)
    : null;

  return {
    conflictColumns,
    action: 'UPDATE',
    updateColumns,
    updateExpressions,
    whereExpression,
    targetTableIndex: tableEntry.tableIndex,
    excludedTableIndex: excludedEntry.tableIndex,
  };
}

function validateConflictTarget(
  conflictColumns: number[],
  schema: BT.TableSchema,
  ctx: BindContext,
): void {
  // Check if conflict columns match a PK constraint
  if (sameSet(conflictColumns, getPrimaryKeyColumns(schema))) return;

  // Check single-column unique
  if (conflictColumns.length === 1 && schema.columns[conflictColumns[0]].unique) return;

  // Check unique indexes
  const indexes = ctx.catalog.getTableIndexes(schema.name);
  for (const idx of indexes) {
    if (!idx.unique) continue;
    const allSimple = idx.expressions.every((e) => e.type === 'column');
    if (!allSimple) continue;
    const idxCols = idx.expressions.flatMap(getIndexColumns).map((colName) => findColumnIndexOrThrow(schema, colName));
    if (sameSet(conflictColumns, idxCols)) return;
  }

  const colNames = conflictColumns.map((i) => schema.columns[i].name).join(', ');
  throw new BindError(
    `ON CONFLICT columns (${colNames}) do not match any unique constraint on table "${schema.name}"`,
  );
}

function sameSet(a: number[], b: number[]): boolean {
  if (a.length !== b.length) return false;
  const sa = [...a].sort();
  const sb = [...b].sort();
  return sa.every((v, i) => v === sb[i]);
}

function makeInsert(
  schema: BT.TableSchema,
  columns: number[],
  children: BT.LogicalOperator[],
  expressions: BT.BoundExpression[],
): BT.LogicalInsert {
  return {
    type: LogicalOperatorType.LOGICAL_INSERT,
    tableName: schema.name,
    schema,
    columns,
    children,
    expressions,
    types: [],
    estimatedCardinality: 0,
    getColumnBindings: () => [],
  };
}
