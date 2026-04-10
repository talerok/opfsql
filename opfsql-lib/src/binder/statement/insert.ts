import type { InsertStatement } from '../../parser/types.js';
import type * as BT from '../types.js';
import { LogicalOperatorType } from '../types.js';
import { BindError } from '../core/errors.js';
import type { BindContext } from '../core/context.js';
import { requireTable } from '../core/helpers.js';
import { bindExpression } from '../expression/index.js';
import { bindQueryNode } from './query-node.js';

export function bindInsert(
  ctx: BindContext,
  stmt: InsertStatement,
): BT.LogicalInsert {
  const schema = requireTable(ctx, stmt.table);

  let columnIndices: number[];
  if (stmt.columns.length > 0) {
    const seen = new Set<string>();
    columnIndices = stmt.columns.map((colName) => {
      const lower = colName.toLowerCase();
      if (seen.has(lower)) {
        throw new BindError(`Duplicate column "${colName}" in INSERT`);
      }
      seen.add(lower);
      const idx = schema.columns.findIndex(
        (c) => c.name.toLowerCase() === lower,
      );
      if (idx === -1) {
        throw new BindError(
          `Column "${colName}" not found in table "${stmt.table}"`,
        );
      }
      return idx;
    });
  } else {
    columnIndices = schema.columns.map((_, i) => i);
  }

  if (stmt.select_statement) {
    const scope = ctx.createScope();
    const selectPlan = bindQueryNode(ctx, stmt.select_statement.node, scope);
    if (selectPlan.types.length !== columnIndices.length) {
      throw new BindError(
        `INSERT SELECT column count mismatch: expected ${columnIndices.length}, got ${selectPlan.types.length}`,
      );
    }
    return {
      type: LogicalOperatorType.LOGICAL_INSERT,
      tableName: schema.name,
      schema,
      columns: columnIndices,
      children: [selectPlan],
      expressions: [],
      types: [],
      estimatedCardinality: 0,
      getColumnBindings: () => [],
    };
  }

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

  return {
    type: LogicalOperatorType.LOGICAL_INSERT,
    tableName: schema.name,
    schema,
    columns: columnIndices,
    children: [],
    expressions: boundExprs,
    types: [],
    estimatedCardinality: 0,
    getColumnBindings: () => [],
  };
}
