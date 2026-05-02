import type { UpdateStatement } from "../../parser/types.js";
import type { BindContext } from "../core/context.js";
import { makeFilter, makeGet } from "../core/operators.js";
import { findColumnIndexOrThrow } from "../core/utils/find-column.js";
import { requireTable } from "../core/utils/require-table.js";
import { bindExpression } from "../expression/index.js";
import type * as BT from "../types.js";
import { LogicalOperatorType } from "../types.js";

export function bindUpdate(
  ctx: BindContext,
  stmt: UpdateStatement,
): BT.LogicalUpdate {
  const schema = requireTable(ctx, stmt.table);
  const scope = ctx.createScope();
  const entry = scope.addTable(stmt.table, stmt.table, schema);

  let scan: BT.LogicalOperator = makeGet(entry, schema);

  if (stmt.where_clause) {
    const filter = bindExpression(ctx, stmt.where_clause, scope);
    scan = makeFilter(scan, [filter]);
  }

  const updateColumns: number[] = [];
  const expressions: BT.BoundExpression[] = [];
  for (const clause of stmt.set_clauses) {
    updateColumns.push(findColumnIndexOrThrow(schema, clause.column));
    expressions.push(bindExpression(ctx, clause.value, scope));
  }

  return {
    type: LogicalOperatorType.LOGICAL_UPDATE,
    tableName: schema.name,
    schema,
    children: [scan],
    updateColumns,
    expressions,
    types: [],
    estimatedCardinality: 0,
    columnBindings: [],
  };
}
