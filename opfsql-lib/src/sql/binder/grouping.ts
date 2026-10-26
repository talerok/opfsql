import { SemanticError } from "../../errors.js";
import { isAggregateFunction } from "../../functions/index.js";
import {
  childExpressions,
  ExpressionClass,
  type OrderByNode,
  type ParsedExpression,
  type SelectNode,
} from "../parser/index.js";
import type { ColumnReference, logical } from "../plan/index.js";
import { nameOf } from "./columns.js";
import { isSameExpression } from "./same-expression.js";
import { mapChildren, mapPlan } from "./walk.js";

// ---------------------------------------------------------------------------
// The grouping model. A query with GROUP BY, HAVING or an aggregate call
// computes an aggregate node: its groups, bound to `groupIndex`, and its
// aggregate calls, bound to `aggregateIndex`. What is computed above it —
// the select list, HAVING, ORDER BY — reads those columns: an expression
// that is (structurally) a group reads the group's column, and a column of
// the query's FROM that is neither grouped nor inside an aggregate is an
// error, as in PostgreSQL. SQLite's "bare columns" are not supported.
// ---------------------------------------------------------------------------

type Expression = logical.Expression;

/** True if `node`, ordered by `orderBy`, computes an aggregate node. */
export function isAggregating(
  node: SelectNode,
  orderBy: readonly OrderByNode[],
): boolean {
  const expressions = [
    ...node.selectList,
    ...orderBy.map((order) => order.expression),
  ];
  const hasAggregate = expressions.some((expression) =>
    hasAggregateCall(expression),
  );
  return hasAggregate || node.groupBy.length > 0 || node.having !== null;
}

/** True if `expression` calls an aggregate at its own level, not in a subquery. */
export function hasAggregateCall(expression: ParsedExpression): boolean {
  const isCall =
    expression.expressionClass === ExpressionClass.FUNCTION &&
    isAggregateFunction(expression.functionName);
  return (
    isCall ||
    childExpressions(expression).some((child) => hasAggregateCall(child))
  );
}

export class Grouping {
  /**
   * `groups` are bound over the query's FROM, whose relations have the
   * table indexes `fromTables`.
   */
  constructor(
    readonly groups: readonly Expression[],
    readonly groupIndex: number,
    private readonly fromTables: ReadonlySet<number>,
  ) {}

  /** Group `group`'s column above the aggregate. */
  column(group: number): ColumnReference | null {
    const expression = this.groups[group];
    if (expression === undefined) return null;
    return this.groupColumn(group, expression, 0);
  }

  /**
   * `expression`, bound over the FROM, as it reads above the aggregate:
   * groups become the group columns, `depth` levels out inside subqueries.
   */
  rewrite(expression: Expression, depth = 0): Expression {
    const group = this.groups.findIndex((candidate) =>
      isSameExpression(candidate, expression),
    );
    if (group !== -1) return this.groupColumn(group, expression, depth);
    switch (expression.kind) {
      case "column": {
        this.checkGrouped(expression);
        return expression;
      }
      case "json-access": {
        const column = this.rewrite(expression.column, depth);
        return column.kind === "column" ? { ...expression, column } : column;
      }
      case "scalar-subquery":
      case "exists": {
        const plan = this.rewritePlan(expression.plan, depth + 1);
        return { ...expression, plan };
      }
      case "quantified-comparison": {
        const plan = this.rewritePlan(expression.plan, depth + 1);
        const needle = this.rewrite(expression.needle, depth);
        return { ...expression, needle, plan };
      }
      case "constant":
      case "parameter":
      case "comparison":
      case "conjunction":
      case "not":
      case "is-null":
      case "arithmetic":
      case "negate":
      case "concatenate":
      case "in-list":
      case "between":
      case "function":
      case "case":
      case "cast": {
        return mapChildren(expression, (child) => this.rewrite(child, depth));
      }
    }
  }

  private rewritePlan(plan: logical.Plan, depth: number): logical.Plan {
    return mapPlan(plan, (expression) => this.rewrite(expression, depth));
  }

  private groupColumn(
    group: number,
    expression: Expression,
    depth: number,
  ): ColumnReference {
    return {
      kind: "column",
      binding: { tableIndex: this.groupIndex, columnIndex: group },
      depth,
      name: nameOf(expression, `group${String(group + 1)}`),
      type: expression.type,
    };
  }

  private checkGrouped(column: ColumnReference): void {
    if (!this.fromTables.has(column.binding.tableIndex)) return;
    throw new SemanticError(
      `column ${column.name} must appear in the GROUP BY clause or be used in an aggregate function`,
    );
  }
}
