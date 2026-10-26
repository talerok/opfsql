import { type ColumnReference, ROWID_COLUMN } from "./expression.js";
import {
  type ExpressionStyle,
  formatAggregateCall,
  formatConditions,
  formatExpression,
} from "./format-expression.js";
import type { Expression, OrderKey, Plan } from "./logical.js";

// ---------------------------------------------------------------------------
// A logical plan as text, one line per node, its children indented under it:
// for tests, which state a plan as the tree they expect, and for reading
// plans while debugging. A column prints as `name#table.column`, with
// `^depth` when it belongs to an enclosing query. A subquery prints as
// `subquery(n)`, and its plan follows the line of the node that holds it.
// ---------------------------------------------------------------------------

/** The plan as indented lines, one per node. */
export function formatPlan(plan: Plan): string {
  const printer = new PlanPrinter();
  printer.node(plan, 0, null);
  return printer.lines.join("\n");
}

const INDENT = "  ";

/** A subquery met in a node's expressions, printed after the node. */
interface Subquery {
  readonly number: number;
  readonly plan: Plan;
  readonly isCorrelated: boolean;
}

type Labeled = readonly [Plan, string | null];

class PlanPrinter {
  readonly lines: string[] = [];
  private subqueries = 0;
  private pending: Subquery[] = [];
  /** How this printer's expressions read: columns with their bindings, subqueries numbered. */
  private readonly style: ExpressionStyle<Plan> = {
    column: (reference) => formatColumn(reference),
    subquery: (plan, isCorrelated) => this.reference(plan, isCorrelated),
  };

  node(plan: Plan, depth: number, label: string | null): void {
    const prefix = label === null ? "" : `${label}: `;
    this.lines.push(INDENT.repeat(depth) + prefix + this.describe(plan));
    const found = this.pending;
    this.pending = [];
    for (const subquery of found) {
      const correlated = subquery.isCorrelated ? ", correlated" : "";
      const name = `subquery(${String(subquery.number)}${correlated})`;
      this.node(subquery.plan, depth + 1, name);
    }
    for (const [child, childLabel] of childrenOf(plan)) {
      this.node(child, depth + 1, childLabel);
    }
  }

  /** The node's own line. */
  private describe(plan: Plan): string {
    switch (plan.kind) {
      case "get": {
        return describeGet(plan);
      }
      case "values": {
        const rows = plan.rows.map((row) => `(${this.list(row)})`);
        return `values #${String(plan.tableIndex)} ${rows.join(", ")}`;
      }
      case "filter": {
        return `filter ${this.conjuncts(plan.conditions)}`;
      }
      case "project": {
        return `project #${String(plan.tableIndex)} (${this.list(plan.expressions)})`;
      }
      case "aggregate": {
        const calls = plan.aggregates.map((call) =>
          formatAggregateCall(call, this.style),
        );
        const groups = `#${String(plan.groupIndex)} (${this.list(plan.groups)})`;
        return `aggregate ${groups} #${String(plan.aggregateIndex)} (${calls.join(", ")})`;
      }
      case "join": {
        const on = plan.conditions.length === 0 ? "" : " on ";
        return `join ${plan.joinType}${on}${this.conjuncts(plan.conditions)}`;
      }
      case "order": {
        return `order ${plan.keys.map((key) => this.orderKey(key)).join(", ")}`;
      }
      case "limit": {
        return this.limit(plan.limit, plan.offset);
      }
      case "distinct": {
        return "distinct";
      }
      case "set-operation": {
        return `${plan.operator.toLowerCase()} #${String(plan.tableIndex)}`;
      }
      case "cte": {
        return `cte #${String(plan.cteIndex)} ${plan.name}`;
      }
      case "cte-scan": {
        return `cte-scan #${String(plan.tableIndex)} of #${String(plan.cteIndex)}`;
      }
      case "recursive-cte": {
        const union = plan.isUnionAll ? "union all" : "union";
        return `recursive-cte #${String(plan.cteIndex)} ${plan.name} #${String(plan.tableIndex)} ${union}`;
      }
    }
  }

  private conjuncts(conditions: readonly Expression[]): string {
    return formatConditions(conditions, this.style);
  }

  private limit(limit: Expression | null, offset: Expression | null): string {
    const count = limit === null ? "all" : this.expression(limit);
    if (offset === null) return `limit ${count}`;
    return `limit ${count} offset ${this.expression(offset)}`;
  }

  private orderKey(key: OrderKey): string {
    const direction = key.direction === "ascending" ? "ASC" : "DESC";
    const nulls = key.nulls === "first" ? "NULLS FIRST" : "NULLS LAST";
    return `${this.expression(key.expression)} ${direction} ${nulls}`;
  }

  private list(expressions: readonly Expression[]): string {
    return expressions
      .map((expression) => this.expression(expression))
      .join(", ");
  }

  private expression(expression: Expression): string {
    return formatExpression(expression, this.style);
  }

  /** Numbers a subquery; its plan is printed after the node that holds it. */
  private reference(plan: Plan, isCorrelated: boolean): string {
    this.subqueries++;
    const number = this.subqueries;
    this.pending.push({ number, plan, isCorrelated });
    return `subquery(${String(number)})`;
  }
}

/** A node's children in order, labeled where the order does not say what they are. */
function childrenOf(plan: Plan): readonly Labeled[] {
  switch (plan.kind) {
    case "get":
    case "values":
    case "cte-scan": {
      return [];
    }
    case "filter":
    case "project":
    case "aggregate":
    case "order":
    case "limit":
    case "distinct": {
      return [[plan.child, null]];
    }
    case "join":
    case "set-operation": {
      return [
        [plan.left, null],
        [plan.right, null],
      ];
    }
    case "cte": {
      return [
        [plan.definition, "definition"],
        [plan.body, "body"],
      ];
    }
    case "recursive-cte": {
      return [
        [plan.anchor, "anchor"],
        [plan.step, "step"],
      ];
    }
  }
}

function describeGet(plan: Extract<Plan, { kind: "get" }>): string {
  const columns = plan.columns.map((column) => columnLabel(plan, column));
  return `get #${String(plan.tableIndex)} ${plan.table.name} (${columns.join(", ")})`;
}

function columnLabel(
  plan: Extract<Plan, { kind: "get" }>,
  column: number,
): string {
  if (column === ROWID_COLUMN) return "rowid";
  return plan.table.columns[column]?.name ?? String(column);
}

/** A column with its binding: `name#table.column`, and `^depth` when it is a column of an enclosing query. */
function formatColumn(reference: ColumnReference): string {
  const { tableIndex, columnIndex } = reference.binding;
  const column = columnIndex === ROWID_COLUMN ? "rowid" : String(columnIndex);
  const depth = reference.depth === 0 ? "" : `^${String(reference.depth)}`;
  return `${reference.name}#${String(tableIndex)}.${column}${depth}`;
}
