import type { StaticType } from "../../functions/index.js";
import { bytesToHex, canonicalJson, type Value } from "../../values/index.js";
import {
  ROWID_COLUMN,
  type AggregateCall,
  type ArithmeticOperator,
  type ColumnReference,
  type ComparisonOperator,
  type JsonPathStep,
} from "./expression.js";
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
        const calls = plan.aggregates.map((call) => this.aggregate(call));
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
    return conditions.map((condition) => this.operand(condition)).join(" AND ");
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

  private aggregate(call: AggregateCall<Plan>): string {
    if (call.arguments.length === 0) return `${call.name}(*)`;
    const distinct = call.isDistinct ? "DISTINCT " : "";
    return `${call.name}(${distinct}${this.list(call.arguments)})`;
  }

  private list(expressions: readonly Expression[]): string {
    return expressions
      .map((expression) => this.expression(expression))
      .join(", ");
  }

  /** An expression, parenthesized when it has operators of its own. */
  private operand(expression: Expression): string {
    const text = this.expression(expression);
    return isAtomic(expression) ? text : `(${text})`;
  }

  private binary(
    left: Expression,
    operator: string,
    right: Expression,
  ): string {
    return `${this.operand(left)} ${operator} ${this.operand(right)}`;
  }

  private expression(expression: Expression): string {
    switch (expression.kind) {
      case "column": {
        return formatColumn(expression);
      }
      case "constant": {
        return formatValue(expression.value, expression.type);
      }
      case "parameter": {
        return `$${String(expression.index + 1)}`;
      }
      case "comparison": {
        const { left, operator, right } = expression;
        return this.binary(left, comparisonText(operator), right);
      }
      case "conjunction": {
        const parts = expression.operands.map((part) => this.operand(part));
        return parts.join(` ${expression.operator} `);
      }
      case "arithmetic": {
        const { left, operator, right } = expression;
        return this.binary(left, arithmeticText(operator), right);
      }
      case "concatenate": {
        return this.binary(expression.left, "||", expression.right);
      }
      case "not":
      case "is-null":
      case "negate":
      case "in-list":
      case "between": {
        return this.prefixOrPostfix(expression);
      }
      case "function":
      case "case":
      case "cast":
      case "json-access": {
        return this.primary(expression);
      }
      case "scalar-subquery":
      case "exists":
      case "quantified-comparison": {
        return this.subqueryExpression(expression);
      }
    }
  }

  private prefixOrPostfix(
    expression: Extract<
      Expression,
      { kind: "not" | "is-null" | "negate" | "in-list" | "between" }
    >,
  ): string {
    switch (expression.kind) {
      case "not": {
        return `NOT ${this.operand(expression.operand)}`;
      }
      case "is-null": {
        return `${this.operand(expression.operand)} IS NULL`;
      }
      case "negate": {
        return `-${this.operand(expression.operand)}`;
      }
      case "in-list": {
        return `${this.operand(expression.needle)} IN (${this.list(expression.list)})`;
      }
      case "between": {
        const { input, lower, upper } = expression;
        return `${this.operand(input)} BETWEEN ${this.operand(lower)} AND ${this.operand(upper)}`;
      }
    }
  }

  private primary(
    expression: Extract<
      Expression,
      { kind: "function" | "case" | "cast" | "json-access" }
    >,
  ): string {
    switch (expression.kind) {
      case "function": {
        return `${expression.name}(${this.list(expression.arguments)})`;
      }
      case "case": {
        return this.caseExpression(expression);
      }
      case "cast": {
        return `CAST(${this.expression(expression.operand)} AS ${expression.type})`;
      }
      case "json-access": {
        const path = expression.path.map((step) => formatStep(step));
        return formatColumn(expression.column) + path.join("");
      }
    }
  }

  private caseExpression(
    expression: Extract<Expression, { kind: "case" }>,
  ): string {
    const parts = ["CASE"];
    if (expression.operand !== null) {
      parts.push(this.expression(expression.operand));
    }
    for (const branch of expression.branches) {
      const when = this.expression(branch.when);
      parts.push(`WHEN ${when} THEN ${this.expression(branch.result)}`);
    }
    if (expression.otherwise !== null) {
      parts.push(`ELSE ${this.expression(expression.otherwise)}`);
    }
    parts.push("END");
    return parts.join(" ");
  }

  private subqueryExpression(
    expression: Extract<
      Expression,
      { kind: "scalar-subquery" | "exists" | "quantified-comparison" }
    >,
  ): string {
    const subquery = this.reference(expression.plan, expression.isCorrelated);
    switch (expression.kind) {
      case "scalar-subquery": {
        return subquery;
      }
      case "exists": {
        return `EXISTS ${subquery}`;
      }
      case "quantified-comparison": {
        const operator = comparisonText(expression.operator);
        const needle = this.operand(expression.needle);
        return `${needle} ${operator} ${expression.quantifier} ${subquery}`;
      }
    }
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

/** Names, literals and calls: what needs no parentheses as an operand. */
function isAtomic(expression: Expression): boolean {
  switch (expression.kind) {
    case "column":
    case "constant":
    case "parameter":
    case "function":
    case "case":
    case "cast":
    case "json-access":
    case "scalar-subquery": {
      return true;
    }
    case "comparison":
    case "conjunction":
    case "not":
    case "is-null":
    case "arithmetic":
    case "negate":
    case "concatenate":
    case "in-list":
    case "between":
    case "exists":
    case "quantified-comparison": {
      return false;
    }
  }
}

function comparisonText(operator: ComparisonOperator): string {
  switch (operator) {
    case "EQUAL": {
      return "=";
    }
    case "NOT_EQUAL": {
      return "<>";
    }
    case "LESS": {
      return "<";
    }
    case "GREATER": {
      return ">";
    }
    case "LESS_EQUAL": {
      return "<=";
    }
    case "GREATER_EQUAL": {
      return ">=";
    }
  }
}

function arithmeticText(operator: ArithmeticOperator): string {
  switch (operator) {
    case "ADD": {
      return "+";
    }
    case "SUBTRACT": {
      return "-";
    }
    case "MULTIPLY": {
      return "*";
    }
    case "DIVIDE": {
      return "/";
    }
    case "MOD": {
      return "%";
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

function formatColumn(reference: ColumnReference): string {
  const { tableIndex, columnIndex } = reference.binding;
  const column = columnIndex === ROWID_COLUMN ? "rowid" : String(columnIndex);
  const depth = reference.depth === 0 ? "" : `^${String(reference.depth)}`;
  return `${reference.name}#${String(tableIndex)}.${column}${depth}`;
}

function formatStep(step: JsonPathStep): string {
  return step.kind === "key" ? `.${step.key}` : `[${String(step.index)}]`;
}

/** A constant as SQL writes it; a REAL keeps its decimal point. */
function formatValue(value: Value, type: StaticType): string {
  if (value === null) return "NULL";
  if (typeof value === "boolean") return value ? "TRUE" : "FALSE";
  if (typeof value === "number") return formatNumber(value, type);
  if (typeof value === "string") return `'${value.replaceAll("'", "''")}'`;
  if (value instanceof Uint8Array) return `X'${bytesToHex(value)}'`;
  return `JSON '${canonicalJson(value)}'`;
}

function formatNumber(value: number, type: StaticType): string {
  const text = String(value);
  const isIntegralReal = type === "REAL" && !/[.e]/.test(text);
  return isIntegralReal ? `${text}.0` : text;
}
