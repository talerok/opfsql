import {
  type LogicalOperator,
  LogicalOperatorType,
  type BoundExpression,
  BoundExpressionClass,
  type LogicalGet,
  type LogicalFilter,
  type LogicalProjection,
  type LogicalAggregate,
  type LogicalComparisonJoin,
  type LogicalOrderBy,
  type LogicalLimit,
  type LogicalUnion,
  type LogicalInsert,
  type LogicalUpdate,
  type LogicalDelete,
  type LogicalCreateTable,
  type LogicalCreateIndex,
  type LogicalAlterTable,
  type LogicalDrop,
  type LogicalCTERef,
  type LogicalMaterializedCTE,
  type LogicalRecursiveCTE,
  type ComparisonType,
} from "../binder/types.js";

// ---------------------------------------------------------------------------
// Shared symbols
// ---------------------------------------------------------------------------

const CMP: Record<ComparisonType, string> = {
  EQUAL: "=",
  NOT_EQUAL: "!=",
  LESS: "<",
  GREATER: ">",
  LESS_EQUAL: "<=",
  GREATER_EQUAL: ">=",
};

const BINARY_OP: Record<string, string> = {
  ADD: "+", SUBTRACT: "-", MULTIPLY: "*", DIVIDE: "/", MOD: "%", CONCAT: "||",
};

// ---------------------------------------------------------------------------
// Expression formatter
// ---------------------------------------------------------------------------

export function formatExpression(expr: BoundExpression): string {
  switch (expr.expressionClass) {
    case BoundExpressionClass.BOUND_COLUMN_REF:
      return `${expr.tableName}.${expr.columnName}`;
    case BoundExpressionClass.BOUND_CONSTANT:
      return formatConstant(expr.value);
    case BoundExpressionClass.BOUND_PARAMETER:
      return `$${expr.index + 1}`;
    case BoundExpressionClass.BOUND_COMPARISON:
      return `${formatExpression(expr.left)} ${CMP[expr.comparisonType]} ${formatExpression(expr.right)}`;
    case BoundExpressionClass.BOUND_CONJUNCTION:
      return expr.children.map(formatExpression).join(` ${expr.conjunctionType} `);
    case BoundExpressionClass.BOUND_OPERATOR:
      return formatOperator(expr.operatorType, expr.children);
    case BoundExpressionClass.BOUND_BETWEEN:
      return `${formatExpression(expr.input)} BETWEEN ${formatExpression(expr.lower)} AND ${formatExpression(expr.upper)}`;
    case BoundExpressionClass.BOUND_FUNCTION:
      return `${expr.functionName}(${expr.children.map(formatExpression).join(", ")})`;
    case BoundExpressionClass.BOUND_AGGREGATE:
      return formatAggregate(expr.functionName, expr.isStar, expr.distinct, expr.children);
    case BoundExpressionClass.BOUND_SUBQUERY:
      return `(${expr.subqueryType} subquery)`;
    case BoundExpressionClass.BOUND_CASE:
      return "CASE ...";
    case BoundExpressionClass.BOUND_CAST:
      return `CAST(${formatExpression(expr.child)} AS ${expr.castType})`;
    case BoundExpressionClass.BOUND_JSON_ACCESS:
      return formatExpression(expr.child) + expr.path
        .map((s) => (s.type === "field" ? `.${s.name}` : `[${s.value}]`))
        .join("");
  }
}

function formatConstant(value: unknown): string {
  if (value === null) return "NULL";
  if (typeof value === "string") return `'${value}'`;
  if (value instanceof Uint8Array) return `x'...'`;
  return String(value);
}

function formatOperator(op: string, ch: BoundExpression[]): string {
  if (op === "NOT") return `NOT ${formatExpression(ch[0])}`;
  if (op === "NEGATE") return `-${formatExpression(ch[0])}`;
  if (op === "IS_NULL") return `${formatExpression(ch[0])} IS NULL`;
  if (op === "IS_NOT_NULL") return `${formatExpression(ch[0])} IS NOT NULL`;
  if (op === "IN") return `${formatExpression(ch[0])} IN (...)`;
  if (op === "NOT_IN") return `${formatExpression(ch[0])} NOT IN (...)`;
  return `${formatExpression(ch[0])} ${BINARY_OP[op] ?? op} ${formatExpression(ch[1])}`;
}

function formatAggregate(name: string, isStar: boolean, distinct: boolean, children: BoundExpression[]): string {
  if (isStar) return `${name}(*)`;
  const prefix = distinct ? "DISTINCT " : "";
  return `${name}(${prefix}${children.map(formatExpression).join(", ")})`;
}

// ---------------------------------------------------------------------------
// Plan node formatter
// ---------------------------------------------------------------------------

function formatNode(node: LogicalOperator): string {
  switch (node.type) {
    case LogicalOperatorType.LOGICAL_GET:
      return formatGet(node as LogicalGet);
    case LogicalOperatorType.LOGICAL_FILTER:
      return `Filter (${(node as LogicalFilter).expressions.map(formatExpression).join(" AND ")})`;
    case LogicalOperatorType.LOGICAL_PROJECTION:
      return formatProjection(node as LogicalProjection);
    case LogicalOperatorType.LOGICAL_AGGREGATE_AND_GROUP_BY:
      return formatAgg(node as LogicalAggregate);
    case LogicalOperatorType.LOGICAL_COMPARISON_JOIN:
      return formatJoin(node as LogicalComparisonJoin);
    case LogicalOperatorType.LOGICAL_CROSS_PRODUCT:
      return "CrossJoin";
    case LogicalOperatorType.LOGICAL_ORDER_BY:
      return formatSort(node as LogicalOrderBy);
    case LogicalOperatorType.LOGICAL_LIMIT:
      return formatLimit(node as LogicalLimit);
    case LogicalOperatorType.LOGICAL_DISTINCT:
      return "Distinct";
    case LogicalOperatorType.LOGICAL_UNION:
      return (node as LogicalUnion).all ? "UnionAll" : "Union";
    case LogicalOperatorType.LOGICAL_INSERT:
      return `Insert ${(node as LogicalInsert).tableName}`;
    case LogicalOperatorType.LOGICAL_UPDATE:
      return `Update ${(node as LogicalUpdate).tableName}`;
    case LogicalOperatorType.LOGICAL_DELETE:
      return `Delete ${(node as LogicalDelete).tableName}`;
    case LogicalOperatorType.LOGICAL_CREATE_TABLE:
      return `CreateTable ${(node as LogicalCreateTable).schema.name}`;
    case LogicalOperatorType.LOGICAL_CREATE_INDEX: {
      const ci = node as LogicalCreateIndex;
      return `CreateIndex ${ci.index.name} on ${ci.index.tableName}`;
    }
    case LogicalOperatorType.LOGICAL_ALTER_TABLE: {
      const at = node as LogicalAlterTable;
      return `AlterTable ${at.tableName} ${at.action.type}`;
    }
    case LogicalOperatorType.LOGICAL_DROP: {
      const d = node as LogicalDrop;
      return `Drop ${d.dropType} ${d.name}`;
    }
    case LogicalOperatorType.LOGICAL_CTE_REF:
      return `CTERef ${(node as LogicalCTERef).cteName}`;
    case LogicalOperatorType.LOGICAL_MATERIALIZED_CTE:
      return `CTE ${(node as LogicalMaterializedCTE).cteName}`;
    case LogicalOperatorType.LOGICAL_RECURSIVE_CTE:
      return `RecursiveCTE ${(node as LogicalRecursiveCTE).cteName}`;
  }
}

function formatGet(n: LogicalGet): string {
  let label: string;
  if (n.indexHint?.kind === 'union') {
    const names = n.indexHint.branches.map((b) => b.indexDef.name).join(", ");
    label = `IndexUnionScan ${n.tableName} (${names})`;
  } else if (n.indexHint?.kind === 'scan') {
    if (n.indexHint.predicates.length === 0 && n.indexHint.coveredFilters.length === 0) {
      label = `IndexOrderScan ${n.tableName} (${n.indexHint.indexDef.name})`;
    } else {
      label = `IndexScan ${n.tableName} (${n.indexHint.indexDef.name})`;
    }
  } else {
    label = `Scan ${n.tableName}`;
  }
  if (n.tableFilters.length === 0) return label + cardinality(n);
  const filters = n.tableFilters
    .map((f) => `${formatExpression(f.expression)} ${CMP[f.comparisonType]} ${formatExpression(f.constant)}`)
    .join(", ");
  return `${label} [${filters}]${cardinality(n)}`;
}

function formatProjection(n: LogicalProjection): string {
  const cols = n.aliases
    .map((a, i) => a ?? formatExpression(n.expressions[i]))
    .join(", ");
  return `Projection (${cols})`;
}

function formatAgg(n: LogicalAggregate): string {
  if (n.minMaxHint) {
    return `IndexMinMax [${n.minMaxHint.functionName}] (${n.minMaxHint.indexDef.name})`;
  }
  const aggs = n.expressions.map(formatExpression).join(", ");
  if (n.groups.length === 0) return `Aggregate [${aggs}]`;
  return `Aggregate [${aggs}] group by (${n.groups.map(formatExpression).join(", ")})`;
}

function formatJoin(n: LogicalComparisonJoin): string {
  const conds = n.conditions
    .map((c) => `${formatExpression(c.left)} ${CMP[c.comparisonType]} ${formatExpression(c.right)}`)
    .join(" AND ");
  return `HashJoin [${n.joinType}] (${conds})`;
}

function formatSort(n: LogicalOrderBy): string {
  const cols = n.orders
    .map((o) => `${formatExpression(o.expression)} ${o.orderType === "ASCENDING" ? "ASC" : "DESC"}`)
    .join(", ");
  return `Sort (${cols})${n.topN != null ? ` top=${n.topN}` : ""}`;
}

function formatLimit(n: LogicalLimit): string {
  let s = `Limit ${n.limitVal ?? "ALL"}`;
  if (n.offsetVal > 0) s += ` Offset ${n.offsetVal}`;
  return s;
}

function cardinality(node: LogicalOperator): string {
  return node.estimatedCardinality > 0
    ? ` (~${node.estimatedCardinality} rows)`
    : "";
}

// ---------------------------------------------------------------------------
// Public API
// ---------------------------------------------------------------------------

export function formatPlan(node: LogicalOperator): string {
  const lines: string[] = [];

  function walk(n: LogicalOperator, depth: number): void {
    lines.push("  ".repeat(depth) + formatNode(n));
    for (const child of n.children) {
      walk(child, depth + 1);
    }
  }

  walk(node, 0);
  return lines.join("\n");
}
