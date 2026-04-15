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
  type JoinCondition,
} from "../binder/types.js";

// ---------------------------------------------------------------------------
// Expression formatter
// ---------------------------------------------------------------------------

const CMP_SYMBOLS: Record<ComparisonType, string> = {
  EQUAL: "=",
  NOT_EQUAL: "!=",
  LESS: "<",
  GREATER: ">",
  LESS_EQUAL: "<=",
  GREATER_EQUAL: ">=",
};

export function formatExpression(expr: BoundExpression): string {
  switch (expr.expressionClass) {
    case BoundExpressionClass.BOUND_COLUMN_REF:
      return `${expr.tableName}.${expr.columnName}`;

    case BoundExpressionClass.BOUND_CONSTANT: {
      if (expr.value === null) return "NULL";
      if (typeof expr.value === "string") return `'${expr.value}'`;
      if (expr.value instanceof Uint8Array) return `x'...'`;
      return String(expr.value);
    }

    case BoundExpressionClass.BOUND_PARAMETER:
      return `$${expr.index + 1}`;

    case BoundExpressionClass.BOUND_COMPARISON:
      return `${formatExpression(expr.left)} ${CMP_SYMBOLS[expr.comparisonType]} ${formatExpression(expr.right)}`;

    case BoundExpressionClass.BOUND_CONJUNCTION:
      return expr.children
        .map(formatExpression)
        .join(` ${expr.conjunctionType} `);

    case BoundExpressionClass.BOUND_OPERATOR: {
      const op = expr.operatorType;
      const ch = expr.children;
      if (op === "NOT") return `NOT ${formatExpression(ch[0])}`;
      if (op === "NEGATE") return `-${formatExpression(ch[0])}`;
      if (op === "IS_NULL") return `${formatExpression(ch[0])} IS NULL`;
      if (op === "IS_NOT_NULL") return `${formatExpression(ch[0])} IS NOT NULL`;
      if (op === "IN") return `${formatExpression(ch[0])} IN (...)`;
      if (op === "NOT_IN") return `${formatExpression(ch[0])} NOT IN (...)`;
      const symbols: Record<string, string> = {
        ADD: "+", SUBTRACT: "-", MULTIPLY: "*", DIVIDE: "/", MOD: "%", CONCAT: "||",
      };
      return `${formatExpression(ch[0])} ${symbols[op] ?? op} ${formatExpression(ch[1])}`;
    }

    case BoundExpressionClass.BOUND_BETWEEN:
      return `${formatExpression(expr.input)} BETWEEN ${formatExpression(expr.lower)} AND ${formatExpression(expr.upper)}`;

    case BoundExpressionClass.BOUND_FUNCTION:
      return `${expr.functionName}(${expr.children.map(formatExpression).join(", ")})`;

    case BoundExpressionClass.BOUND_AGGREGATE: {
      if (expr.isStar) return `${expr.functionName}(*)`;
      const prefix = expr.distinct ? "DISTINCT " : "";
      return `${expr.functionName}(${prefix}${expr.children.map(formatExpression).join(", ")})`;
    }

    case BoundExpressionClass.BOUND_SUBQUERY:
      return `(${expr.subqueryType} subquery)`;

    case BoundExpressionClass.BOUND_CASE:
      return "CASE ...";

    case BoundExpressionClass.BOUND_CAST:
      return `CAST(${formatExpression(expr.child)} AS ${expr.castType})`;

    case BoundExpressionClass.BOUND_JSON_ACCESS: {
      const base = formatExpression(expr.child);
      const path = expr.path
        .map((seg) => (seg.type === "field" ? `.${seg.name}` : `[${seg.value}]`))
        .join("");
      return `${base}${path}`;
    }
  }
}

function formatCondition(c: JoinCondition): string {
  return `${formatExpression(c.left)} ${CMP_SYMBOLS[c.comparisonType]} ${formatExpression(c.right)}`;
}

// ---------------------------------------------------------------------------
// Plan node formatter
// ---------------------------------------------------------------------------

function cardinality(node: LogicalOperator): string {
  return node.estimatedCardinality > 0
    ? ` (~${node.estimatedCardinality} rows)`
    : "";
}

function formatNode(node: LogicalOperator): string {
  switch (node.type) {
    case LogicalOperatorType.LOGICAL_GET: {
      const n = node as LogicalGet;
      const label = n.indexHint
        ? `IndexScan ${n.tableName} (${n.indexHint.indexDef.name})`
        : `Scan ${n.tableName}`;
      const filters = n.tableFilters.length > 0
        ? ` [${n.tableFilters.map((f) => {
            const col = n.schema.columns[f.columnIndex]?.name ?? `col${f.columnIndex}`;
            return `${col} ${CMP_SYMBOLS[f.comparisonType]} ${formatExpression(f.constant)}`;
          }).join(", ")}]`
        : "";
      return `${label}${filters}${cardinality(node)}`;
    }

    case LogicalOperatorType.LOGICAL_FILTER: {
      const n = node as LogicalFilter;
      const cond = n.expressions.map(formatExpression).join(" AND ");
      return `Filter (${cond})`;
    }

    case LogicalOperatorType.LOGICAL_PROJECTION: {
      const n = node as LogicalProjection;
      const cols = n.aliases
        .map((a, i) => a ?? formatExpression(n.expressions[i]))
        .join(", ");
      return `Projection (${cols})`;
    }

    case LogicalOperatorType.LOGICAL_AGGREGATE_AND_GROUP_BY: {
      const n = node as LogicalAggregate;
      const aggs = n.expressions.map(formatExpression).join(", ");
      if (n.groups.length > 0) {
        const groups = n.groups.map(formatExpression).join(", ");
        return `Aggregate [${aggs}] group by (${groups})`;
      }
      return `Aggregate [${aggs}]`;
    }

    case LogicalOperatorType.LOGICAL_COMPARISON_JOIN: {
      const n = node as LogicalComparisonJoin;
      const conds = n.conditions.map(formatCondition).join(" AND ");
      return `HashJoin [${n.joinType}] (${conds})`;
    }

    case LogicalOperatorType.LOGICAL_CROSS_PRODUCT:
      return "CrossJoin";

    case LogicalOperatorType.LOGICAL_ORDER_BY: {
      const n = node as LogicalOrderBy;
      const cols = n.orders
        .map((o) => {
          const dir = o.orderType === "ASCENDING" ? "ASC" : "DESC";
          return `${formatExpression(o.expression)} ${dir}`;
        })
        .join(", ");
      const topN = n.topN != null ? ` top=${n.topN}` : "";
      return `Sort (${cols})${topN}`;
    }

    case LogicalOperatorType.LOGICAL_LIMIT: {
      const n = node as LogicalLimit;
      let s = `Limit ${n.limitVal ?? "ALL"}`;
      if (n.offsetVal > 0) s += ` Offset ${n.offsetVal}`;
      return s;
    }

    case LogicalOperatorType.LOGICAL_DISTINCT:
      return "Distinct";

    case LogicalOperatorType.LOGICAL_UNION: {
      const n = node as LogicalUnion;
      return n.all ? "UnionAll" : "Union";
    }

    case LogicalOperatorType.LOGICAL_INSERT: {
      const n = node as LogicalInsert;
      return `Insert ${n.tableName}`;
    }

    case LogicalOperatorType.LOGICAL_UPDATE: {
      const n = node as LogicalUpdate;
      return `Update ${n.tableName}`;
    }

    case LogicalOperatorType.LOGICAL_DELETE: {
      const n = node as LogicalDelete;
      return `Delete ${n.tableName}`;
    }

    case LogicalOperatorType.LOGICAL_CREATE_TABLE: {
      const n = node as LogicalCreateTable;
      return `CreateTable ${n.schema.name}`;
    }

    case LogicalOperatorType.LOGICAL_CREATE_INDEX: {
      const n = node as LogicalCreateIndex;
      return `CreateIndex ${n.index.name} on ${n.index.tableName}`;
    }

    case LogicalOperatorType.LOGICAL_ALTER_TABLE: {
      const n = node as LogicalAlterTable;
      return `AlterTable ${n.tableName} ${n.action.type}`;
    }

    case LogicalOperatorType.LOGICAL_DROP: {
      const n = node as LogicalDrop;
      return `Drop ${n.dropType} ${n.name}`;
    }

    case LogicalOperatorType.LOGICAL_CTE_REF: {
      const n = node as LogicalCTERef;
      return `CTERef ${n.cteName}`;
    }

    case LogicalOperatorType.LOGICAL_MATERIALIZED_CTE: {
      const n = node as LogicalMaterializedCTE;
      return `CTE ${n.cteName}`;
    }

    case LogicalOperatorType.LOGICAL_RECURSIVE_CTE: {
      const n = node as LogicalRecursiveCTE;
      return `RecursiveCTE ${n.cteName}`;
    }
  }
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
