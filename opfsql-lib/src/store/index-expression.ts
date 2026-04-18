import type {
  BoundCaseExpression,
  BoundCastExpression,
  BoundColumnRefExpression,
  BoundComparisonExpression,
  BoundConstantExpression,
  BoundExpression,
  BoundFunctionExpression,
  BoundJsonAccessExpression,
  BoundOperatorExpression,
} from "../binder/types.js";
import { BoundExpressionClass } from "../binder/types.js";
import type {
  IndexExpression,
  TableSchema,
} from "../types.js";

// ---------------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------------

function findColumn(schema: TableSchema, name: string): number {
  const idx = schema.columns.findIndex(
    (c) => c.name.toLowerCase() === name.toLowerCase(),
  );
  if (idx === -1) {
    throw new Error(`Column "${name}" not found in table "${schema.name}"`);
  }
  return idx;
}

function makeColumnRef(
  schema: TableSchema,
  tableIndex: number,
  colName: string,
): BoundColumnRefExpression {
  const colIdx = findColumn(schema, colName);
  const col = schema.columns[colIdx];
  return {
    expressionClass: BoundExpressionClass.BOUND_COLUMN_REF,
    binding: { tableIndex, columnIndex: colIdx },
    tableName: schema.name,
    columnName: col.name,
    returnType: col.type,
  };
}

// ---------------------------------------------------------------------------
// BoundExpression → IndexExpression (for catalog storage)
//
// Strips binding info (tableIndex/columnIndex), replaces with column names.
// ---------------------------------------------------------------------------

export function boundToIndexExpression(
  expr: BoundExpression,
  schema: TableSchema,
): IndexExpression {
  const conv = (e: BoundExpression) => boundToIndexExpression(e, schema);
  const rt = expr.returnType;

  switch (expr.expressionClass) {
    case BoundExpressionClass.BOUND_COLUMN_REF: {
      const ref = expr as BoundColumnRefExpression;
      return {
        type: "column",
        name: schema.columns[ref.binding.columnIndex].name,
        returnType: rt,
      };
    }
    case BoundExpressionClass.BOUND_JSON_ACCESS: {
      const ja = expr as BoundJsonAccessExpression;
      return {
        type: "json_access",
        column: schema.columns[ja.child.binding.columnIndex].name,
        path: ja.path,
        returnType: rt,
      };
    }
    case BoundExpressionClass.BOUND_FUNCTION: {
      const fn = expr as BoundFunctionExpression;
      return {
        type: "function",
        name: fn.functionName,
        args: fn.children.map(conv),
        returnType: rt,
      };
    }
    case BoundExpressionClass.BOUND_CAST: {
      const cast = expr as BoundCastExpression;
      return {
        type: "cast",
        child: conv(cast.child),
        castType: cast.castType,
        returnType: rt,
      };
    }
    case BoundExpressionClass.BOUND_OPERATOR: {
      const op = expr as BoundOperatorExpression;
      return {
        type: "operator",
        operatorType: op.operatorType,
        args: op.children.map(conv),
        returnType: rt,
      };
    }
    case BoundExpressionClass.BOUND_COMPARISON: {
      const cmp = expr as BoundComparisonExpression;
      return {
        type: "comparison",
        comparisonType: cmp.comparisonType,
        left: conv(cmp.left),
        right: conv(cmp.right),
        returnType: rt,
      };
    }
    case BoundExpressionClass.BOUND_CASE: {
      const cs = expr as BoundCaseExpression;
      return {
        type: "case",
        checks: cs.caseChecks.map((ch) => ({
          when: conv(ch.when),
          then: conv(ch.then),
        })),
        elseExpr: cs.elseExpr ? conv(cs.elseExpr) : null,
        returnType: rt,
      };
    }
    case BoundExpressionClass.BOUND_CONSTANT: {
      const ct = expr as BoundConstantExpression;
      return {
        type: "constant",
        value: ct.value as string | number | boolean | null,
        returnType: rt,
      };
    }
    default:
      throw new Error(
        `Unsupported expression type in index: ${expr.expressionClass}`,
      );
  }
}

// ---------------------------------------------------------------------------
// IndexExpression → BoundExpression (for optimizer/executor use)
//
// Resolves column names back to bindings via schema lookup.
// ---------------------------------------------------------------------------

export function bindIndexExpression(
  expr: IndexExpression,
  schema: TableSchema,
  tableIndex: number,
): BoundExpression {
  const bind = (e: IndexExpression) =>
    bindIndexExpression(e, schema, tableIndex);

  switch (expr.type) {
    case "column":
      return {
        ...makeColumnRef(schema, tableIndex, expr.name),
        returnType: expr.returnType,
      };

    case "json_access":
      return {
        expressionClass: BoundExpressionClass.BOUND_JSON_ACCESS,
        child: makeColumnRef(schema, tableIndex, expr.column),
        path: expr.path,
        returnType: expr.returnType,
      };

    case "function":
      return {
        expressionClass: BoundExpressionClass.BOUND_FUNCTION,
        functionName: expr.name,
        children: expr.args.map(bind),
        returnType: expr.returnType,
      };

    case "cast":
      return {
        expressionClass: BoundExpressionClass.BOUND_CAST,
        child: bind(expr.child),
        castType: expr.castType,
        returnType: expr.returnType,
      };

    case "operator":
      return {
        expressionClass: BoundExpressionClass.BOUND_OPERATOR,
        operatorType: expr.operatorType,
        children: expr.args.map(bind),
        returnType: expr.returnType,
      };

    case "comparison":
      return {
        expressionClass: BoundExpressionClass.BOUND_COMPARISON,
        comparisonType: expr.comparisonType,
        left: bind(expr.left),
        right: bind(expr.right),
        returnType: expr.returnType as "BOOLEAN",
      };

    case "case":
      return {
        expressionClass: BoundExpressionClass.BOUND_CASE,
        caseChecks: expr.checks.map((ch) => ({
          when: bind(ch.when),
          then: bind(ch.then),
        })),
        elseExpr: expr.elseExpr ? bind(expr.elseExpr) : null,
        returnType: expr.returnType,
      };

    case "constant":
      return {
        expressionClass: BoundExpressionClass.BOUND_CONSTANT,
        value: expr.value,
        returnType: expr.returnType,
      };
  }
}

// ---------------------------------------------------------------------------
// Extract column names referenced by an IndexExpression
// ---------------------------------------------------------------------------

export function getIndexColumns(expr: IndexExpression): string[] {
  switch (expr.type) {
    case "column":
      return [expr.name];
    case "json_access":
      return [expr.column];
    case "function":
    case "operator":
      return expr.args.flatMap(getIndexColumns);
    case "cast":
      return getIndexColumns(expr.child);
    case "comparison":
      return [...getIndexColumns(expr.left), ...getIndexColumns(expr.right)];
    case "constant":
      return [];
    case "case": {
      const cols = expr.checks.flatMap((ch) => [
        ...getIndexColumns(ch.when),
        ...getIndexColumns(ch.then),
      ]);
      if (expr.elseExpr) {
        cols.push(...getIndexColumns(expr.elseExpr));
      }
      return cols;
    }
  }
}
