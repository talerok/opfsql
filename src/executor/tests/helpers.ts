import type { ColumnBinding, BoundExpression } from '../../binder/types.js';
import { BoundExpressionClass } from '../../binder/types.js';
import type { LogicalType } from '../../store/types.js';
import type { PhysicalOperator, Tuple } from '../types.js';
import type { EvalContext } from '../evaluate/context.js';

// ---------------------------------------------------------------------------
// Mock physical operator — returns predefined batches
// ---------------------------------------------------------------------------

export class MockOperator implements PhysicalOperator {
  private index = 0;

  constructor(
    private readonly batches: Tuple[][],
    private readonly layout: ColumnBinding[],
  ) {}

  getLayout(): ColumnBinding[] {
    return this.layout;
  }

  async next(): Promise<Tuple[] | null> {
    if (this.index >= this.batches.length) return null;
    return this.batches[this.index++];
  }

  async reset(): Promise<void> {
    this.index = 0;
  }
}

// ---------------------------------------------------------------------------
// Expression builders — shorthand for constructing BoundExpression nodes
// ---------------------------------------------------------------------------

export function colRef(
  tableIndex: number,
  columnIndex: number,
  columnName = `col${columnIndex}`,
  returnType: LogicalType = 'INTEGER',
): BoundExpression {
  return {
    expressionClass: BoundExpressionClass.BOUND_COLUMN_REF,
    binding: { tableIndex, columnIndex },
    tableName: `t${tableIndex}`,
    columnName,
    returnType,
  };
}

export function constant(
  value: string | number | boolean | null,
  returnType?: LogicalType,
): BoundExpression {
  const rt: LogicalType =
    returnType ??
    (typeof value === 'string'
      ? 'TEXT'
      : typeof value === 'number'
        ? 'INTEGER'
        : typeof value === 'boolean'
          ? 'BOOLEAN'
          : 'NULL');
  return {
    expressionClass: BoundExpressionClass.BOUND_CONSTANT,
    value,
    returnType: rt,
  };
}

export function comparison(
  left: BoundExpression,
  right: BoundExpression,
  comparisonType: 'EQUAL' | 'NOT_EQUAL' | 'LESS' | 'GREATER' | 'LESS_EQUAL' | 'GREATER_EQUAL' = 'EQUAL',
): BoundExpression {
  return {
    expressionClass: BoundExpressionClass.BOUND_COMPARISON,
    comparisonType,
    left,
    right,
    returnType: 'BOOLEAN',
  };
}

export function conjunction(
  type: 'AND' | 'OR',
  ...children: BoundExpression[]
): BoundExpression {
  return {
    expressionClass: BoundExpressionClass.BOUND_CONJUNCTION,
    conjunctionType: type,
    children,
    returnType: 'BOOLEAN',
  };
}

export function operator(
  operatorType: string,
  children: BoundExpression[],
  returnType: LogicalType = 'INTEGER',
): BoundExpression {
  return {
    expressionClass: BoundExpressionClass.BOUND_OPERATOR,
    operatorType: operatorType as any,
    children,
    returnType,
  };
}

export function between(
  input: BoundExpression,
  lower: BoundExpression,
  upper: BoundExpression,
): BoundExpression {
  return {
    expressionClass: BoundExpressionClass.BOUND_BETWEEN,
    input,
    lower,
    upper,
    returnType: 'BOOLEAN',
  };
}

export function fnCall(
  functionName: string,
  children: BoundExpression[],
  returnType: LogicalType = 'TEXT',
): BoundExpression {
  return {
    expressionClass: BoundExpressionClass.BOUND_FUNCTION,
    functionName,
    children,
    returnType,
  };
}

export function cast(
  child: BoundExpression,
  castType: LogicalType,
): BoundExpression {
  return {
    expressionClass: BoundExpressionClass.BOUND_CAST,
    child,
    castType,
    returnType: castType,
  };
}

export function caseExpr(
  checks: Array<{ when: BoundExpression; then: BoundExpression }>,
  elseExpr: BoundExpression | null = null,
  returnType: LogicalType = 'INTEGER',
): BoundExpression {
  return {
    expressionClass: BoundExpressionClass.BOUND_CASE,
    caseChecks: checks,
    elseExpr,
    returnType,
  };
}

// ---------------------------------------------------------------------------
// Layout builders
// ---------------------------------------------------------------------------

export function layout(
  ...bindings: Array<[tableIndex: number, columnIndex: number]>
): ColumnBinding[] {
  return bindings.map(([tableIndex, columnIndex]) => ({
    tableIndex,
    columnIndex,
  }));
}

// ---------------------------------------------------------------------------
// Noop eval context
// ---------------------------------------------------------------------------

export const noopCtx: EvalContext = {
  executeSubplan: async () => [],
};
