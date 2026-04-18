import type {
  BoundExpression,
  BoundComparisonExpression,
  BoundConjunctionExpression,
  BoundOperatorExpression,
  BoundConstantExpression,
  LogicalOperator,
} from '../binder/types.js';
import { BoundExpressionClass } from '../binder/types.js';
import {
  mapExpression,
  mapOperatorExpressions,
  isConstant,
  isColumnRef,
  isComparison,
  isConjunction,
  isOperator,
  isFoldable,
  flipComparison,
  isNumericType,
  makeBoolConstant,
  makeNullConstant,
  makeConstant,
  sameExpression,
} from './utils/index.js';

// ============================================================================
// Expression Rewriter — applies simplification rules bottom-up
// ============================================================================

export function rewriteExpressions(plan: LogicalOperator): LogicalOperator {
  mapOperatorExpressions(plan, rewriteExpression);
  return plan;
}

function rewriteExpression(expr: BoundExpression): BoundExpression {
  // Apply rules in order. Each rule returns the expression unchanged if not applicable.
  let result = expr;
  result = constantFolding(result);
  result = comparisonSimplification(result);
  result = conjunctionSimplification(result);
  result = arithmeticSimplification(result);
  result = moveConstants(result);
  return result;
}

// ============================================================================
// Rule 1: Constant Folding — evaluate foldable expressions at optimize time
// ============================================================================

function constantFolding(expr: BoundExpression): BoundExpression {
  if (!isFoldable(expr)) return expr;
  // Already a constant — nothing to fold
  if (isConstant(expr)) return expr;

  return tryEvaluate(expr) ?? expr;
}

function tryEvaluate(expr: BoundExpression): BoundConstantExpression | null {
  switch (expr.expressionClass) {
    case BoundExpressionClass.BOUND_OPERATOR: {
      const op = expr as BoundOperatorExpression;
      const vals = op.children.map((c) => {
        if (!isConstant(c)) return undefined;
        return (c as BoundConstantExpression).value;
      });
      if (vals.some((v) => v === undefined)) return null;

      switch (op.operatorType) {
        case 'ADD': {
          const [a, b] = vals as [number, number];
          if (a === null || b === null) return makeNullConstant(op.returnType);
          return makeConstant(a + b, op.returnType);
        }
        case 'SUBTRACT': {
          const [a, b] = vals as [number, number];
          if (a === null || b === null) return makeNullConstant(op.returnType);
          return makeConstant(a - b, op.returnType);
        }
        case 'MULTIPLY': {
          const [a, b] = vals as [number, number];
          if (a === null || b === null) return makeNullConstant(op.returnType);
          return makeConstant(a * b, op.returnType);
        }
        case 'DIVIDE': {
          const [a, b] = vals as [number, number];
          if (a === null || b === null) return makeNullConstant(op.returnType);
          if (b === 0) return makeNullConstant(op.returnType);
          return makeConstant(a / b, op.returnType);
        }
        case 'MOD': {
          const [a, b] = vals as [number, number];
          if (a === null || b === null) return makeNullConstant(op.returnType);
          if (b === 0) return makeNullConstant(op.returnType);
          return makeConstant(a % b, op.returnType);
        }
        case 'NEGATE': {
          const [a] = vals as [number];
          if (a === null) return makeNullConstant(op.returnType);
          return makeConstant(-a, op.returnType);
        }
        case 'NOT': {
          const [a] = vals as [boolean];
          if (a === null) return makeNullConstant('BOOLEAN');
          return makeBoolConstant(!a);
        }
        case 'IS_NULL': {
          const [a] = vals;
          return makeBoolConstant(a === null);
        }
        case 'IS_NOT_NULL': {
          const [a] = vals;
          return makeBoolConstant(a !== null);
        }
        case 'CONCAT': {
          const [a, b] = vals;
          if (a === null || b === null) return makeNullConstant('TEXT');
          return makeConstant(String(a) + String(b), 'TEXT');
        }
        default:
          return null;
      }
    }

    case BoundExpressionClass.BOUND_COMPARISON: {
      const cmp = expr as BoundComparisonExpression;
      if (!isConstant(cmp.left) || !isConstant(cmp.right)) return null;
      const left = (cmp.left as BoundConstantExpression).value;
      const right = (cmp.right as BoundConstantExpression).value;
      if (left === null || right === null) return makeNullConstant('BOOLEAN');

      switch (cmp.comparisonType) {
        case 'EQUAL':
          return makeBoolConstant(left === right);
        case 'NOT_EQUAL':
          return makeBoolConstant(left !== right);
        case 'LESS':
          return makeBoolConstant(left < right);
        case 'GREATER':
          return makeBoolConstant(left > right);
        case 'LESS_EQUAL':
          return makeBoolConstant(left <= right);
        case 'GREATER_EQUAL':
          return makeBoolConstant(left >= right);
        default:
          return null;
      }
    }

    case BoundExpressionClass.BOUND_CONJUNCTION: {
      const conj = expr as BoundConjunctionExpression;
      const vals = conj.children.map((c) => {
        if (!isConstant(c)) return undefined;
        return (c as BoundConstantExpression).value;
      });
      if (vals.some((v) => v === undefined)) return null;

      if (conj.conjunctionType === 'AND') {
        return makeBoolConstant(vals.every((v) => v === true));
      }
      return makeBoolConstant(vals.some((v) => v === true));
    }

    default:
      return null;
  }
}

// ============================================================================
// Rule 2: Comparison Simplification
// ============================================================================

function comparisonSimplification(expr: BoundExpression): BoundExpression {
  if (!isComparison(expr)) return expr;
  const cmp = expr as BoundComparisonExpression;

  // NULL comparison: anything compared to NULL → NULL (except IS NULL/IS NOT NULL)
  if (isConstant(cmp.left) && (cmp.left as BoundConstantExpression).value === null) {
    return makeNullConstant('BOOLEAN');
  }
  if (isConstant(cmp.right) && (cmp.right as BoundConstantExpression).value === null) {
    return makeNullConstant('BOOLEAN');
  }

  // x = x → true, x <> x → false (only for column refs, which are deterministic)
  if (sameExpression(cmp.left, cmp.right) && isColumnRef(cmp.left)) {
    switch (cmp.comparisonType) {
      case 'EQUAL':
      case 'LESS_EQUAL':
      case 'GREATER_EQUAL':
        return makeBoolConstant(true);
      case 'NOT_EQUAL':
      case 'LESS':
      case 'GREATER':
        return makeBoolConstant(false);
    }
  }

  return expr;
}

// ============================================================================
// Rule 3: Conjunction Simplification
// ============================================================================

function conjunctionSimplification(expr: BoundExpression): BoundExpression {
  if (!isConjunction(expr)) return expr;
  const conj = expr as BoundConjunctionExpression;

  const remaining: BoundExpression[] = [];

  for (const child of conj.children) {
    if (isConstant(child)) {
      const val = (child as BoundConstantExpression).value;
      if (conj.conjunctionType === 'AND') {
        if (val === false || val === null) return makeBoolConstant(false);
        // AND with TRUE — skip (identity element)
        continue;
      } else {
        // OR
        if (val === true) return makeBoolConstant(true);
        // OR with FALSE — skip (identity element)
        if (val === false || val === null) continue;
      }
    }
    remaining.push(child);
  }

  if (remaining.length === 0) {
    return makeBoolConstant(conj.conjunctionType === 'AND');
  }
  if (remaining.length === 1) {
    return remaining[0];
  }
  if (remaining.length === conj.children.length) {
    return expr;
  }
  return { ...conj, children: remaining };
}

// ============================================================================
// Rule 4: Arithmetic Simplification
// ============================================================================

function arithmeticSimplification(expr: BoundExpression): BoundExpression {
  if (!isOperator(expr)) return expr;
  const op = expr as BoundOperatorExpression;

  switch (op.operatorType) {
    case 'ADD': {
      const [left, right] = op.children;
      // x + 0 → x
      if (isConstant(right) && (right as BoundConstantExpression).value === 0) return left;
      // 0 + x → x
      if (isConstant(left) && (left as BoundConstantExpression).value === 0) return right;
      return expr;
    }
    case 'SUBTRACT': {
      const [left, right] = op.children;
      // x - 0 → x
      if (isConstant(right) && (right as BoundConstantExpression).value === 0) return left;
      return expr;
    }
    case 'MULTIPLY': {
      const [left, right] = op.children;
      // x * 1 → x
      if (isConstant(right) && (right as BoundConstantExpression).value === 1) return left;
      // 1 * x → x
      if (isConstant(left) && (left as BoundConstantExpression).value === 1) return right;
      // x * 0 → CASE WHEN x IS NOT NULL THEN 0 ELSE NULL END
      if (isConstant(right) && (right as BoundConstantExpression).value === 0) {
        return constantOrNull(makeConstant(0, op.returnType), left);
      }
      // 0 * x → CASE WHEN x IS NOT NULL THEN 0 ELSE NULL END
      if (isConstant(left) && (left as BoundConstantExpression).value === 0) {
        return constantOrNull(makeConstant(0, op.returnType), right);
      }
      return expr;
    }
    case 'DIVIDE': {
      const [_left, right] = op.children;
      // x / 1 → x
      if (isConstant(right) && (right as BoundConstantExpression).value === 1) return op.children[0];
      // x / 0 → NULL
      if (isConstant(right) && (right as BoundConstantExpression).value === 0) {
        return makeNullConstant(op.returnType);
      }
      return expr;
    }
    default:
      return expr;
  }
}

// ============================================================================
// ConstantOrNull — DuckDB's ExpressionRewriter::ConstantOrNull pattern
// If the input expression could be NULL, wraps the constant in:
//   CASE WHEN expr IS NOT NULL THEN constant ELSE NULL END
// If the input is already a constant (never NULL at runtime), returns the value directly.
// ============================================================================

function constantOrNull(
  value: BoundConstantExpression,
  expr: BoundExpression,
): BoundExpression {
  // If the expression is a non-null constant, we know it can't be NULL
  if (isConstant(expr) && (expr as BoundConstantExpression).value !== null) {
    return value;
  }
  // Otherwise wrap: CASE WHEN expr IS NOT NULL THEN value ELSE NULL END
  return {
    expressionClass: BoundExpressionClass.BOUND_CASE,
    caseChecks: [
      {
        when: {
          expressionClass: BoundExpressionClass.BOUND_OPERATOR,
          operatorType: 'IS_NOT_NULL',
          children: [expr],
          returnType: 'BOOLEAN',
        } as BoundOperatorExpression,
        then: value,
      },
    ],
    elseExpr: makeNullConstant(value.returnType),
    returnType: value.returnType,
  };
}

// ============================================================================
// Rule 5: Move Constants — normalize constant position in comparisons
// ============================================================================

function moveConstants(expr: BoundExpression): BoundExpression {
  if (!isComparison(expr)) return expr;
  const cmp = expr as BoundComparisonExpression;

  // Normalize: if left is a constant and right is not, flip
  if (isConstant(cmp.left) && !isConstant(cmp.right)) {
    return {
      ...cmp,
      left: cmp.right,
      right: cmp.left,
      comparisonType: flipComparison(cmp.comparisonType),
    };
  }

  // Move arithmetic constants: (x + C1) COMP C2 → x COMP (C2 - C1)
  if (isOperator(cmp.left) && isConstant(cmp.right)) {
    const leftOp = cmp.left as BoundOperatorExpression;
    const rightConst = cmp.right as BoundConstantExpression;

    if (
      (leftOp.operatorType === 'ADD' || leftOp.operatorType === 'SUBTRACT') &&
      leftOp.children.length === 2 &&
      isConstant(leftOp.children[1]) &&
      isNumericType(rightConst.returnType)
    ) {
      const c1 = (leftOp.children[1] as BoundConstantExpression).value as number;
      const c2 = rightConst.value as number;

      if (typeof c1 === 'number' && typeof c2 === 'number') {
        const newRHS =
          leftOp.operatorType === 'ADD' ? c2 - c1 : c2 + c1;

        return {
          ...cmp,
          left: leftOp.children[0],
          right: makeConstant(newRHS, rightConst.returnType),
        };
      }
    }
  }

  return expr;
}
