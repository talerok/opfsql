import { describe, it, expect } from 'vitest';
import type { TableFilter } from '../../binder/types.js';
import { BoundExpressionClass } from '../../binder/types.js';
import { compileExpression, compileFilter } from '../evaluate/compile.js';
import { buildResolver } from '../resolve.js';
import { colRef, constant, comparison, layout, noopCtx } from './helpers.js';

// ---------------------------------------------------------------------------
// compileExpression
// ---------------------------------------------------------------------------

describe('compileExpression', () => {
  const bindings = layout([0, 0], [0, 1], [0, 2]);
  const resolver = buildResolver(bindings);

  it('column ref returns value at resolved position', () => {
    const fn = compileExpression(colRef(0, 1), resolver, noopCtx);
    expect(fn([10, 'hello', 30])).toBe('hello');
  });

  it('column ref returns null for undefined value', () => {
    const fn = compileExpression(colRef(0, 2), resolver, noopCtx);
    expect(fn([10, 'hello', undefined as any])).toBeNull();
  });

  it('constant returns fixed value', () => {
    const fn = compileExpression(constant(42), resolver, noopCtx);
    expect(fn([1, 2, 3])).toBe(42);
    expect(fn([])).toBe(42); // tuple doesn't matter
  });

  it('constant null returns null', () => {
    const fn = compileExpression(constant(null), resolver, noopCtx);
    expect(fn([])).toBeNull();
  });

  it('unresolved column ref falls back to general evaluator', () => {
    // Column ref with tableIndex=5 doesn't exist in resolver
    const fn = compileExpression(colRef(5, 0), resolver, noopCtx);
    // General evaluator should handle this (returns null for unresolved)
    expect(typeof fn).toBe('function');
  });

  it('complex expression falls back to general evaluator', () => {
    const expr = comparison(colRef(0, 0), constant(5), 'GREATER');
    const fn = compileExpression(expr, resolver, noopCtx);
    // Should still return a function
    expect(fn([10, 'a', 1])).toBe(true);
    expect(fn([3, 'a', 1])).toBe(false);
  });

  it('JSON access compiles to closure', () => {
    const jsonExpr = {
      expressionClass: BoundExpressionClass.BOUND_JSON_ACCESS as const,
      child: {
        expressionClass: BoundExpressionClass.BOUND_COLUMN_REF as const,
        binding: { tableIndex: 0, columnIndex: 0 },
        tableName: 't0',
        columnName: 'data',
        returnType: 'JSON' as const,
      },
      path: [{ type: 'field' as const, name: 'x' }],
      returnType: 'JSON' as const,
    };
    const fn = compileExpression(jsonExpr, resolver, noopCtx);
    expect(fn([{ x: 42 }, 'b', 'c'])).toBe(42);
  });
});

// ---------------------------------------------------------------------------
// compileFilter
// ---------------------------------------------------------------------------

describe('compileFilter', () => {
  const bindings = layout([0, 0], [0, 1]);
  const resolver = buildResolver(bindings);

  it('constant-based filter', () => {
    const filter: TableFilter = {
      expression: colRef(0, 0, 'id', 'INTEGER'),
      comparisonType: 'EQUAL',
      constant: {
        expressionClass: BoundExpressionClass.BOUND_CONSTANT,
        value: 5,
        returnType: 'INTEGER',
      },
    };
    const compiled = compileFilter(filter, resolver, noopCtx);
    expect(compiled.comparisonType).toBe('EQUAL');
    expect(compiled.getValue([5, 'x'])).toBe(5);
    expect(compiled.getValue([3, 'x'])).toBe(3);
    expect(compiled.getConstant()).toBe(5);
  });

  it('parameter-based filter resolves from params', () => {
    const filter: TableFilter = {
      expression: colRef(0, 1, 'name', 'TEXT'),
      comparisonType: 'EQUAL',
      constant: {
        expressionClass: BoundExpressionClass.BOUND_PARAMETER,
        index: 0,
        returnType: 'TEXT',
      },
    };
    const compiled = compileFilter(filter, resolver, noopCtx);
    expect(compiled.getConstant(['Alice', 'Bob'])).toBe('Alice');
    expect(compiled.getConstant(['Carol'])).toBe('Carol');
  });

  it('parameter-based filter returns null when no params', () => {
    const filter: TableFilter = {
      expression: colRef(0, 0, 'id', 'INTEGER'),
      comparisonType: 'EQUAL',
      constant: {
        expressionClass: BoundExpressionClass.BOUND_PARAMETER,
        index: 2,
        returnType: 'INTEGER',
      },
    };
    const compiled = compileFilter(filter, resolver, noopCtx);
    expect(compiled.getConstant()).toBeNull();
    expect(compiled.getConstant(undefined)).toBeNull();
  });

  it('preserves comparison type', () => {
    for (const ct of ['EQUAL', 'NOT_EQUAL', 'LESS', 'GREATER', 'LESS_EQUAL', 'GREATER_EQUAL'] as const) {
      const filter: TableFilter = {
        expression: colRef(0, 0),
        comparisonType: ct,
        constant: { expressionClass: BoundExpressionClass.BOUND_CONSTANT, value: 1, returnType: 'INTEGER' },
      };
      const compiled = compileFilter(filter, resolver, noopCtx);
      expect(compiled.comparisonType).toBe(ct);
    }
  });
});
