import { describe, it, expect } from 'vitest';
import type { IndexExpression, TableSchema } from '../../types.js';
import type { BoundExpression } from '../../binder/types.js';
import { BoundExpressionClass } from '../../binder/types.js';
import {
  boundToIndexExpression,
  bindIndexExpression,
  getIndexColumns,
} from '../index-expression.js';

const schema: TableSchema = {
  name: 't',
  columns: [
    { name: 'a', type: 'INTEGER', nullable: false, primaryKey: false, unique: false, autoIncrement: false, defaultValue: null },
    { name: 'b', type: 'INTEGER', nullable: false, primaryKey: false, unique: false, autoIncrement: false, defaultValue: null },
    { name: 'data', type: 'JSON', nullable: true, primaryKey: false, unique: false, autoIncrement: false, defaultValue: null },
    { name: 'name', type: 'TEXT', nullable: true, primaryKey: false, unique: false, autoIncrement: false, defaultValue: null },
  ],
};

// ---------------------------------------------------------------------------
// Helpers to build BoundExpressions
// ---------------------------------------------------------------------------

function colRef(colIndex: number): BoundExpression {
  const col = schema.columns[colIndex];
  return {
    expressionClass: BoundExpressionClass.BOUND_COLUMN_REF,
    binding: { tableIndex: 0, columnIndex: colIndex },
    tableName: 't',
    columnName: col.name,
    returnType: col.type,
  };
}

// ---------------------------------------------------------------------------
// operator
// ---------------------------------------------------------------------------

describe('IndexExpression — operator', () => {
  it('round-trips a + b through boundToIndexExpression → bindIndexExpression', () => {
    const bound: BoundExpression = {
      expressionClass: BoundExpressionClass.BOUND_OPERATOR,
      operatorType: 'ADD',
      children: [colRef(0), colRef(1)],
      returnType: 'INTEGER',
    };
    const idx = boundToIndexExpression(bound, schema);
    expect(idx).toEqual({
      type: 'operator',
      operatorType: 'ADD',
      args: [
        { type: 'column', name: 'a', returnType: 'INTEGER' },
        { type: 'column', name: 'b', returnType: 'INTEGER' },
      ],
      returnType: 'INTEGER',
    });

    const rebound = bindIndexExpression(idx, schema, 0);
    expect(rebound.expressionClass).toBe(BoundExpressionClass.BOUND_OPERATOR);
    const op = rebound as Extract<BoundExpression, { expressionClass: typeof BoundExpressionClass.BOUND_OPERATOR }>;
    expect(op.operatorType).toBe('ADD');
    expect(op.children).toHaveLength(2);
    expect(op.returnType).toBe('INTEGER');
  });

  it('handles NEGATE(-a)', () => {
    const bound: BoundExpression = {
      expressionClass: BoundExpressionClass.BOUND_OPERATOR,
      operatorType: 'NEGATE',
      children: [colRef(0)],
      returnType: 'INTEGER',
    };
    const idx = boundToIndexExpression(bound, schema);
    expect(idx).toEqual({
      type: 'operator',
      operatorType: 'NEGATE',
      args: [{ type: 'column', name: 'a', returnType: 'INTEGER' }],
      returnType: 'INTEGER',
    });
    const rebound = bindIndexExpression(idx, schema, 0);
    expect(rebound.returnType).toBe('INTEGER');
  });

  it('handles CONCAT(name || name)', () => {
    const bound: BoundExpression = {
      expressionClass: BoundExpressionClass.BOUND_OPERATOR,
      operatorType: 'CONCAT',
      children: [colRef(3), colRef(3)],
      returnType: 'TEXT',
    };
    const idx = boundToIndexExpression(bound, schema);
    const rebound = bindIndexExpression(idx, schema, 0);
    expect(rebound.returnType).toBe('TEXT');
  });

  it('nested: (a + b) * a', () => {
    const add: BoundExpression = {
      expressionClass: BoundExpressionClass.BOUND_OPERATOR,
      operatorType: 'ADD',
      children: [colRef(0), colRef(1)],
      returnType: 'INTEGER',
    };
    const mul: BoundExpression = {
      expressionClass: BoundExpressionClass.BOUND_OPERATOR,
      operatorType: 'MULTIPLY',
      children: [add, colRef(0)],
      returnType: 'INTEGER',
    };
    const idx = boundToIndexExpression(mul, schema);
    expect(idx.type).toBe('operator');
    const rebound = bindIndexExpression(idx, schema, 0);
    expect(rebound.expressionClass).toBe(BoundExpressionClass.BOUND_OPERATOR);
  });

  it('operator with json_access child: data.x + data.y', () => {
    const jx: BoundExpression = {
      expressionClass: BoundExpressionClass.BOUND_JSON_ACCESS,
      child: {
        expressionClass: BoundExpressionClass.BOUND_COLUMN_REF,
        binding: { tableIndex: 0, columnIndex: 2 },
        tableName: 't',
        columnName: 'data',
        returnType: 'JSON',
      },
      path: [{ type: 'field', name: 'x' }],
      returnType: 'JSON',
    };
    const jy: BoundExpression = {
      expressionClass: BoundExpressionClass.BOUND_JSON_ACCESS,
      child: {
        expressionClass: BoundExpressionClass.BOUND_COLUMN_REF,
        binding: { tableIndex: 0, columnIndex: 2 },
        tableName: 't',
        columnName: 'data',
        returnType: 'JSON',
      },
      path: [{ type: 'field', name: 'y' }],
      returnType: 'JSON',
    };
    const bound: BoundExpression = {
      expressionClass: BoundExpressionClass.BOUND_OPERATOR,
      operatorType: 'ADD',
      children: [jx, jy],
      returnType: 'INTEGER',
    };
    const idx = boundToIndexExpression(bound, schema);
    expect(idx).toEqual({
      type: 'operator',
      operatorType: 'ADD',
      args: [
        { type: 'json_access', column: 'data', path: [{ type: 'field', name: 'x' }], returnType: 'JSON' },
        { type: 'json_access', column: 'data', path: [{ type: 'field', name: 'y' }], returnType: 'JSON' },
      ],
      returnType: 'INTEGER',
    });
  });

  it('getIndexColumns for operator', () => {
    const idx: IndexExpression = {
      type: 'operator',
      operatorType: 'ADD',
      args: [
        { type: 'column', name: 'a', returnType: 'INTEGER' },
        { type: 'column', name: 'b', returnType: 'INTEGER' },
      ],
      returnType: 'INTEGER',
    };
    expect(getIndexColumns(idx).sort()).toEqual(['a', 'b']);
  });
});

// ---------------------------------------------------------------------------
// case
// ---------------------------------------------------------------------------

describe('IndexExpression — case', () => {
  it('round-trips CASE WHEN a > 0 THEN a ELSE b END', () => {
    const bound: BoundExpression = {
      expressionClass: BoundExpressionClass.BOUND_CASE,
      caseChecks: [
        {
          when: {
            expressionClass: BoundExpressionClass.BOUND_COMPARISON,
            comparisonType: 'GREATER',
            left: colRef(0),
            right: {
              expressionClass: BoundExpressionClass.BOUND_CONSTANT,
              value: 0,
              returnType: 'INTEGER',
            },
            returnType: 'BOOLEAN',
          },
          then: colRef(0),
        },
      ],
      elseExpr: colRef(1),
      returnType: 'INTEGER',
    };
    const idx = boundToIndexExpression(bound, schema);
    expect(idx.type).toBe('case');

    const rebound = bindIndexExpression(idx, schema, 0);
    expect(rebound.expressionClass).toBe(BoundExpressionClass.BOUND_CASE);
    const cs = rebound as Extract<BoundExpression, { expressionClass: typeof BoundExpressionClass.BOUND_CASE }>;
    expect(cs.caseChecks).toHaveLength(1);
    expect(cs.elseExpr).not.toBeNull();
    expect(cs.returnType).toBe('INTEGER');
  });

  it('handles CASE without ELSE', () => {
    const bound: BoundExpression = {
      expressionClass: BoundExpressionClass.BOUND_CASE,
      caseChecks: [{ when: colRef(0), then: colRef(1) }],
      elseExpr: null,
      returnType: 'INTEGER',
    };
    const idx = boundToIndexExpression(bound, schema);
    expect((idx as any).elseExpr).toBeNull();

    const rebound = bindIndexExpression(idx, schema, 0);
    expect((rebound as any).elseExpr).toBeNull();
  });

  it('getIndexColumns for case', () => {
    const idx: IndexExpression = {
      type: 'case',
      checks: [
        {
          when: { type: 'column', name: 'a', returnType: 'INTEGER' },
          then: { type: 'column', name: 'b', returnType: 'INTEGER' },
        },
      ],
      elseExpr: { type: 'column', name: 'data', returnType: 'JSON' },
      returnType: 'INTEGER',
    };
    expect(getIndexColumns(idx).sort()).toEqual(['a', 'b', 'data']);
  });
});

// ---------------------------------------------------------------------------
// constant
// ---------------------------------------------------------------------------

describe('IndexExpression — constant', () => {
  it('round-trips integer constant', () => {
    const bound: BoundExpression = {
      expressionClass: BoundExpressionClass.BOUND_CONSTANT,
      value: 42,
      returnType: 'INTEGER',
    };
    const idx = boundToIndexExpression(bound, schema);
    expect(idx).toEqual({ type: 'constant', value: 42, returnType: 'INTEGER' });

    const rebound = bindIndexExpression(idx, schema, 0);
    expect(rebound.expressionClass).toBe(BoundExpressionClass.BOUND_CONSTANT);
    expect((rebound as any).value).toBe(42);
    expect(rebound.returnType).toBe('INTEGER');
  });

  it('round-trips string constant', () => {
    const bound: BoundExpression = {
      expressionClass: BoundExpressionClass.BOUND_CONSTANT,
      value: 'hello',
      returnType: 'TEXT',
    };
    const idx = boundToIndexExpression(bound, schema);
    expect(idx).toEqual({ type: 'constant', value: 'hello', returnType: 'TEXT' });
  });

  it('round-trips null constant', () => {
    const bound: BoundExpression = {
      expressionClass: BoundExpressionClass.BOUND_CONSTANT,
      value: null,
      returnType: 'NULL',
    };
    const idx = boundToIndexExpression(bound, schema);
    expect(idx).toEqual({ type: 'constant', value: null, returnType: 'NULL' });
  });

  it('round-trips boolean constant', () => {
    const bound: BoundExpression = {
      expressionClass: BoundExpressionClass.BOUND_CONSTANT,
      value: true,
      returnType: 'BOOLEAN',
    };
    const idx = boundToIndexExpression(bound, schema);
    expect(idx).toEqual({ type: 'constant', value: true, returnType: 'BOOLEAN' });
  });

  it('getIndexColumns for constant is empty', () => {
    const idx: IndexExpression = { type: 'constant', value: 42, returnType: 'INTEGER' };
    expect(getIndexColumns(idx)).toEqual([]);
  });
});

// ---------------------------------------------------------------------------
// mixed / nested
// ---------------------------------------------------------------------------

describe('IndexExpression — nested expressions', () => {
  it('CASE WHEN a > 0 THEN a + b ELSE -a END', () => {
    const addExpr: BoundExpression = {
      expressionClass: BoundExpressionClass.BOUND_OPERATOR,
      operatorType: 'ADD',
      children: [colRef(0), colRef(1)],
      returnType: 'INTEGER',
    };
    const negExpr: BoundExpression = {
      expressionClass: BoundExpressionClass.BOUND_OPERATOR,
      operatorType: 'NEGATE',
      children: [colRef(0)],
      returnType: 'INTEGER',
    };
    const bound: BoundExpression = {
      expressionClass: BoundExpressionClass.BOUND_CASE,
      caseChecks: [{
        when: {
          expressionClass: BoundExpressionClass.BOUND_COMPARISON,
          comparisonType: 'GREATER',
          left: colRef(0),
          right: { expressionClass: BoundExpressionClass.BOUND_CONSTANT, value: 0, returnType: 'INTEGER' },
          returnType: 'BOOLEAN',
        },
        then: addExpr,
      }],
      elseExpr: negExpr,
      returnType: 'INTEGER',
    };
    const idx = boundToIndexExpression(bound, schema);
    expect(idx.type).toBe('case');

    // Check nested structure
    const caseIdx = idx as Extract<IndexExpression, { type: 'case' }>;
    expect(caseIdx.checks[0].then.type).toBe('operator');
    expect(caseIdx.elseExpr!.type).toBe('operator');

    const rebound = bindIndexExpression(idx, schema, 0);
    expect(rebound.expressionClass).toBe(BoundExpressionClass.BOUND_CASE);
  });

  it('operator with constant child: a + 1', () => {
    const bound: BoundExpression = {
      expressionClass: BoundExpressionClass.BOUND_OPERATOR,
      operatorType: 'ADD',
      children: [
        colRef(0),
        { expressionClass: BoundExpressionClass.BOUND_CONSTANT, value: 1, returnType: 'INTEGER' },
      ],
      returnType: 'INTEGER',
    };
    const idx = boundToIndexExpression(bound, schema);
    expect(idx).toEqual({
      type: 'operator',
      operatorType: 'ADD',
      args: [
        { type: 'column', name: 'a', returnType: 'INTEGER' },
        { type: 'constant', value: 1, returnType: 'INTEGER' },
      ],
      returnType: 'INTEGER',
    });
    expect(getIndexColumns(idx)).toEqual(['a']);
  });
});
