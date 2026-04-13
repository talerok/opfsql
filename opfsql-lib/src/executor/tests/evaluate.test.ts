import { describe, it, expect } from 'vitest';
import { evaluateExpression } from '../evaluate/index.js';
import { buildResolver } from '../resolve.js';
import {
  colRef,
  constant,
  comparison,
  conjunction,
  operator,
  between,
  fnCall,
  cast,
  caseExpr,
  layout,
  noopCtx,
} from './helpers.js';
import type { Tuple } from '../types.js';
import type { Resolver } from '../resolve.js';

// Common setup: tuple [10, 'hello', null, true] with layout t0.c0..c3
const testLayout = layout([0, 0], [0, 1], [0, 2], [0, 3]);
const resolver = buildResolver(testLayout);
const tuple: Tuple = [10, 'hello', null, true];

function eval_(expr: Parameters<typeof evaluateExpression>[0], t = tuple, r = resolver) {
  return evaluateExpression(expr, t, r, noopCtx);
}

// ---------------------------------------------------------------------------
// Constants and column refs
// ---------------------------------------------------------------------------

describe('evaluateExpression', () => {
  describe('constants', () => {
    it('returns integer', () => {
      expect(eval_(constant(42))).toBe(42);
    });

    it('returns string', () => {
      expect(eval_(constant('abc'))).toBe('abc');
    });

    it('returns null', () => {
      expect(eval_(constant(null))).toBeNull();
    });

    it('returns boolean', () => {
      expect(eval_(constant(true))).toBe(true);
    });
  });

  describe('column refs', () => {
    it('resolves by binding', () => {
      expect(eval_(colRef(0, 0))).toBe(10);
      expect(eval_(colRef(0, 1))).toBe('hello');
    });

    it('resolves null column', () => {
      expect(eval_(colRef(0, 2))).toBeNull();
    });
  });

  // ---------------------------------------------------------------------------
  // Comparisons
  // ---------------------------------------------------------------------------

  describe('comparisons', () => {
    it('EQUAL — true', () => {
      expect(eval_(comparison(constant(5), constant(5)))).toBe(true);
    });

    it('EQUAL — false', () => {
      expect(eval_(comparison(constant(5), constant(6)))).toBe(false);
    });

    it('EQUAL — null propagation', () => {
      expect(eval_(comparison(constant(null), constant(5)))).toBeNull();
    });

    it('LESS', () => {
      expect(eval_(comparison(constant(3), constant(5), 'LESS'))).toBe(true);
      expect(eval_(comparison(constant(5), constant(3), 'LESS'))).toBe(false);
    });

    it('GREATER', () => {
      expect(eval_(comparison(constant(5), constant(3), 'GREATER'))).toBe(true);
    });

    it('LESS_EQUAL', () => {
      expect(eval_(comparison(constant(5), constant(5), 'LESS_EQUAL'))).toBe(true);
    });

    it('NOT_EQUAL', () => {
      expect(eval_(comparison(constant(1), constant(2), 'NOT_EQUAL'))).toBe(true);
      expect(eval_(comparison(constant(1), constant(1), 'NOT_EQUAL'))).toBe(false);
    });

    it('compares strings', () => {
      expect(eval_(comparison(constant('a'), constant('b'), 'LESS'))).toBe(true);
      expect(eval_(comparison(constant('b'), constant('a'), 'LESS'))).toBe(false);
    });
  });

  // ---------------------------------------------------------------------------
  // Conjunctions
  // ---------------------------------------------------------------------------

  describe('conjunctions', () => {
    it('AND — both true', () => {
      expect(eval_(conjunction('AND', constant(true), constant(true)))).toBe(true);
    });

    it('AND — one false', () => {
      expect(eval_(conjunction('AND', constant(true), constant(false)))).toBe(false);
    });

    it('AND — false and null → false', () => {
      expect(eval_(conjunction('AND', constant(false), constant(null, 'BOOLEAN')))).toBe(false);
    });

    it('AND — true and null → null', () => {
      expect(eval_(conjunction('AND', constant(true), constant(null, 'BOOLEAN')))).toBeNull();
    });

    it('OR — one true', () => {
      expect(eval_(conjunction('OR', constant(false), constant(true)))).toBe(true);
    });

    it('OR — true and null → true', () => {
      expect(eval_(conjunction('OR', constant(true), constant(null, 'BOOLEAN')))).toBe(true);
    });

    it('OR — false and null → null', () => {
      expect(eval_(conjunction('OR', constant(false), constant(null, 'BOOLEAN')))).toBeNull();
    });
  });

  // ---------------------------------------------------------------------------
  // Operators
  // ---------------------------------------------------------------------------

  describe('operators', () => {
    it('ADD', () => {
      expect(eval_(operator('ADD', [constant(3), constant(4)]))).toBe(7);
    });

    it('SUBTRACT', () => {
      expect(eval_(operator('SUBTRACT', [constant(10), constant(3)]))).toBe(7);
    });

    it('MULTIPLY', () => {
      expect(eval_(operator('MULTIPLY', [constant(3), constant(4)]))).toBe(12);
    });

    it('DIVIDE', () => {
      expect(eval_(operator('DIVIDE', [constant(10), constant(3)]))).toBeCloseTo(3.333, 2);
    });

    it('DIVIDE by zero → null', () => {
      expect(eval_(operator('DIVIDE', [constant(10), constant(0)]))).toBeNull();
    });

    it('MOD', () => {
      expect(eval_(operator('MOD', [constant(10), constant(3)]))).toBe(1);
    });

    it('MOD by zero → null', () => {
      expect(eval_(operator('MOD', [constant(10), constant(0)]))).toBeNull();
    });

    it('NEGATE', () => {
      expect(eval_(operator('NEGATE', [constant(5)]))).toBe(-5);
    });

    it('NOT', () => {
      expect(eval_(operator('NOT', [constant(true)], 'BOOLEAN'))).toBe(false);
      expect(eval_(operator('NOT', [constant(false)], 'BOOLEAN'))).toBe(true);
    });

    it('NOT null → null', () => {
      expect(eval_(operator('NOT', [constant(null, 'BOOLEAN')], 'BOOLEAN'))).toBeNull();
    });

    it('IS_NULL', () => {
      expect(eval_(operator('IS_NULL', [constant(null)], 'BOOLEAN'))).toBe(true);
      expect(eval_(operator('IS_NULL', [constant(5)], 'BOOLEAN'))).toBe(false);
    });

    it('IS_NOT_NULL', () => {
      expect(eval_(operator('IS_NOT_NULL', [constant(null)], 'BOOLEAN'))).toBe(false);
      expect(eval_(operator('IS_NOT_NULL', [constant(5)], 'BOOLEAN'))).toBe(true);
    });

    it('IN — found', () => {
      expect(eval_(operator('IN', [constant(2), constant(1), constant(2), constant(3)], 'BOOLEAN'))).toBe(true);
    });

    it('IN — not found', () => {
      expect(eval_(operator('IN', [constant(5), constant(1), constant(2)], 'BOOLEAN'))).toBe(false);
    });

    it('IN — null input → null', () => {
      expect(eval_(operator('IN', [constant(null), constant(1)], 'BOOLEAN'))).toBeNull();
    });

    it('NOT_IN', () => {
      expect(eval_(operator('NOT_IN', [constant(5), constant(1), constant(2)], 'BOOLEAN'))).toBe(true);
      expect(eval_(operator('NOT_IN', [constant(1), constant(1), constant(2)], 'BOOLEAN'))).toBe(false);
    });

    it('arithmetic with null → null', () => {
      expect(eval_(operator('ADD', [constant(null), constant(3)]))).toBeNull();
      expect(eval_(operator('ADD', [constant(3), constant(null)]))).toBeNull();
    });

    it('CONCAT strings', () => {
      expect(eval_(operator('CONCAT', [constant('hello'), constant(' world')], 'TEXT'))).toBe('hello world');
    });

    it('CONCAT coerces numbers to strings', () => {
      expect(eval_(operator('CONCAT', [constant(123), constant('abc')], 'TEXT'))).toBe('123abc');
    });

    it('CONCAT with null → null', () => {
      expect(eval_(operator('CONCAT', [constant(null), constant('x')], 'TEXT'))).toBeNull();
      expect(eval_(operator('CONCAT', [constant('x'), constant(null)], 'TEXT'))).toBeNull();
    });
  });

  // ---------------------------------------------------------------------------
  // Between
  // ---------------------------------------------------------------------------

  describe('between', () => {
    it('value in range', () => {
      expect(eval_(between(constant(5), constant(1), constant(10)))).toBe(true);
    });

    it('value at boundary', () => {
      expect(eval_(between(constant(1), constant(1), constant(10)))).toBe(true);
      expect(eval_(between(constant(10), constant(1), constant(10)))).toBe(true);
    });

    it('value out of range', () => {
      expect(eval_(between(constant(0), constant(1), constant(10)))).toBe(false);
    });

    it('null input → null', () => {
      expect(eval_(between(constant(null), constant(1), constant(10)))).toBeNull();
    });
  });

  // ---------------------------------------------------------------------------
  // Functions
  // ---------------------------------------------------------------------------

  describe('functions', () => {
    it('UPPER', () => {
      expect(eval_(fnCall('UPPER', [constant('hello')]))).toBe('HELLO');
    });

    it('LOWER', () => {
      expect(eval_(fnCall('LOWER', [constant('HELLO')]))).toBe('hello');
    });

    it('LENGTH', () => {
      expect(eval_(fnCall('LENGTH', [constant('abc')], 'INTEGER'))).toBe(3);
    });

    it('TRIM', () => {
      expect(eval_(fnCall('TRIM', [constant('  hi  ')]))).toBe('hi');
    });

    it('SUBSTR', () => {
      expect(eval_(fnCall('SUBSTR', [constant('hello'), constant(2), constant(3)]))).toBe('ell');
    });

    it('REPLACE', () => {
      expect(eval_(fnCall('REPLACE', [constant('hello'), constant('l'), constant('r')]))).toBe('herro');
    });

    it('CONCAT', () => {
      expect(eval_(fnCall('CONCAT', [constant('a'), constant('b'), constant('c')]))).toBe('abc');
    });

    it('ABS', () => {
      expect(eval_(fnCall('ABS', [constant(-5)], 'INTEGER'))).toBe(5);
    });

    it('ROUND', () => {
      expect(eval_(fnCall('ROUND', [constant(3.7)], 'INTEGER'))).toBe(4);
    });

    it('FLOOR', () => {
      expect(eval_(fnCall('FLOOR', [constant(3.7)], 'INTEGER'))).toBe(3);
    });

    it('CEIL', () => {
      expect(eval_(fnCall('CEIL', [constant(3.2)], 'INTEGER'))).toBe(4);
    });

    it('COALESCE — returns first non-null', () => {
      expect(eval_(fnCall('COALESCE', [constant(null), constant(null), constant(5)], 'INTEGER'))).toBe(5);
    });

    it('COALESCE — all null', () => {
      expect(eval_(fnCall('COALESCE', [constant(null), constant(null)], 'INTEGER'))).toBeNull();
    });

    it('NULLIF — equal → null', () => {
      expect(eval_(fnCall('NULLIF', [constant(5), constant(5)], 'INTEGER'))).toBeNull();
    });

    it('NULLIF — not equal → first', () => {
      expect(eval_(fnCall('NULLIF', [constant(5), constant(3)], 'INTEGER'))).toBe(5);
    });

    it('LIKE — match', () => {
      expect(eval_(fnCall('LIKE', [constant('hello'), constant('hel%')], 'BOOLEAN'))).toBe(true);
    });

    it('LIKE — no match', () => {
      expect(eval_(fnCall('LIKE', [constant('hello'), constant('xyz%')], 'BOOLEAN'))).toBe(false);
    });

    it('LIKE — underscore wildcard', () => {
      expect(eval_(fnCall('LIKE', [constant('hello'), constant('h_llo')], 'BOOLEAN'))).toBe(true);
    });

    it('TYPEOF', () => {
      expect(eval_(fnCall('TYPEOF', [constant(42)]))).toBe('number');
      expect(eval_(fnCall('TYPEOF', [constant('abc')]))).toBe('string');
      expect(eval_(fnCall('TYPEOF', [constant(null)]))).toBe('null');
      expect(eval_(fnCall('TYPEOF', [constant(true)]))).toBe('boolean');
    });

    it('function with null input → null (except COALESCE/TYPEOF)', () => {
      expect(eval_(fnCall('UPPER', [constant(null)]))).toBeNull();
      expect(eval_(fnCall('ABS', [constant(null)], 'INTEGER'))).toBeNull();
    });

    it('NOT_LIKE — match returns false', () => {
      expect(eval_(fnCall('NOT_LIKE', [constant('hello'), constant('hel%')], 'BOOLEAN'))).toBe(false);
    });

    it('NOT_LIKE — no match returns true', () => {
      expect(eval_(fnCall('NOT_LIKE', [constant('hello'), constant('xyz%')], 'BOOLEAN'))).toBe(true);
    });

    it('NOT_LIKE — null input → null', () => {
      expect(eval_(fnCall('NOT_LIKE', [constant(null), constant('%')], 'BOOLEAN'))).toBeNull();
    });

    it('LTRIM', () => {
      expect(eval_(fnCall('LTRIM', [constant('  hi  ')]))).toBe('hi  ');
    });

    it('RTRIM', () => {
      expect(eval_(fnCall('RTRIM', [constant('  hi  ')]))).toBe('  hi');
    });

    it('SUBSTR without length → to end', () => {
      expect(eval_(fnCall('SUBSTR', [constant('hello'), constant(3)]))).toBe('llo');
    });

    it('ROUND with precision', () => {
      expect(eval_(fnCall('ROUND', [constant(3.14159), constant(2)], 'REAL'))).toBeCloseTo(3.14, 2);
    });

    it('COALESCE — first non-null is returned', () => {
      expect(eval_(fnCall('COALESCE', [constant(null), constant(10), constant(20)], 'INTEGER'))).toBe(10);
    });
  });

  // ---------------------------------------------------------------------------
  // CASE
  // ---------------------------------------------------------------------------

  describe('case', () => {
    it('matches first truthy WHEN', () => {
      const expr = caseExpr(
        [
          { when: constant(false), then: constant(1) },
          { when: constant(true),  then: constant(2) },
        ],
        constant(99),
      );
      expect(eval_(expr)).toBe(2);
    });

    it('falls through to ELSE', () => {
      const expr = caseExpr(
        [{ when: constant(false), then: constant(1) }],
        constant(99),
      );
      expect(eval_(expr)).toBe(99);
    });

    it('no ELSE → null', () => {
      const expr = caseExpr([{ when: constant(false), then: constant(1) }]);
      expect(eval_(expr)).toBeNull();
    });
  });

  // ---------------------------------------------------------------------------
  // CAST
  // ---------------------------------------------------------------------------

  describe('cast', () => {
    it('TEXT → INTEGER', () => {
      expect(eval_(cast(constant('42'), 'INTEGER'))).toBe(42);
    });

    it('INTEGER → TEXT', () => {
      expect(eval_(cast(constant(42), 'TEXT'))).toBe('42');
    });

    it('REAL → INTEGER (truncate)', () => {
      expect(eval_(cast(constant(3.9), 'INTEGER'))).toBe(3);
    });

    it('TEXT → BOOLEAN', () => {
      expect(eval_(cast(constant('true'), 'BOOLEAN'))).toBe(true);
      expect(eval_(cast(constant('false'), 'BOOLEAN'))).toBe(false);
    });

    it('NULL cast → null', () => {
      expect(eval_(cast(constant(null), 'INTEGER'))).toBeNull();
    });

    it('BOOLEAN → INTEGER', () => {
      expect(eval_(cast(constant(true), 'INTEGER'))).toBe(1);
      expect(eval_(cast(constant(false), 'INTEGER'))).toBe(0);
    });

    it('INTEGER → BOOLEAN', () => {
      expect(eval_(cast(constant(1), 'BOOLEAN'))).toBe(true);
      expect(eval_(cast(constant(0), 'BOOLEAN'))).toBe(false);
    });

    it('BOOLEAN → TEXT', () => {
      expect(eval_(cast(constant(true), 'TEXT'))).toBe('true');
      expect(eval_(cast(constant(false), 'TEXT'))).toBe('false');
    });

    it('INTEGER → REAL preserves value', () => {
      expect(eval_(cast(constant(42), 'REAL'))).toBe(42);
    });

    it('TEXT → REAL', () => {
      expect(eval_(cast(constant('3.14'), 'REAL'))).toBeCloseTo(3.14, 2);
    });
  });

  // ---------------------------------------------------------------------------
  // GREATER_EQUAL comparison
  // ---------------------------------------------------------------------------

  describe('additional comparisons', () => {
    it('GREATER_EQUAL', () => {
      expect(eval_(comparison(constant(5), constant(5), 'GREATER_EQUAL'))).toBe(true);
      expect(eval_(comparison(constant(6), constant(5), 'GREATER_EQUAL'))).toBe(true);
      expect(eval_(comparison(constant(4), constant(5), 'GREATER_EQUAL'))).toBe(false);
    });

    it('comparison with both NULL → null', () => {
      expect(eval_(comparison(constant(null), constant(null)))).toBeNull();
    });
  });
});

// ---------------------------------------------------------------------------
// Subquery — EXISTS early termination
// ---------------------------------------------------------------------------

import { evalSubquery } from '../evaluate/subquery.js';
import { BoundExpressionClass } from '../../binder/types.js';
import type { LogicalOperator } from '../../binder/types.js';
import type { SyncEvalContext } from '../evaluate/context.js';

const dummySubplan = {} as LogicalOperator;

function mockCtx(): SyncEvalContext & { calls: Array<{ limit?: number }> } {
  const calls: Array<{ limit?: number }> = [];
  return {
    calls,
    executeSubplan: (_plan, _ot, _or, limit) => {
      calls.push({ limit });
      return limit !== undefined ? [[1]] : [[1], [2], [3]];
    },
  };
}

function subqueryExpr(type: 'EXISTS' | 'NOT_EXISTS' | 'SCALAR') {
  return {
    expressionClass: BoundExpressionClass.BOUND_SUBQUERY as const,
    subqueryType: type,
    subplan: dummySubplan,
    returnType: 'BOOLEAN' as const,
  };
}

describe('evalSubquery — EXISTS early termination', () => {
  it('EXISTS passes limit=1 to executeSubplan', () => {
    const ctx = mockCtx();
    evalSubquery(subqueryExpr('EXISTS'), [], () => -1, ctx);
    expect(ctx.calls).toHaveLength(1);
    expect(ctx.calls[0].limit).toBe(1);
  });

  it('NOT_EXISTS passes limit=1 to executeSubplan', () => {
    const ctx = mockCtx();
    evalSubquery(subqueryExpr('NOT_EXISTS'), [], () => -1, ctx);
    expect(ctx.calls).toHaveLength(1);
    expect(ctx.calls[0].limit).toBe(1);
  });

  it('SCALAR does NOT pass limit', () => {
    const calls: Array<{ limit?: number }> = [];
    const ctx: SyncEvalContext = {
      executeSubplan: (_plan, _ot, _or, limit) => {
        calls.push({ limit });
        return [[42]];
      },
    };
    evalSubquery(subqueryExpr('SCALAR'), [], () => -1, ctx);
    expect(calls).toHaveLength(1);
    expect(calls[0].limit).toBeUndefined();
  });

  it('EXISTS returns true when rows exist', () => {
    const ctx = mockCtx();
    const result = evalSubquery(subqueryExpr('EXISTS'), [], () => -1, ctx);
    expect(result).toBe(true);
  });

  it('EXISTS returns false when no rows', () => {
    const ctx: SyncEvalContext = { executeSubplan: () => [] };
    const result = evalSubquery(subqueryExpr('EXISTS'), [], () => -1, ctx);
    expect(result).toBe(false);
  });

  it('NOT_EXISTS returns false when rows exist', () => {
    const ctx = mockCtx();
    const result = evalSubquery(subqueryExpr('NOT_EXISTS'), [], () => -1, ctx);
    expect(result).toBe(false);
  });

  it('NOT_EXISTS returns true when no rows', () => {
    const ctx: SyncEvalContext = { executeSubplan: () => [] };
    const result = evalSubquery(subqueryExpr('NOT_EXISTS'), [], () => -1, ctx);
    expect(result).toBe(true);
  });

  it('SCALAR returns null when no rows', () => {
    const ctx: SyncEvalContext = { executeSubplan: () => [] };
    const result = evalSubquery(subqueryExpr('SCALAR'), [], () => -1, ctx);
    expect(result).toBeNull();
  });

  it('SCALAR returns single value', () => {
    const ctx: SyncEvalContext = { executeSubplan: () => [[42]] };
    const result = evalSubquery(subqueryExpr('SCALAR'), [], () => -1, ctx);
    expect(result).toBe(42);
  });

  it('SCALAR throws on multiple rows', () => {
    const ctx: SyncEvalContext = { executeSubplan: () => [[1], [2]] };
    expect(() =>
      evalSubquery(subqueryExpr('SCALAR'), [], () => -1, ctx),
    ).toThrow();
  });
});
