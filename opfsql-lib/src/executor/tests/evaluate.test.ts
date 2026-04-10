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
    it('returns integer', async () => {
      expect(await eval_(constant(42))).toBe(42);
    });

    it('returns string', async () => {
      expect(await eval_(constant('abc'))).toBe('abc');
    });

    it('returns null', async () => {
      expect(await eval_(constant(null))).toBeNull();
    });

    it('returns boolean', async () => {
      expect(await eval_(constant(true))).toBe(true);
    });
  });

  describe('column refs', () => {
    it('resolves by binding', async () => {
      expect(await eval_(colRef(0, 0))).toBe(10);
      expect(await eval_(colRef(0, 1))).toBe('hello');
    });

    it('resolves null column', async () => {
      expect(await eval_(colRef(0, 2))).toBeNull();
    });
  });

  // ---------------------------------------------------------------------------
  // Comparisons
  // ---------------------------------------------------------------------------

  describe('comparisons', () => {
    it('EQUAL — true', async () => {
      expect(await eval_(comparison(constant(5), constant(5)))).toBe(true);
    });

    it('EQUAL — false', async () => {
      expect(await eval_(comparison(constant(5), constant(6)))).toBe(false);
    });

    it('EQUAL — null propagation', async () => {
      expect(await eval_(comparison(constant(null), constant(5)))).toBeNull();
    });

    it('LESS', async () => {
      expect(await eval_(comparison(constant(3), constant(5), 'LESS'))).toBe(true);
      expect(await eval_(comparison(constant(5), constant(3), 'LESS'))).toBe(false);
    });

    it('GREATER', async () => {
      expect(await eval_(comparison(constant(5), constant(3), 'GREATER'))).toBe(true);
    });

    it('LESS_EQUAL', async () => {
      expect(await eval_(comparison(constant(5), constant(5), 'LESS_EQUAL'))).toBe(true);
    });

    it('NOT_EQUAL', async () => {
      expect(await eval_(comparison(constant(1), constant(2), 'NOT_EQUAL'))).toBe(true);
      expect(await eval_(comparison(constant(1), constant(1), 'NOT_EQUAL'))).toBe(false);
    });

    it('compares strings', async () => {
      expect(await eval_(comparison(constant('a'), constant('b'), 'LESS'))).toBe(true);
      expect(await eval_(comparison(constant('b'), constant('a'), 'LESS'))).toBe(false);
    });
  });

  // ---------------------------------------------------------------------------
  // Conjunctions
  // ---------------------------------------------------------------------------

  describe('conjunctions', () => {
    it('AND — both true', async () => {
      expect(await eval_(conjunction('AND', constant(true), constant(true)))).toBe(true);
    });

    it('AND — one false', async () => {
      expect(await eval_(conjunction('AND', constant(true), constant(false)))).toBe(false);
    });

    it('AND — false and null → false', async () => {
      expect(await eval_(conjunction('AND', constant(false), constant(null, 'BOOLEAN')))).toBe(false);
    });

    it('AND — true and null → null', async () => {
      expect(await eval_(conjunction('AND', constant(true), constant(null, 'BOOLEAN')))).toBeNull();
    });

    it('OR — one true', async () => {
      expect(await eval_(conjunction('OR', constant(false), constant(true)))).toBe(true);
    });

    it('OR — true and null → true', async () => {
      expect(await eval_(conjunction('OR', constant(true), constant(null, 'BOOLEAN')))).toBe(true);
    });

    it('OR — false and null → null', async () => {
      expect(await eval_(conjunction('OR', constant(false), constant(null, 'BOOLEAN')))).toBeNull();
    });
  });

  // ---------------------------------------------------------------------------
  // Operators
  // ---------------------------------------------------------------------------

  describe('operators', () => {
    it('ADD', async () => {
      expect(await eval_(operator('ADD', [constant(3), constant(4)]))).toBe(7);
    });

    it('SUBTRACT', async () => {
      expect(await eval_(operator('SUBTRACT', [constant(10), constant(3)]))).toBe(7);
    });

    it('MULTIPLY', async () => {
      expect(await eval_(operator('MULTIPLY', [constant(3), constant(4)]))).toBe(12);
    });

    it('DIVIDE', async () => {
      expect(await eval_(operator('DIVIDE', [constant(10), constant(3)]))).toBeCloseTo(3.333, 2);
    });

    it('DIVIDE by zero → null', async () => {
      expect(await eval_(operator('DIVIDE', [constant(10), constant(0)]))).toBeNull();
    });

    it('MOD', async () => {
      expect(await eval_(operator('MOD', [constant(10), constant(3)]))).toBe(1);
    });

    it('MOD by zero → null', async () => {
      expect(await eval_(operator('MOD', [constant(10), constant(0)]))).toBeNull();
    });

    it('NEGATE', async () => {
      expect(await eval_(operator('NEGATE', [constant(5)]))).toBe(-5);
    });

    it('NOT', async () => {
      expect(await eval_(operator('NOT', [constant(true)], 'BOOLEAN'))).toBe(false);
      expect(await eval_(operator('NOT', [constant(false)], 'BOOLEAN'))).toBe(true);
    });

    it('NOT null → null', async () => {
      expect(await eval_(operator('NOT', [constant(null, 'BOOLEAN')], 'BOOLEAN'))).toBeNull();
    });

    it('IS_NULL', async () => {
      expect(await eval_(operator('IS_NULL', [constant(null)], 'BOOLEAN'))).toBe(true);
      expect(await eval_(operator('IS_NULL', [constant(5)], 'BOOLEAN'))).toBe(false);
    });

    it('IS_NOT_NULL', async () => {
      expect(await eval_(operator('IS_NOT_NULL', [constant(null)], 'BOOLEAN'))).toBe(false);
      expect(await eval_(operator('IS_NOT_NULL', [constant(5)], 'BOOLEAN'))).toBe(true);
    });

    it('IN — found', async () => {
      expect(await eval_(operator('IN', [constant(2), constant(1), constant(2), constant(3)], 'BOOLEAN'))).toBe(true);
    });

    it('IN — not found', async () => {
      expect(await eval_(operator('IN', [constant(5), constant(1), constant(2)], 'BOOLEAN'))).toBe(false);
    });

    it('IN — null input → null', async () => {
      expect(await eval_(operator('IN', [constant(null), constant(1)], 'BOOLEAN'))).toBeNull();
    });

    it('NOT_IN', async () => {
      expect(await eval_(operator('NOT_IN', [constant(5), constant(1), constant(2)], 'BOOLEAN'))).toBe(true);
      expect(await eval_(operator('NOT_IN', [constant(1), constant(1), constant(2)], 'BOOLEAN'))).toBe(false);
    });

    it('arithmetic with null → null', async () => {
      expect(await eval_(operator('ADD', [constant(null), constant(3)]))).toBeNull();
      expect(await eval_(operator('ADD', [constant(3), constant(null)]))).toBeNull();
    });
  });

  // ---------------------------------------------------------------------------
  // Between
  // ---------------------------------------------------------------------------

  describe('between', () => {
    it('value in range', async () => {
      expect(await eval_(between(constant(5), constant(1), constant(10)))).toBe(true);
    });

    it('value at boundary', async () => {
      expect(await eval_(between(constant(1), constant(1), constant(10)))).toBe(true);
      expect(await eval_(between(constant(10), constant(1), constant(10)))).toBe(true);
    });

    it('value out of range', async () => {
      expect(await eval_(between(constant(0), constant(1), constant(10)))).toBe(false);
    });

    it('null input → null', async () => {
      expect(await eval_(between(constant(null), constant(1), constant(10)))).toBeNull();
    });
  });

  // ---------------------------------------------------------------------------
  // Functions
  // ---------------------------------------------------------------------------

  describe('functions', () => {
    it('UPPER', async () => {
      expect(await eval_(fnCall('UPPER', [constant('hello')]))).toBe('HELLO');
    });

    it('LOWER', async () => {
      expect(await eval_(fnCall('LOWER', [constant('HELLO')]))).toBe('hello');
    });

    it('LENGTH', async () => {
      expect(await eval_(fnCall('LENGTH', [constant('abc')], 'INTEGER'))).toBe(3);
    });

    it('TRIM', async () => {
      expect(await eval_(fnCall('TRIM', [constant('  hi  ')]))).toBe('hi');
    });

    it('SUBSTR', async () => {
      expect(await eval_(fnCall('SUBSTR', [constant('hello'), constant(2), constant(3)]))).toBe('ell');
    });

    it('REPLACE', async () => {
      expect(await eval_(fnCall('REPLACE', [constant('hello'), constant('l'), constant('r')]))).toBe('herro');
    });

    it('CONCAT', async () => {
      expect(await eval_(fnCall('CONCAT', [constant('a'), constant('b'), constant('c')]))).toBe('abc');
    });

    it('ABS', async () => {
      expect(await eval_(fnCall('ABS', [constant(-5)], 'INTEGER'))).toBe(5);
    });

    it('ROUND', async () => {
      expect(await eval_(fnCall('ROUND', [constant(3.7)], 'INTEGER'))).toBe(4);
    });

    it('FLOOR', async () => {
      expect(await eval_(fnCall('FLOOR', [constant(3.7)], 'INTEGER'))).toBe(3);
    });

    it('CEIL', async () => {
      expect(await eval_(fnCall('CEIL', [constant(3.2)], 'INTEGER'))).toBe(4);
    });

    it('COALESCE — returns first non-null', async () => {
      expect(await eval_(fnCall('COALESCE', [constant(null), constant(null), constant(5)], 'INTEGER'))).toBe(5);
    });

    it('COALESCE — all null', async () => {
      expect(await eval_(fnCall('COALESCE', [constant(null), constant(null)], 'INTEGER'))).toBeNull();
    });

    it('NULLIF — equal → null', async () => {
      expect(await eval_(fnCall('NULLIF', [constant(5), constant(5)], 'INTEGER'))).toBeNull();
    });

    it('NULLIF — not equal → first', async () => {
      expect(await eval_(fnCall('NULLIF', [constant(5), constant(3)], 'INTEGER'))).toBe(5);
    });

    it('LIKE — match', async () => {
      expect(await eval_(fnCall('LIKE', [constant('hello'), constant('hel%')], 'BOOLEAN'))).toBe(true);
    });

    it('LIKE — no match', async () => {
      expect(await eval_(fnCall('LIKE', [constant('hello'), constant('xyz%')], 'BOOLEAN'))).toBe(false);
    });

    it('LIKE — underscore wildcard', async () => {
      expect(await eval_(fnCall('LIKE', [constant('hello'), constant('h_llo')], 'BOOLEAN'))).toBe(true);
    });

    it('TYPEOF', async () => {
      expect(await eval_(fnCall('TYPEOF', [constant(42)]))).toBe('number');
      expect(await eval_(fnCall('TYPEOF', [constant('abc')]))).toBe('string');
      expect(await eval_(fnCall('TYPEOF', [constant(null)]))).toBe('null');
      expect(await eval_(fnCall('TYPEOF', [constant(true)]))).toBe('boolean');
    });

    it('function with null input → null (except COALESCE/TYPEOF)', async () => {
      expect(await eval_(fnCall('UPPER', [constant(null)]))).toBeNull();
      expect(await eval_(fnCall('ABS', [constant(null)], 'INTEGER'))).toBeNull();
    });
  });

  // ---------------------------------------------------------------------------
  // CASE
  // ---------------------------------------------------------------------------

  describe('case', () => {
    it('matches first truthy WHEN', async () => {
      const expr = caseExpr(
        [
          { when: constant(false), then: constant(1) },
          { when: constant(true), then: constant(2) },
        ],
        constant(99),
      );
      expect(await eval_(expr)).toBe(2);
    });

    it('falls through to ELSE', async () => {
      const expr = caseExpr(
        [{ when: constant(false), then: constant(1) }],
        constant(99),
      );
      expect(await eval_(expr)).toBe(99);
    });

    it('no ELSE → null', async () => {
      const expr = caseExpr([{ when: constant(false), then: constant(1) }]);
      expect(await eval_(expr)).toBeNull();
    });
  });

  // ---------------------------------------------------------------------------
  // CAST
  // ---------------------------------------------------------------------------

  describe('cast', () => {
    it('TEXT → INTEGER', async () => {
      expect(await eval_(cast(constant('42'), 'INTEGER'))).toBe(42);
    });

    it('INTEGER → TEXT', async () => {
      expect(await eval_(cast(constant(42), 'TEXT'))).toBe('42');
    });

    it('REAL → INTEGER (truncate)', async () => {
      expect(await eval_(cast(constant(3.9), 'INTEGER'))).toBe(3);
    });

    it('TEXT → BOOLEAN', async () => {
      expect(await eval_(cast(constant('true'), 'BOOLEAN'))).toBe(true);
      expect(await eval_(cast(constant('false'), 'BOOLEAN'))).toBe(false);
    });

    it('NULL cast → null', async () => {
      expect(await eval_(cast(constant(null), 'INTEGER'))).toBeNull();
    });
  });
});

// ---------------------------------------------------------------------------
// Subquery — EXISTS early termination
// ---------------------------------------------------------------------------

import { evalSubquery } from '../evaluate/subquery.js';
import { BoundExpressionClass } from '../../binder/types.js';
import type { LogicalOperator } from '../../binder/types.js';
import type { EvalContext } from '../evaluate/context.js';

const dummySubplan = {} as LogicalOperator;

function mockCtx(): EvalContext & { calls: Array<{ limit?: number }> } {
  const calls: Array<{ limit?: number }> = [];
  return {
    calls,
    executeSubplan: async (_plan, _ot, _or, limit) => {
      calls.push({ limit });
      return limit !== undefined ? [[1]] : [[1], [2], [3]];
    },
  };
}

function subqueryExpr(type: 'EXISTS' | 'NOT_EXISTS' | 'SCALAR') {
  return {
    expressionClass: BoundExpressionClass.BOUND_SUBQUERY,
    subqueryType: type,
    subplan: dummySubplan,
    returnType: 'BOOLEAN' as const,
  };
}

describe('evalSubquery — EXISTS early termination', () => {
  it('EXISTS passes limit=1 to executeSubplan', async () => {
    const ctx = mockCtx();
    await evalSubquery(subqueryExpr('EXISTS'), [], () => -1, ctx);
    expect(ctx.calls).toHaveLength(1);
    expect(ctx.calls[0].limit).toBe(1);
  });

  it('NOT_EXISTS passes limit=1 to executeSubplan', async () => {
    const ctx = mockCtx();
    await evalSubquery(subqueryExpr('NOT_EXISTS'), [], () => -1, ctx);
    expect(ctx.calls).toHaveLength(1);
    expect(ctx.calls[0].limit).toBe(1);
  });

  it('SCALAR does NOT pass limit', async () => {
    const calls: Array<{ limit?: number }> = [];
    const ctx: EvalContext = {
      executeSubplan: async (_plan, _ot, _or, limit) => {
        calls.push({ limit });
        return [[42]]; // single row for SCALAR
      },
    };
    await evalSubquery(subqueryExpr('SCALAR'), [], () => -1, ctx);
    expect(calls).toHaveLength(1);
    expect(calls[0].limit).toBeUndefined();
  });

  it('EXISTS returns true when rows exist', async () => {
    const ctx = mockCtx();
    const result = await evalSubquery(subqueryExpr('EXISTS'), [], () => -1, ctx);
    expect(result).toBe(true);
  });

  it('EXISTS returns false when no rows', async () => {
    const ctx: EvalContext = {
      executeSubplan: async () => [],
    };
    const result = await evalSubquery(subqueryExpr('EXISTS'), [], () => -1, ctx);
    expect(result).toBe(false);
  });

  it('NOT_EXISTS returns false when rows exist', async () => {
    const ctx = mockCtx();
    const result = await evalSubquery(subqueryExpr('NOT_EXISTS'), [], () => -1, ctx);
    expect(result).toBe(false);
  });

  it('NOT_EXISTS returns true when no rows', async () => {
    const ctx: EvalContext = {
      executeSubplan: async () => [],
    };
    const result = await evalSubquery(subqueryExpr('NOT_EXISTS'), [], () => -1, ctx);
    expect(result).toBe(true);
  });
});
