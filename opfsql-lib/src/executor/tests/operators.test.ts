import { describe, it, expect } from 'vitest';
import { LogicalOperatorType } from '../../binder/types.js';
import type {
  LogicalProjection,
  LogicalComparisonJoin,
  LogicalOrderBy,
  LogicalLimit,
  LogicalAggregate,
  BoundAggregateExpression,
  BoundOrderByNode,
} from '../../binder/types.js';
import { BoundExpressionClass } from '../../binder/types.js';
import { PhysicalFilter } from '../operators/filter.js';
import { PhysicalProjection } from '../operators/projection.js';
import { PhysicalHashJoin, PhysicalNestedLoopJoin } from '../operators/join.js';
import { PhysicalHashAggregate } from '../operators/aggregate.js';
import { PhysicalSort } from '../operators/sort.js';
import { PhysicalLimit as PhysicalLimitOp } from '../operators/limit.js';
import { PhysicalDistinct, PhysicalUnion } from '../operators/set.js';
import { PhysicalMaterialize, PhysicalCTEScan } from '../operators/cte.js';
import { drainOperator } from '../operators/utils.js';
import type { CTECacheEntry } from '../types.js';
import {
  MockOperator,
  colRef,
  constant,
  comparison,
  operator,
  layout,
  noopCtx,
} from './helpers.js';

// ---------------------------------------------------------------------------
// Filter
// ---------------------------------------------------------------------------

describe('PhysicalFilter', () => {
  it('keeps matching tuples', () => {
    const child = new MockOperator(
      [[[1, 'a'], [2, 'b'], [3, 'c']]],
      layout([0, 0], [0, 1]),
    );
    const cond = comparison(colRef(0, 0), constant(1), 'GREATER');
    const filter = new PhysicalFilter(child, cond, noopCtx);
    const result = drainOperator(filter);
    expect(result).toEqual([[2, 'b'], [3, 'c']]);
  });

  it('returns empty when all filtered out', () => {
    const child = new MockOperator(
      [[[1, 'a']]],
      layout([0, 0], [0, 1]),
    );
    const cond = comparison(colRef(0, 0), constant(100), 'GREATER');
    const filter = new PhysicalFilter(child, cond, noopCtx);
    const result = drainOperator(filter);
    expect(result).toEqual([]);
  });

  it('null condition filters tuple out', () => {
    const child = new MockOperator(
      [[[null, 'a']]],
      layout([0, 0], [0, 1]),
    );
    const cond = comparison(colRef(0, 0), constant(1), 'EQUAL');
    const filter = new PhysicalFilter(child, cond, noopCtx);
    const result = drainOperator(filter);
    expect(result).toEqual([]);
  });
});

// ---------------------------------------------------------------------------
// Projection
// ---------------------------------------------------------------------------

describe('PhysicalProjection', () => {
  it('projects expressions', () => {
    const child = new MockOperator(
      [[[10, 'hello']]],
      layout([0, 0], [0, 1]),
    );
    const op = {
      type: LogicalOperatorType.LOGICAL_PROJECTION,
      expressions: [
        colRef(0, 1, 'name', 'TEXT'),
        operator('ADD', [colRef(0, 0), constant(5)]),
      ],
      getColumnBindings: () => layout([1, 0], [1, 1]),
    } as unknown as LogicalProjection;

    const proj = new PhysicalProjection(child, op, noopCtx);
    const result = drainOperator(proj);
    expect(result).toEqual([['hello', 15]]);
  });

  it('handles constant projection', () => {
    const child = new MockOperator(
      [[[1], [2]]],
      layout([0, 0]),
    );
    const op = {
      type: LogicalOperatorType.LOGICAL_PROJECTION,
      expressions: [constant(42)],
      getColumnBindings: () => layout([1, 0]),
    } as unknown as LogicalProjection;

    const proj = new PhysicalProjection(child, op, noopCtx);
    const result = drainOperator(proj);
    expect(result).toEqual([[42], [42]]);
  });
});

// ---------------------------------------------------------------------------
// Hash Join
// ---------------------------------------------------------------------------

describe('PhysicalHashJoin', () => {
  function makeJoinOp(joinType: 'INNER' | 'LEFT'): LogicalComparisonJoin {
    return {
      type: LogicalOperatorType.LOGICAL_COMPARISON_JOIN,
      joinType,
      conditions: [
        {
          left: colRef(0, 0),
          right: colRef(1, 0),
          comparisonType: 'EQUAL',
        },
      ],
    } as unknown as LogicalComparisonJoin;
  }

  it('INNER JOIN — matching rows only', () => {
    const probe = new MockOperator([[[1, 'a'], [2, 'b'], [3, 'c']]], layout([0, 0], [0, 1]));
    const build = new MockOperator([[[1, 'x'], [2, 'y']]], layout([1, 0], [1, 1]));
    const join = new PhysicalHashJoin(probe, build, makeJoinOp('INNER'), noopCtx);
    const result = drainOperator(join);
    expect(result).toEqual([
      [1, 'a', 1, 'x'],
      [2, 'b', 2, 'y'],
    ]);
  });

  it('LEFT JOIN — unmatched probe gets nulls', () => {
    const probe = new MockOperator([[[1, 'a'], [2, 'b'], [3, 'c']]], layout([0, 0], [0, 1]));
    const build = new MockOperator([[[1, 'x']]], layout([1, 0], [1, 1]));
    const join = new PhysicalHashJoin(probe, build, makeJoinOp('LEFT'), noopCtx);
    const result = drainOperator(join);
    expect(result).toEqual([
      [1, 'a', 1, 'x'],
      [2, 'b', null, null],
      [3, 'c', null, null],
    ]);
  });

  it('NULL join key — never matches', () => {
    const probe = new MockOperator([[[null, 'a']]], layout([0, 0], [0, 1]));
    const build = new MockOperator([[[null, 'x']]], layout([1, 0], [1, 1]));
    const join = new PhysicalHashJoin(probe, build, makeJoinOp('INNER'), noopCtx);
    const result = drainOperator(join);
    expect(result).toEqual([]);
  });

  it('empty build side', () => {
    const probe = new MockOperator([[[1, 'a']]], layout([0, 0], [0, 1]));
    const build = new MockOperator([], layout([1, 0], [1, 1]));
    const join = new PhysicalHashJoin(probe, build, makeJoinOp('INNER'), noopCtx);
    const result = drainOperator(join);
    expect(result).toEqual([]);
  });
});

// ---------------------------------------------------------------------------
// Nested Loop Join (CROSS)
// ---------------------------------------------------------------------------

describe('PhysicalNestedLoopJoin', () => {
  it('produces cartesian product', () => {
    const left = new MockOperator([[[1], [2]]], layout([0, 0]));
    const right = new MockOperator([[['a'], ['b']]], layout([1, 0]));
    const join = new PhysicalNestedLoopJoin(left, right);
    const result = drainOperator(join);
    expect(result).toEqual([
      [1, 'a'],
      [1, 'b'],
      [2, 'a'],
      [2, 'b'],
    ]);
  });

  it('empty right side → no output', () => {
    const left = new MockOperator([[[1]]], layout([0, 0]));
    const right = new MockOperator([], layout([1, 0]));
    const join = new PhysicalNestedLoopJoin(left, right);
    const result = drainOperator(join);
    expect(result).toEqual([]);
  });
});

// ---------------------------------------------------------------------------
// Aggregate
// ---------------------------------------------------------------------------

describe('PhysicalHashAggregate', () => {
  function makeAgg(
    functionName: string,
    childExpr = colRef(0, 0),
    isStar = false,
    distinct = false,
    aggregateIndex = 0,
  ): BoundAggregateExpression {
    return {
      expressionClass: BoundExpressionClass.BOUND_AGGREGATE,
      functionName: functionName as any,
      children: isStar ? [] : [childExpr],
      distinct,
      isStar,
      aggregateIndex,
      returnType: 'INTEGER',
    };
  }

  function makeAggOp(
    expressions: BoundAggregateExpression[],
    groups: any[] = [],
    groupIndex = 0,
    aggregateIndex = 100,
  ) {
    const bindings = [
      ...groups.map((_: any, i: number) => ({ tableIndex: groupIndex, columnIndex: i })),
      ...expressions.map((_, i) => ({ tableIndex: aggregateIndex, columnIndex: i })),
    ];
    return {
      type: LogicalOperatorType.LOGICAL_AGGREGATE_AND_GROUP_BY,
      groupIndex,
      aggregateIndex,
      expressions,
      groups,
      havingExpression: null,
      types: bindings.map(() => 'INTEGER' as const),
      estimatedCardinality: 0,
      getColumnBindings: () => bindings,
    } as unknown as LogicalAggregate;
  }

  it('COUNT(*) on non-empty data', () => {
    const child = new MockOperator([[[1], [2], [3]]], layout([0, 0]));
    const op = makeAggOp([makeAgg('COUNT', undefined, true)]);
    const agg = new PhysicalHashAggregate(child, op, noopCtx);
    expect(drainOperator(agg)).toEqual([[3]]);
  });

  it('COUNT(*) on empty → 0', () => {
    const child = new MockOperator([], layout([0, 0]));
    const op = makeAggOp([makeAgg('COUNT', undefined, true)]);
    const agg = new PhysicalHashAggregate(child, op, noopCtx);
    expect(drainOperator(agg)).toEqual([[0]]);
  });

  it('SUM', () => {
    const child = new MockOperator([[[10], [20], [30]]], layout([0, 0]));
    const op = makeAggOp([makeAgg('SUM')]);
    expect(drainOperator(new PhysicalHashAggregate(child, op, noopCtx))).toEqual([[60]]);
  });

  it('SUM on empty → null', () => {
    const child = new MockOperator([], layout([0, 0]));
    const op = makeAggOp([makeAgg('SUM')]);
    expect(drainOperator(new PhysicalHashAggregate(child, op, noopCtx))).toEqual([[null]]);
  });

  it('AVG', () => {
    const child = new MockOperator([[[10], [20], [30]]], layout([0, 0]));
    const op = makeAggOp([makeAgg('AVG')]);
    expect(drainOperator(new PhysicalHashAggregate(child, op, noopCtx))).toEqual([[20]]);
  });

  it('MIN / MAX', () => {
    const child = new MockOperator([[[5], [1], [9]]], layout([0, 0]));
    const op = makeAggOp([
      makeAgg('MIN', colRef(0, 0), false, false, 0),
      makeAgg('MAX', colRef(0, 0), false, false, 1),
    ]);
    expect(drainOperator(new PhysicalHashAggregate(child, op, noopCtx))).toEqual([[1, 9]]);
  });

  it('COUNT(expr) ignores NULL', () => {
    const child = new MockOperator([[[1], [null], [3]]], layout([0, 0]));
    const op = makeAggOp([makeAgg('COUNT')]);
    expect(drainOperator(new PhysicalHashAggregate(child, op, noopCtx))).toEqual([[2]]);
  });

  it('GROUP BY', () => {
    const child = new MockOperator(
      [[['a', 10], ['b', 20], ['a', 30]]],
      layout([0, 0], [0, 1]),
    );
    const op = makeAggOp(
      [makeAgg('SUM', colRef(0, 1))],
      [colRef(0, 0, 'group', 'TEXT')],
    );
    const result = drainOperator(new PhysicalHashAggregate(child, op, noopCtx));
    expect(result).toHaveLength(2);
    const sorted = [...result].sort((a, b) => String(a[0]).localeCompare(String(b[0])));
    expect(sorted).toEqual([['a', 40], ['b', 20]]);
  });
});

// ---------------------------------------------------------------------------
// Sort
// ---------------------------------------------------------------------------

describe('PhysicalSort', () => {
  function makeOrderBy(orders: BoundOrderByNode[]): LogicalOrderBy {
    return {
      type: LogicalOperatorType.LOGICAL_ORDER_BY,
      orders,
    } as unknown as LogicalOrderBy;
  }

  it('sorts ASC', () => {
    const child = new MockOperator([[[3], [1], [2]]], layout([0, 0]));
    const op = makeOrderBy([
      { expression: colRef(0, 0), orderType: 'ASCENDING', nullOrder: 'NULLS_LAST' },
    ]);
    expect(drainOperator(new PhysicalSort(child, op, noopCtx))).toEqual([[1], [2], [3]]);
  });

  it('sorts DESC', () => {
    const child = new MockOperator([[[3], [1], [2]]], layout([0, 0]));
    const op = makeOrderBy([
      { expression: colRef(0, 0), orderType: 'DESCENDING', nullOrder: 'NULLS_LAST' },
    ]);
    expect(drainOperator(new PhysicalSort(child, op, noopCtx))).toEqual([[3], [2], [1]]);
  });

  it('NULLS FIRST', () => {
    const child = new MockOperator([[[3], [null], [1]]], layout([0, 0]));
    const op = makeOrderBy([
      { expression: colRef(0, 0), orderType: 'ASCENDING', nullOrder: 'NULLS_FIRST' },
    ]);
    expect(drainOperator(new PhysicalSort(child, op, noopCtx))).toEqual([[null], [1], [3]]);
  });

  it('NULLS LAST', () => {
    const child = new MockOperator([[[null], [3], [1]]], layout([0, 0]));
    const op = makeOrderBy([
      { expression: colRef(0, 0), orderType: 'ASCENDING', nullOrder: 'NULLS_LAST' },
    ]);
    expect(drainOperator(new PhysicalSort(child, op, noopCtx))).toEqual([[1], [3], [null]]);
  });
});

// ---------------------------------------------------------------------------
// Limit
// ---------------------------------------------------------------------------

describe('PhysicalLimit', () => {
  function makeLimit(limitVal: number | null, offsetVal = 0): LogicalLimit {
    return {
      type: LogicalOperatorType.LOGICAL_LIMIT,
      limitVal,
      offsetVal,
    } as unknown as LogicalLimit;
  }

  it('limits rows', () => {
    const child = new MockOperator([[[1], [2], [3], [4], [5]]], layout([0, 0]));
    expect(drainOperator(new PhysicalLimitOp(child, makeLimit(3)))).toEqual([[1], [2], [3]]);
  });

  it('offset skips rows', () => {
    const child = new MockOperator([[[1], [2], [3], [4], [5]]], layout([0, 0]));
    expect(drainOperator(new PhysicalLimitOp(child, makeLimit(2, 2)))).toEqual([[3], [4]]);
  });

  it('offset beyond data → empty', () => {
    const child = new MockOperator([[[1], [2]]], layout([0, 0]));
    expect(drainOperator(new PhysicalLimitOp(child, makeLimit(10, 100)))).toEqual([]);
  });

  it('limit null → all rows (only offset)', () => {
    const child = new MockOperator([[[1], [2], [3]]], layout([0, 0]));
    expect(drainOperator(new PhysicalLimitOp(child, makeLimit(null, 1)))).toEqual([[2], [3]]);
  });
});

// ---------------------------------------------------------------------------
// Distinct
// ---------------------------------------------------------------------------

describe('PhysicalDistinct', () => {
  it('removes duplicates', () => {
    const child = new MockOperator(
      [[[1, 'a'], [2, 'b'], [1, 'a'], [3, 'c']]],
      layout([0, 0], [0, 1]),
    );
    expect(drainOperator(new PhysicalDistinct(child))).toEqual([[1, 'a'], [2, 'b'], [3, 'c']]);
  });

  it('all duplicates → one row', () => {
    const child = new MockOperator([[[1], [1], [1]]], layout([0, 0]));
    expect(drainOperator(new PhysicalDistinct(child))).toEqual([[1]]);
  });
});

// ---------------------------------------------------------------------------
// Union
// ---------------------------------------------------------------------------

describe('PhysicalUnion', () => {
  it('UNION ALL — all rows', () => {
    const left = new MockOperator([[[1], [2]]], layout([0, 0]));
    const right = new MockOperator([[[2], [3]]], layout([0, 0]));
    expect(drainOperator(new PhysicalUnion(left, right, true))).toEqual([[1], [2], [2], [3]]);
  });

  it('UNION — deduplicates', () => {
    const left = new MockOperator([[[1], [2]]], layout([0, 0]));
    const right = new MockOperator([[[2], [3]]], layout([0, 0]));
    expect(drainOperator(new PhysicalUnion(left, right, false))).toEqual([[1], [2], [3]]);
  });
});

// ---------------------------------------------------------------------------
// CTE (Materialize + CTE Scan)
// ---------------------------------------------------------------------------

describe('CTE operators', () => {
  it('materialize caches and delegates to main plan', () => {
    const cteCache = new Map<number, CTECacheEntry>();
    const cteDef = new MockOperator([[[10], [20]]], layout([0, 0]));
    const mainPlan = new MockOperator([[['result']]], layout([1, 0]));

    const mat = new PhysicalMaterialize(cteDef, mainPlan, 0, cteCache);
    const result = drainOperator(mat);

    expect(result).toEqual([['result']]);
    expect(cteCache.has(0)).toBe(true);
    expect(cteCache.get(0)!.tuples).toEqual([[10], [20]]);
  });

  it('CTE scan reads from cache', () => {
    const cteCache = new Map<number, CTECacheEntry>();
    cteCache.set(0, {
      tuples: [[1, 'a'], [2, 'b']],
      layout: layout([0, 0], [0, 1]),
    });

    const ref = {
      type: LogicalOperatorType.LOGICAL_CTE_REF,
      cteName: 'cte0',
      cteIndex: 0,
      getColumnBindings: () => layout([0, 0], [0, 1]),
    } as any;

    const scan = new PhysicalCTEScan(ref, cteCache);
    expect(drainOperator(scan)).toEqual([[1, 'a'], [2, 'b']]);
  });

  it('CTE scan throws if not materialized', () => {
    const cteCache = new Map<number, CTECacheEntry>();
    const ref = {
      type: LogicalOperatorType.LOGICAL_CTE_REF,
      cteName: 'missing',
      cteIndex: 99,
      getColumnBindings: () => [],
    } as any;

    const scan = new PhysicalCTEScan(ref, cteCache);
    expect(() => scan.next()).toThrow('not materialized');
  });
});
