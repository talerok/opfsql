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
  it('keeps matching tuples', async () => {
    const child = new MockOperator(
      [[[1, 'a'], [2, 'b'], [3, 'c']]],
      layout([0, 0], [0, 1]),
    );
    // condition: col0 > 1
    const cond = comparison(colRef(0, 0), constant(1), 'GREATER');
    const filter = new PhysicalFilter(child, cond, noopCtx);
    const result = await drainOperator(filter);
    expect(result).toEqual([[2, 'b'], [3, 'c']]);
  });

  it('returns null when all filtered out', async () => {
    const child = new MockOperator(
      [[[1, 'a']]],
      layout([0, 0], [0, 1]),
    );
    const cond = comparison(colRef(0, 0), constant(100), 'GREATER');
    const filter = new PhysicalFilter(child, cond, noopCtx);
    const result = await drainOperator(filter);
    expect(result).toEqual([]);
  });

  it('null condition filters tuple out', async () => {
    const child = new MockOperator(
      [[[null, 'a']]],
      layout([0, 0], [0, 1]),
    );
    const cond = comparison(colRef(0, 0), constant(1), 'EQUAL');
    const filter = new PhysicalFilter(child, cond, noopCtx);
    const result = await drainOperator(filter);
    expect(result).toEqual([]);
  });
});

// ---------------------------------------------------------------------------
// Projection
// ---------------------------------------------------------------------------

describe('PhysicalProjection', () => {
  it('projects expressions', async () => {
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
    const result = await drainOperator(proj);
    expect(result).toEqual([['hello', 15]]);
  });

  it('handles constant projection', async () => {
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
    const result = await drainOperator(proj);
    expect(result).toEqual([[42], [42]]);
  });
});

// ---------------------------------------------------------------------------
// Hash Join
// ---------------------------------------------------------------------------

describe('PhysicalHashJoin', () => {
  const probeData = new MockOperator(
    [[[1, 'a'], [2, 'b'], [3, 'c']]],
    layout([0, 0], [0, 1]),
  );
  const buildData = new MockOperator(
    [[[1, 'x'], [2, 'y']]],
    layout([1, 0], [1, 1]),
  );

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

  it('INNER JOIN — matching rows only', async () => {
    const probe = new MockOperator([[[1, 'a'], [2, 'b'], [3, 'c']]], layout([0, 0], [0, 1]));
    const build = new MockOperator([[[1, 'x'], [2, 'y']]], layout([1, 0], [1, 1]));
    const join = new PhysicalHashJoin(probe, build, makeJoinOp('INNER'), noopCtx);
    const result = await drainOperator(join);
    expect(result).toEqual([
      [1, 'a', 1, 'x'],
      [2, 'b', 2, 'y'],
    ]);
  });

  it('LEFT JOIN — unmatched probe gets nulls', async () => {
    const probe = new MockOperator([[[1, 'a'], [2, 'b'], [3, 'c']]], layout([0, 0], [0, 1]));
    const build = new MockOperator([[[1, 'x']]], layout([1, 0], [1, 1]));
    const join = new PhysicalHashJoin(probe, build, makeJoinOp('LEFT'), noopCtx);
    const result = await drainOperator(join);
    expect(result).toEqual([
      [1, 'a', 1, 'x'],
      [2, 'b', null, null],
      [3, 'c', null, null],
    ]);
  });

  it('NULL join key — never matches', async () => {
    const probe = new MockOperator([[[null, 'a']]], layout([0, 0], [0, 1]));
    const build = new MockOperator([[[null, 'x']]], layout([1, 0], [1, 1]));
    const join = new PhysicalHashJoin(probe, build, makeJoinOp('INNER'), noopCtx);
    const result = await drainOperator(join);
    expect(result).toEqual([]);
  });

  it('empty build side', async () => {
    const probe = new MockOperator([[[1, 'a']]], layout([0, 0], [0, 1]));
    const build = new MockOperator([], layout([1, 0], [1, 1]));
    const join = new PhysicalHashJoin(probe, build, makeJoinOp('INNER'), noopCtx);
    const result = await drainOperator(join);
    expect(result).toEqual([]);
  });
});

// ---------------------------------------------------------------------------
// Nested Loop Join (CROSS)
// ---------------------------------------------------------------------------

describe('PhysicalNestedLoopJoin', () => {
  it('produces cartesian product', async () => {
    const left = new MockOperator([[[1], [2]]], layout([0, 0]));
    const right = new MockOperator([[['a'], ['b']]], layout([1, 0]));
    const join = new PhysicalNestedLoopJoin(left, right);
    const result = await drainOperator(join);
    expect(result).toEqual([
      [1, 'a'],
      [1, 'b'],
      [2, 'a'],
      [2, 'b'],
    ]);
  });

  it('empty right side → no output', async () => {
    const left = new MockOperator([[[1]]], layout([0, 0]));
    const right = new MockOperator([], layout([1, 0]));
    const join = new PhysicalNestedLoopJoin(left, right);
    const result = await drainOperator(join);
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

  it('COUNT(*) on non-empty data', async () => {
    const child = new MockOperator([[[1], [2], [3]]], layout([0, 0]));
    const op = makeAggOp([makeAgg('COUNT', undefined, true)]);
    const agg = new PhysicalHashAggregate(child, op, noopCtx);
    const result = await drainOperator(agg);
    expect(result).toEqual([[3]]);
  });

  it('COUNT(*) on empty → 0', async () => {
    const child = new MockOperator([], layout([0, 0]));
    const op = makeAggOp([makeAgg('COUNT', undefined, true)]);
    const agg = new PhysicalHashAggregate(child, op, noopCtx);
    const result = await drainOperator(agg);
    expect(result).toEqual([[0]]);
  });

  it('SUM', async () => {
    const child = new MockOperator([[[10], [20], [30]]], layout([0, 0]));
    const op = makeAggOp([makeAgg('SUM')]);
    const agg = new PhysicalHashAggregate(child, op, noopCtx);
    const result = await drainOperator(agg);
    expect(result).toEqual([[60]]);
  });

  it('SUM on empty → null', async () => {
    const child = new MockOperator([], layout([0, 0]));
    const op = makeAggOp([makeAgg('SUM')]);
    const agg = new PhysicalHashAggregate(child, op, noopCtx);
    const result = await drainOperator(agg);
    expect(result).toEqual([[null]]);
  });

  it('AVG', async () => {
    const child = new MockOperator([[[10], [20], [30]]], layout([0, 0]));
    const op = makeAggOp([makeAgg('AVG')]);
    const agg = new PhysicalHashAggregate(child, op, noopCtx);
    const result = await drainOperator(agg);
    expect(result).toEqual([[20]]);
  });

  it('MIN / MAX', async () => {
    const child = new MockOperator([[[5], [1], [9]]], layout([0, 0]));
    const op = makeAggOp([makeAgg('MIN', colRef(0, 0), false, false, 0), makeAgg('MAX', colRef(0, 0), false, false, 1)]);
    const agg = new PhysicalHashAggregate(child, op, noopCtx);
    const result = await drainOperator(agg);
    expect(result).toEqual([[1, 9]]);
  });

  it('COUNT(expr) ignores NULL', async () => {
    const child = new MockOperator([[[1], [null], [3]]], layout([0, 0]));
    const op = makeAggOp([makeAgg('COUNT')]);
    const agg = new PhysicalHashAggregate(child, op, noopCtx);
    const result = await drainOperator(agg);
    expect(result).toEqual([[2]]);
  });

  it('GROUP BY', async () => {
    const child = new MockOperator(
      [[['a', 10], ['b', 20], ['a', 30]]],
      layout([0, 0], [0, 1]),
    );
    const op = makeAggOp(
      [makeAgg('SUM', colRef(0, 1))],
      [colRef(0, 0, 'group', 'TEXT')],
    );
    const agg = new PhysicalHashAggregate(child, op, noopCtx);
    const result = await drainOperator(agg);
    // Groups: 'a' → 40, 'b' → 20
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

  it('sorts ASC', async () => {
    const child = new MockOperator([[[3], [1], [2]]], layout([0, 0]));
    const op = makeOrderBy([
      { expression: colRef(0, 0), orderType: 'ASCENDING', nullOrder: 'NULLS_LAST' },
    ]);
    const sort = new PhysicalSort(child, op, noopCtx);
    const result = await drainOperator(sort);
    expect(result).toEqual([[1], [2], [3]]);
  });

  it('sorts DESC', async () => {
    const child = new MockOperator([[[3], [1], [2]]], layout([0, 0]));
    const op = makeOrderBy([
      { expression: colRef(0, 0), orderType: 'DESCENDING', nullOrder: 'NULLS_LAST' },
    ]);
    const sort = new PhysicalSort(child, op, noopCtx);
    const result = await drainOperator(sort);
    expect(result).toEqual([[3], [2], [1]]);
  });

  it('NULLS FIRST', async () => {
    const child = new MockOperator([[[3], [null], [1]]], layout([0, 0]));
    const op = makeOrderBy([
      { expression: colRef(0, 0), orderType: 'ASCENDING', nullOrder: 'NULLS_FIRST' },
    ]);
    const sort = new PhysicalSort(child, op, noopCtx);
    const result = await drainOperator(sort);
    expect(result).toEqual([[null], [1], [3]]);
  });

  it('NULLS LAST', async () => {
    const child = new MockOperator([[[null], [3], [1]]], layout([0, 0]));
    const op = makeOrderBy([
      { expression: colRef(0, 0), orderType: 'ASCENDING', nullOrder: 'NULLS_LAST' },
    ]);
    const sort = new PhysicalSort(child, op, noopCtx);
    const result = await drainOperator(sort);
    expect(result).toEqual([[1], [3], [null]]);
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

  it('limits rows', async () => {
    const child = new MockOperator([[[1], [2], [3], [4], [5]]], layout([0, 0]));
    const limit = new PhysicalLimitOp(child, makeLimit(3));
    const result = await drainOperator(limit);
    expect(result).toEqual([[1], [2], [3]]);
  });

  it('offset skips rows', async () => {
    const child = new MockOperator([[[1], [2], [3], [4], [5]]], layout([0, 0]));
    const limit = new PhysicalLimitOp(child, makeLimit(2, 2));
    const result = await drainOperator(limit);
    expect(result).toEqual([[3], [4]]);
  });

  it('offset beyond data → empty', async () => {
    const child = new MockOperator([[[1], [2]]], layout([0, 0]));
    const limit = new PhysicalLimitOp(child, makeLimit(10, 100));
    const result = await drainOperator(limit);
    expect(result).toEqual([]);
  });

  it('limit null → all rows (only offset)', async () => {
    const child = new MockOperator([[[1], [2], [3]]], layout([0, 0]));
    const limit = new PhysicalLimitOp(child, makeLimit(null, 1));
    const result = await drainOperator(limit);
    expect(result).toEqual([[2], [3]]);
  });
});

// ---------------------------------------------------------------------------
// Distinct
// ---------------------------------------------------------------------------

describe('PhysicalDistinct', () => {
  it('removes duplicates', async () => {
    const child = new MockOperator(
      [[[1, 'a'], [2, 'b'], [1, 'a'], [3, 'c']]],
      layout([0, 0], [0, 1]),
    );
    const distinct = new PhysicalDistinct(child);
    const result = await drainOperator(distinct);
    expect(result).toEqual([[1, 'a'], [2, 'b'], [3, 'c']]);
  });

  it('all duplicates → one row', async () => {
    const child = new MockOperator(
      [[[1], [1], [1]]],
      layout([0, 0]),
    );
    const distinct = new PhysicalDistinct(child);
    const result = await drainOperator(distinct);
    expect(result).toEqual([[1]]);
  });
});

// ---------------------------------------------------------------------------
// Union
// ---------------------------------------------------------------------------

describe('PhysicalUnion', () => {
  it('UNION ALL — all rows', async () => {
    const left = new MockOperator([[[1], [2]]], layout([0, 0]));
    const right = new MockOperator([[[2], [3]]], layout([0, 0]));
    const union = new PhysicalUnion(left, right, true);
    const result = await drainOperator(union);
    expect(result).toEqual([[1], [2], [2], [3]]);
  });

  it('UNION — deduplicates', async () => {
    const left = new MockOperator([[[1], [2]]], layout([0, 0]));
    const right = new MockOperator([[[2], [3]]], layout([0, 0]));
    const union = new PhysicalUnion(left, right, false);
    const result = await drainOperator(union);
    expect(result).toEqual([[1], [2], [3]]);
  });
});

// ---------------------------------------------------------------------------
// CTE (Materialize + CTE Scan)
// ---------------------------------------------------------------------------

describe('CTE operators', () => {
  it('materialize caches and delegates to main plan', async () => {
    const cteCache = new Map<number, CTECacheEntry>();
    const cteDef = new MockOperator([[[10], [20]]], layout([0, 0]));
    const mainPlan = new MockOperator([[['result']]], layout([1, 0]));

    const mat = new PhysicalMaterialize(cteDef, mainPlan, 0, cteCache);
    const result = await drainOperator(mat);

    expect(result).toEqual([['result']]);
    expect(cteCache.has(0)).toBe(true);
    expect(cteCache.get(0)!.tuples).toEqual([[10], [20]]);
  });

  it('CTE scan reads from cache', async () => {
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
    const result = await drainOperator(scan);
    expect(result).toEqual([[1, 'a'], [2, 'b']]);
  });

  it('CTE scan throws if not materialized', async () => {
    const cteCache = new Map<number, CTECacheEntry>();
    const ref = {
      type: LogicalOperatorType.LOGICAL_CTE_REF,
      cteName: 'missing',
      cteIndex: 99,
      getColumnBindings: () => [],
    } as any;

    const scan = new PhysicalCTEScan(ref, cteCache);
    await expect(scan.next()).rejects.toThrow('not materialized');
  });
});
