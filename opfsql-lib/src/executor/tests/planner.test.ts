import { describe, it, expect, vi } from 'vitest';
import type {
  LogicalGet,
  LogicalFilter,
  LogicalProjection,
  LogicalAggregate,
  LogicalComparisonJoin,
  LogicalOrderBy,
  LogicalLimit,
  LogicalUnion,
  BoundExpression,
  BoundAggregateExpression,
  IndexScanHint,
  IndexUnionHint,
  MinMaxHint,
} from '../../binder/types.js';
import { BoundExpressionClass, LogicalOperatorType } from '../../binder/types.js';
import type { SyncIIndexManager, SyncIRowManager, TableSchema, IndexDef } from '../../store/types.js';
import { createPhysicalPlan } from '../planner/index.js';
import { PhysicalScan, PhysicalChildScan } from '../operators/scan.js';
import { PhysicalIndexScan } from '../operators/index-scan.js';
import { PhysicalIndexMinMax } from '../operators/index-min-max.js';
import { PhysicalIndexUnionScan } from '../operators/index-union-scan.js';
import { PhysicalFilter } from '../operators/filter.js';
import { PhysicalProjection as PhysicalProjectionOp } from '../operators/projection.js';
import { PhysicalHashAggregate } from '../operators/aggregate.js';
import { PhysicalHashJoin } from '../operators/join.js';
import { PhysicalSort } from '../operators/sort.js';
import { PhysicalLimit as PhysicalLimitOp } from '../operators/limit.js';
import { PhysicalDistinct, PhysicalUnion as PhysicalUnionOp } from '../operators/set.js';
import { ExecutorError } from '../errors.js';
import type { CTECacheEntry } from '../types.js';
import { colRef, constant, comparison, noopCtx } from './helpers.js';

// ---------------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------------

const usersSchema: TableSchema = {
  name: 'users',
  columns: [
    { name: 'id',   type: 'INTEGER', nullable: false, primaryKey: true,  unique: true,  autoIncrement: false, defaultValue: null },
    { name: 'name', type: 'TEXT',    nullable: false, primaryKey: false, unique: false, autoIncrement: false, defaultValue: null },
  ],
};

const idxId: IndexDef = {
  name: 'idx_id',
  tableName: 'users',
  expressions: [{ type: 'column', name: 'id', returnType: 'INTEGER' }],
  unique: true,
};

function mockRowManager(): SyncIRowManager {
  return {
    createTable: vi.fn(() => 0),
    prepareInsert: vi.fn(() => 0),
    prepareUpdate: vi.fn(() => 0),
    prepareDelete: vi.fn(),
    scanTable: vi.fn(() => []),
    readRow: vi.fn(() => null),
    deleteTableData: vi.fn(),
  };
}

function mockIndexManager(): SyncIIndexManager {
  return {
    insert: vi.fn(),
    delete: vi.fn(),
    search: vi.fn(() => []),
    bulkLoad: vi.fn(() => 0),
    dropIndex: vi.fn(),
    first: vi.fn(() => null),
    last: vi.fn(() => null),
  };
}

function makeGet(overrides: Partial<LogicalGet> = {}): LogicalGet {
  return {
    type: LogicalOperatorType.LOGICAL_GET,
    children: [],
    expressions: [],
    types: ['INTEGER', 'TEXT'],
    estimatedCardinality: 100,
    tableIndex: 0,
    tableName: 'users',
    schema: usersSchema,
    columnIds: [0, 1],
    tableFilters: [],
    getColumnBindings: () => [
      { tableIndex: 0, columnIndex: 0 },
      { tableIndex: 0, columnIndex: 1 },
    ],
    ...overrides,
  };
}

const cteCache = new Map<number, CTECacheEntry>();

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

describe('createPhysicalPlan', () => {
  it('LOGICAL_GET → PhysicalScan', () => {
    const plan = createPhysicalPlan(makeGet(), mockRowManager(), cteCache, noopCtx);
    expect(plan).toBeInstanceOf(PhysicalScan);
  });

  it('LOGICAL_GET with children → PhysicalChildScan', () => {
    const childGet = makeGet();
    const parentGet = makeGet({ children: [childGet] });
    const plan = createPhysicalPlan(parentGet, mockRowManager(), cteCache, noopCtx);
    expect(plan).toBeInstanceOf(PhysicalChildScan);
  });

  it('LOGICAL_GET with indexHint scan → PhysicalIndexScan', () => {
    const hint: IndexScanHint = {
      kind: 'scan',
      indexDef: idxId,
      predicates: [{
        columnPosition: 0,
        comparisonType: 'EQUAL',
        value: { expressionClass: BoundExpressionClass.BOUND_CONSTANT, value: 1, returnType: 'INTEGER' },
      }],
      residualFilters: [],
      coveredFilters: [],
    };
    const get = makeGet({ indexHint: hint });
    const plan = createPhysicalPlan(get, mockRowManager(), cteCache, noopCtx, mockIndexManager());
    expect(plan).toBeInstanceOf(PhysicalIndexScan);
  });

  it('LOGICAL_GET with indexHint union → PhysicalIndexUnionScan', () => {
    const hint: IndexUnionHint = {
      kind: 'union',
      branches: [{
        kind: 'scan',
        indexDef: idxId,
        predicates: [{
          columnPosition: 0,
          comparisonType: 'EQUAL',
          value: { expressionClass: BoundExpressionClass.BOUND_CONSTANT, value: 1, returnType: 'INTEGER' },
        }],
        residualFilters: [],
        coveredFilters: [],
      }],
      originalFilter: comparison(colRef(0, 0), constant(1), 'EQUAL'),
    };
    const get = makeGet({ indexHint: hint });
    const plan = createPhysicalPlan(get, mockRowManager(), cteCache, noopCtx, mockIndexManager());
    expect(plan).toBeInstanceOf(PhysicalIndexUnionScan);
  });

  it('LOGICAL_GET with indexHint but no indexManager → PhysicalScan', () => {
    const hint: IndexScanHint = {
      kind: 'scan',
      indexDef: idxId,
      predicates: [],
      residualFilters: [],
      coveredFilters: [],
    };
    const get = makeGet({ indexHint: hint });
    const plan = createPhysicalPlan(get, mockRowManager(), cteCache, noopCtx);
    expect(plan).toBeInstanceOf(PhysicalScan);
  });

  it('LOGICAL_FILTER single expr → PhysicalFilter', () => {
    const filter: LogicalFilter = {
      type: LogicalOperatorType.LOGICAL_FILTER,
      children: [makeGet()],
      expressions: [comparison(colRef(0, 0), constant(1), 'EQUAL')],
      types: ['INTEGER', 'TEXT'],
      estimatedCardinality: 50,
      getColumnBindings: () => [
        { tableIndex: 0, columnIndex: 0 },
        { tableIndex: 0, columnIndex: 1 },
      ],
    };
    const plan = createPhysicalPlan(filter, mockRowManager(), cteCache, noopCtx);
    expect(plan).toBeInstanceOf(PhysicalFilter);
  });

  it('LOGICAL_FILTER multiple exprs → PhysicalFilter with AND conjunction', () => {
    const filter: LogicalFilter = {
      type: LogicalOperatorType.LOGICAL_FILTER,
      children: [makeGet()],
      expressions: [
        comparison(colRef(0, 0), constant(1), 'GREATER'),
        comparison(colRef(0, 0), constant(10), 'LESS'),
      ],
      types: ['INTEGER', 'TEXT'],
      estimatedCardinality: 50,
      getColumnBindings: () => [
        { tableIndex: 0, columnIndex: 0 },
        { tableIndex: 0, columnIndex: 1 },
      ],
    };
    const plan = createPhysicalPlan(filter, mockRowManager(), cteCache, noopCtx);
    expect(plan).toBeInstanceOf(PhysicalFilter);
  });

  it('LOGICAL_PROJECTION → PhysicalProjection', () => {
    const proj: LogicalProjection = {
      type: LogicalOperatorType.LOGICAL_PROJECTION,
      tableIndex: 1,
      children: [makeGet()],
      expressions: [colRef(0, 0)],
      aliases: [null],
      types: ['INTEGER'],
      estimatedCardinality: 100,
      getColumnBindings: () => [{ tableIndex: 1, columnIndex: 0 }],
    };
    const plan = createPhysicalPlan(proj, mockRowManager(), cteCache, noopCtx);
    expect(plan).toBeInstanceOf(PhysicalProjectionOp);
  });

  it('LOGICAL_AGGREGATE without hint → PhysicalHashAggregate', () => {
    const aggExpr: BoundAggregateExpression = {
      expressionClass: BoundExpressionClass.BOUND_AGGREGATE,
      functionName: 'COUNT',
      distinct: false,
      isStar: true,
      aggregateIndex: 0,
      children: [],
      returnType: 'INTEGER',
    };
    const agg: LogicalAggregate = {
      type: LogicalOperatorType.LOGICAL_AGGREGATE_AND_GROUP_BY,
      groupIndex: 0,
      aggregateIndex: 1,
      children: [makeGet()],
      expressions: [aggExpr],
      groups: [],
      havingExpression: null,
      types: ['INTEGER'],
      estimatedCardinality: 1,
      getColumnBindings: () => [{ tableIndex: 1, columnIndex: 0 }],
    };
    const plan = createPhysicalPlan(agg, mockRowManager(), cteCache, noopCtx);
    expect(plan).toBeInstanceOf(PhysicalHashAggregate);
  });

  it('LOGICAL_AGGREGATE with minMaxHint → PhysicalIndexMinMax', () => {
    const aggExpr: BoundAggregateExpression = {
      expressionClass: BoundExpressionClass.BOUND_AGGREGATE,
      functionName: 'MIN',
      distinct: false,
      isStar: false,
      aggregateIndex: 0,
      children: [colRef(0, 0)],
      returnType: 'INTEGER',
    };
    const hint: MinMaxHint = { indexDef: idxId, functionName: 'MIN', keyPosition: 0 };
    const agg: LogicalAggregate = {
      type: LogicalOperatorType.LOGICAL_AGGREGATE_AND_GROUP_BY,
      groupIndex: 0,
      aggregateIndex: 1,
      children: [makeGet()],
      expressions: [aggExpr],
      groups: [],
      havingExpression: null,
      types: ['INTEGER'],
      estimatedCardinality: 1,
      minMaxHint: hint,
      getColumnBindings: () => [{ tableIndex: 1, columnIndex: 0 }],
    };
    const plan = createPhysicalPlan(agg, mockRowManager(), cteCache, noopCtx, mockIndexManager());
    expect(plan).toBeInstanceOf(PhysicalIndexMinMax);
  });

  it('LOGICAL_ORDER_BY → PhysicalSort', () => {
    const orderBy: LogicalOrderBy = {
      type: LogicalOperatorType.LOGICAL_ORDER_BY,
      children: [makeGet()],
      orders: [{ expression: colRef(0, 0), orderType: 'ASCENDING', nullOrder: 'NULLS_LAST' }],
      expressions: [],
      types: ['INTEGER', 'TEXT'],
      estimatedCardinality: 100,
      getColumnBindings: () => [
        { tableIndex: 0, columnIndex: 0 },
        { tableIndex: 0, columnIndex: 1 },
      ],
    };
    const plan = createPhysicalPlan(orderBy, mockRowManager(), cteCache, noopCtx);
    expect(plan).toBeInstanceOf(PhysicalSort);
  });

  it('LOGICAL_LIMIT → PhysicalLimit', () => {
    const limit: LogicalLimit = {
      type: LogicalOperatorType.LOGICAL_LIMIT,
      children: [makeGet()],
      limitVal: 10,
      offsetVal: 0,
      expressions: [],
      types: ['INTEGER', 'TEXT'],
      estimatedCardinality: 10,
      getColumnBindings: () => [
        { tableIndex: 0, columnIndex: 0 },
        { tableIndex: 0, columnIndex: 1 },
      ],
    };
    const plan = createPhysicalPlan(limit, mockRowManager(), cteCache, noopCtx);
    expect(plan).toBeInstanceOf(PhysicalLimitOp);
  });

  it('LOGICAL_DISTINCT → PhysicalDistinct', () => {
    const distinct = {
      type: LogicalOperatorType.LOGICAL_DISTINCT as const,
      children: [makeGet()] as [LogicalGet],
      expressions: [] as BoundExpression[],
      types: ['INTEGER' as const, 'TEXT' as const],
      estimatedCardinality: 50,
      getColumnBindings: () => [
        { tableIndex: 0, columnIndex: 0 },
        { tableIndex: 0, columnIndex: 1 },
      ],
    };
    const plan = createPhysicalPlan(distinct as any, mockRowManager(), cteCache, noopCtx);
    expect(plan).toBeInstanceOf(PhysicalDistinct);
  });

  it('LOGICAL_UNION → PhysicalUnion', () => {
    const union: LogicalUnion = {
      type: LogicalOperatorType.LOGICAL_UNION,
      children: [makeGet(), makeGet()],
      all: false,
      expressions: [],
      types: ['INTEGER', 'TEXT'],
      estimatedCardinality: 200,
      getColumnBindings: () => [
        { tableIndex: 0, columnIndex: 0 },
        { tableIndex: 0, columnIndex: 1 },
      ],
    };
    const plan = createPhysicalPlan(union, mockRowManager(), cteCache, noopCtx);
    expect(plan).toBeInstanceOf(PhysicalUnionOp);
  });

  it('LOGICAL_COMPARISON_JOIN → PhysicalHashJoin', () => {
    const join: LogicalComparisonJoin = {
      type: LogicalOperatorType.LOGICAL_COMPARISON_JOIN,
      joinType: 'INNER',
      children: [makeGet(), makeGet({ tableIndex: 1, getColumnBindings: () => [{ tableIndex: 1, columnIndex: 0 }, { tableIndex: 1, columnIndex: 1 }] })],
      conditions: [{ left: colRef(0, 0), right: colRef(1, 0), comparisonType: 'EQUAL' }],
      expressions: [],
      types: ['INTEGER', 'TEXT', 'INTEGER', 'TEXT'],
      estimatedCardinality: 100,
      getColumnBindings: () => [
        { tableIndex: 0, columnIndex: 0 },
        { tableIndex: 0, columnIndex: 1 },
        { tableIndex: 1, columnIndex: 0 },
        { tableIndex: 1, columnIndex: 1 },
      ],
    };
    const plan = createPhysicalPlan(join, mockRowManager(), cteCache, noopCtx);
    expect(plan).toBeInstanceOf(PhysicalHashJoin);
  });

  it('DML/DDL type throws ExecutorError', () => {
    const insert = {
      type: LogicalOperatorType.LOGICAL_INSERT,
      children: [],
      expressions: [],
      types: [],
      estimatedCardinality: 0,
      getColumnBindings: () => [],
    };
    expect(() =>
      createPhysicalPlan(insert as any, mockRowManager(), cteCache, noopCtx),
    ).toThrow(ExecutorError);
  });
});
