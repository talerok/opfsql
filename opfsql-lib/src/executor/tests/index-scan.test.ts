import { describe, it, expect, vi } from 'vitest';
import type {
  IndexSearchPredicate,
  LogicalGet,
  TableFilter,
} from '../../binder/types.js';
import { BoundExpressionClass, LogicalOperatorType } from '../../binder/types.js';
import type { SyncIIndexManager, SyncIRowManager, TableSchema, IndexDef } from '../../store/types.js';
import { PhysicalIndexScan } from '../operators/index-scan.js';
import { drainOperator } from '../operators/utils.js';
import type { SyncEvalContext } from '../evaluate/context.js';
import { colRef, noopCtx } from './helpers.js';

// ---------------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------------

const usersSchema: TableSchema = {
  name: 'users',
  columns: [
    { name: 'id',   type: 'INTEGER', nullable: false, primaryKey: true,  unique: true,  autoIncrement: false, defaultValue: null },
    { name: 'name', type: 'TEXT',    nullable: false, primaryKey: false, unique: false, autoIncrement: false, defaultValue: null },
    { name: 'age',  type: 'INTEGER', nullable: true,  primaryKey: false, unique: false, autoIncrement: false, defaultValue: null },
  ],
};

const idxAge: IndexDef = {
  name: 'idx_age',
  tableName: 'users',
  expressions: [{ type: 'column', name: 'age', returnType: 'INTEGER' }],
  unique: false,
};

function makeGet(overrides: Partial<LogicalGet> = {}): LogicalGet {
  return {
    type: LogicalOperatorType.LOGICAL_GET,
    children: [],
    expressions: [],
    types: ['INTEGER', 'TEXT', 'INTEGER'],
    estimatedCardinality: 100,
    tableIndex: 0,
    tableName: 'users',
    schema: usersSchema,
    columnIds: [0, 1, 2],
    tableFilters: [],
    getColumnBindings: () => [
      { tableIndex: 0, columnIndex: 0 },
      { tableIndex: 0, columnIndex: 1 },
      { tableIndex: 0, columnIndex: 2 },
    ],
    ...overrides,
  };
}

const rowStore: Record<number, Record<string, any>> = {
  1: { id: 1, name: 'Alice', age: 30 },
  2: { id: 2, name: 'Bob',   age: 25 },
  3: { id: 3, name: 'Carol', age: 35 },
  4: { id: 4, name: 'Dave',  age: 30 },
};

function mockRowManager(): SyncIRowManager {
  return {
    createTable: vi.fn(() => 0),
    prepareInsert: vi.fn(() => 0),
    prepareUpdate: vi.fn(() => 0),
    prepareDelete: vi.fn(),
    scanTable: vi.fn(() => []),
    readRow: vi.fn((_, rowId: number) => rowStore[rowId] ?? null),
    deleteTableData: vi.fn(),
  };
}

function mockIndexManager(searchResult: number[]): SyncIIndexManager {
  return {
    insert: vi.fn(),
    delete: vi.fn(),
    search: vi.fn(() => searchResult),
    bulkLoad: vi.fn(() => 0),
    dropIndex: vi.fn(),
    first: vi.fn(() => null),
    last: vi.fn(() => null),
  };
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

describe('PhysicalIndexScan', () => {
  it('fetches rows by index-returned rowIds', () => {
    const rm = mockRowManager();
    const im = mockIndexManager([1, 3]);

    const predicates: IndexSearchPredicate[] = [{
      columnPosition: 0,
      comparisonType: 'GREATER_EQUAL',
      value: { expressionClass: BoundExpressionClass.BOUND_CONSTANT, value: 30, returnType: 'INTEGER' },
    }];

    const scan = new PhysicalIndexScan(makeGet(), rm, im, idxAge, predicates, [], noopCtx);
    const result = drainOperator(scan);
    expect(result).toEqual([
      [1, 'Alice', 30],
      [3, 'Carol', 35],
    ]);
    expect(im.search).toHaveBeenCalledWith('idx_age', [
      { columnPosition: 0, comparisonType: 'GREATER_EQUAL', value: 30 },
    ]);
  });

  it('applies residual filters after fetch', () => {
    const rm = mockRowManager();
    const im = mockIndexManager([1, 3, 4]); // age >= 30: Alice(30), Carol(35), Dave(30)

    const predicates: IndexSearchPredicate[] = [{
      columnPosition: 0,
      comparisonType: 'GREATER_EQUAL',
      value: { expressionClass: BoundExpressionClass.BOUND_CONSTANT, value: 30, returnType: 'INTEGER' },
    }];

    // residual: name != 'Dave'
    const residual: TableFilter = {
      expression: colRef(0, 1, 'name', 'TEXT'),
      comparisonType: 'NOT_EQUAL',
      constant: { expressionClass: BoundExpressionClass.BOUND_CONSTANT, value: 'Dave', returnType: 'TEXT' },
    };

    const scan = new PhysicalIndexScan(makeGet(), rm, im, idxAge, predicates, [residual], noopCtx);
    const result = drainOperator(scan);
    expect(result).toEqual([
      [1, 'Alice', 30],
      [3, 'Carol', 35],
    ]);
  });

  it('skips null rows from readRow', () => {
    const rm = mockRowManager();
    // rowId 99 doesn't exist in rowStore
    const im = mockIndexManager([1, 99, 3]);

    const predicates: IndexSearchPredicate[] = [{
      columnPosition: 0,
      comparisonType: 'GREATER_EQUAL',
      value: { expressionClass: BoundExpressionClass.BOUND_CONSTANT, value: 0, returnType: 'INTEGER' },
    }];

    const scan = new PhysicalIndexScan(makeGet(), rm, im, idxAge, predicates, [], noopCtx);
    const result = drainOperator(scan);
    expect(result).toEqual([
      [1, 'Alice', 30],
      [3, 'Carol', 35],
    ]);
  });

  it('returns null for empty index result', () => {
    const rm = mockRowManager();
    const im = mockIndexManager([]);

    const predicates: IndexSearchPredicate[] = [{
      columnPosition: 0,
      comparisonType: 'EQUAL',
      value: { expressionClass: BoundExpressionClass.BOUND_CONSTANT, value: 999, returnType: 'INTEGER' },
    }];

    const scan = new PhysicalIndexScan(makeGet(), rm, im, idxAge, predicates, [], noopCtx);
    expect(scan.next()).toBeNull();
  });

  it('resolves parameter-based predicate values', () => {
    const rm = mockRowManager();
    const im = mockIndexManager([2]);

    const predicates: IndexSearchPredicate[] = [{
      columnPosition: 0,
      comparisonType: 'EQUAL',
      value: { expressionClass: BoundExpressionClass.BOUND_PARAMETER, index: 0, returnType: 'INTEGER' },
    }];

    const ctx: SyncEvalContext = { ...noopCtx, params: [25] };
    const scan = new PhysicalIndexScan(makeGet(), rm, im, idxAge, predicates, [], ctx);
    const result = drainOperator(scan);
    expect(result).toEqual([[2, 'Bob', 25]]);
    expect(im.search).toHaveBeenCalledWith('idx_age', [
      { columnPosition: 0, comparisonType: 'EQUAL', value: 25 },
    ]);
  });

  it('reset re-fetches from index', () => {
    const rm = mockRowManager();
    const im = mockIndexManager([1]);

    const predicates: IndexSearchPredicate[] = [{
      columnPosition: 0,
      comparisonType: 'EQUAL',
      value: { expressionClass: BoundExpressionClass.BOUND_CONSTANT, value: 30, returnType: 'INTEGER' },
    }];

    const scan = new PhysicalIndexScan(makeGet(), rm, im, idxAge, predicates, [], noopCtx);
    const first = drainOperator(scan);
    expect(first).toHaveLength(1);

    scan.reset();
    const second = drainOperator(scan);
    expect(second).toEqual(first);
    expect(im.search).toHaveBeenCalledTimes(2);
  });

  it('getLayout returns column bindings', () => {
    const rm = mockRowManager();
    const im = mockIndexManager([]);
    const scan = new PhysicalIndexScan(makeGet(), rm, im, idxAge, [], [], noopCtx);
    expect(scan.getLayout()).toEqual([
      { tableIndex: 0, columnIndex: 0 },
      { tableIndex: 0, columnIndex: 1 },
      { tableIndex: 0, columnIndex: 2 },
    ]);
  });
});
