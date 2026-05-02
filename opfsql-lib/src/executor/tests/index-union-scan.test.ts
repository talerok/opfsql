import { describe, it, expect, vi } from 'vitest';
import type {
  BoundExpression,
  IndexScanHint,
  IndexUnionHint,
  LogicalGet,
} from '../../binder/types.js';
import { BoundExpressionClass, LogicalOperatorType } from '../../binder/types.js';
import type { SyncIIndexManager, SyncIRowManager, TableSchema, IndexDef } from '../../store/types.js';
import { PhysicalIndexUnionScan } from '../operators/index-union-scan.js';
import { drainOperator } from '../operators/utils.js';
import type { SyncEvalContext } from '../evaluate/context.js';
import { colRef, constant, comparison, conjunction, noopCtx } from './helpers.js';

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

const idxName: IndexDef = {
  name: 'idx_name',
  tableName: 'users',
  expressions: [{ type: 'column', name: 'name', returnType: 'TEXT' }],
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
    columnBindings: [
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

function mockIndexManager(searchResults: Map<string, number[]>): SyncIIndexManager {
  return {
    insert: vi.fn(),
    delete: vi.fn(),
    search: vi.fn((indexName: string) => searchResults.get(indexName) ?? []),
    bulkLoad: vi.fn(() => 0),
    dropIndex: vi.fn(),
    first: vi.fn(() => null),
    last: vi.fn(() => null),
  };
}

// Build an OR expression: age = 30 OR name = 'Carol'
function makeOrFilter(): BoundExpression {
  return conjunction(
    'OR',
    comparison(colRef(0, 2, 'age', 'INTEGER'), constant(30), 'EQUAL'),
    comparison(colRef(0, 1, 'name', 'TEXT'), constant('Carol'), 'EQUAL'),
  );
}

function makeBranch(indexDef: IndexDef, predicates: IndexScanHint['predicates']): IndexScanHint {
  return {
    kind: 'scan',
    indexDef,
    predicates,
    residualFilters: [],
    coveredFilters: [],
  };
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

describe('PhysicalIndexUnionScan', () => {
  it('unions rowIds from multiple branches', () => {
    const searchResults = new Map([
      ['idx_age',  [1, 4]],   // age = 30 → Alice(1), Dave(4)
      ['idx_name', [3]],      // name = 'Carol' → Carol(3)
    ]);
    const rm = mockRowManager();
    const im = mockIndexManager(searchResults);

    const hint: IndexUnionHint = {
      kind: 'union',
      branches: [
        makeBranch(idxAge, [{
          columnPosition: 0,
          comparisonType: 'EQUAL',
          value: { expressionClass: BoundExpressionClass.BOUND_CONSTANT, value: 30, returnType: 'INTEGER' },
        }]),
        makeBranch(idxName, [{
          columnPosition: 0,
          comparisonType: 'EQUAL',
          value: { expressionClass: BoundExpressionClass.BOUND_CONSTANT, value: 'Carol', returnType: 'TEXT' },
        }]),
      ],
      originalFilter: makeOrFilter(),
    };

    const scan = new PhysicalIndexUnionScan(makeGet(), rm, im, hint, noopCtx);
    const result = drainOperator(scan);
    // rowIds sorted: [1, 3, 4], all pass the OR filter
    expect(result).toEqual([
      [1, 'Alice', 30],
      [3, 'Carol', 35],
      [4, 'Dave',  30],
    ]);
  });

  it('deduplicates overlapping rowIds', () => {
    // Both branches return rowId 1 (Alice: age=30, name='Alice')
    const searchResults = new Map([
      ['idx_age',  [1, 4]],   // age=30 → Alice(1), Dave(4)
      ['idx_name', [1]],      // name='Alice' → Alice(1)
    ]);
    const rm = mockRowManager();
    const im = mockIndexManager(searchResults);

    const hint: IndexUnionHint = {
      kind: 'union',
      branches: [
        makeBranch(idxAge, [{
          columnPosition: 0,
          comparisonType: 'EQUAL',
          value: { expressionClass: BoundExpressionClass.BOUND_CONSTANT, value: 30, returnType: 'INTEGER' },
        }]),
        makeBranch(idxName, [{
          columnPosition: 0,
          comparisonType: 'EQUAL',
          value: { expressionClass: BoundExpressionClass.BOUND_CONSTANT, value: 'Alice', returnType: 'TEXT' },
        }]),
      ],
      originalFilter: conjunction(
        'OR',
        comparison(colRef(0, 2, 'age', 'INTEGER'), constant(30), 'EQUAL'),
        comparison(colRef(0, 1, 'name', 'TEXT'), constant('Alice'), 'EQUAL'),
      ),
    };

    const scan = new PhysicalIndexUnionScan(makeGet(), rm, im, hint, noopCtx);
    const result = drainOperator(scan);
    // rowId 1 appears only once despite being in both branches
    const ids = result.map((t) => t[0]);
    expect(ids).toEqual([1, 4]);
  });

  it('returns sorted rowIds for deterministic order', () => {
    const searchResults = new Map([
      ['idx_age',  [4, 1]],
      ['idx_name', [3]],
    ]);
    const rm = mockRowManager();
    const im = mockIndexManager(searchResults);

    const hint: IndexUnionHint = {
      kind: 'union',
      branches: [
        makeBranch(idxAge, [{
          columnPosition: 0,
          comparisonType: 'EQUAL',
          value: { expressionClass: BoundExpressionClass.BOUND_CONSTANT, value: 30, returnType: 'INTEGER' },
        }]),
        makeBranch(idxName, [{
          columnPosition: 0,
          comparisonType: 'EQUAL',
          value: { expressionClass: BoundExpressionClass.BOUND_CONSTANT, value: 'Carol', returnType: 'TEXT' },
        }]),
      ],
      originalFilter: makeOrFilter(),
    };

    const scan = new PhysicalIndexUnionScan(makeGet(), rm, im, hint, noopCtx);
    const result = drainOperator(scan);
    const ids = result.map((t) => t[0]);
    expect(ids).toEqual([1, 3, 4]); // sorted
  });

  it('returns empty when all branches return no rowIds', () => {
    const searchResults = new Map([
      ['idx_age',  [] as number[]],
      ['idx_name', [] as number[]],
    ]);
    const rm = mockRowManager();
    const im = mockIndexManager(searchResults);

    const hint: IndexUnionHint = {
      kind: 'union',
      branches: [
        makeBranch(idxAge, [{
          columnPosition: 0,
          comparisonType: 'EQUAL',
          value: { expressionClass: BoundExpressionClass.BOUND_CONSTANT, value: 999, returnType: 'INTEGER' },
        }]),
        makeBranch(idxName, [{
          columnPosition: 0,
          comparisonType: 'EQUAL',
          value: { expressionClass: BoundExpressionClass.BOUND_CONSTANT, value: 'Nobody', returnType: 'TEXT' },
        }]),
      ],
      originalFilter: makeOrFilter(),
    };

    const scan = new PhysicalIndexUnionScan(makeGet(), rm, im, hint, noopCtx);
    expect(scan.next()).toBeNull();
  });

  it('skips null rows', () => {
    // rowId 99 doesn't exist
    const searchResults = new Map([
      ['idx_age',  [1, 99]],
      ['idx_name', [] as number[]],
    ]);
    const rm = mockRowManager();
    const im = mockIndexManager(searchResults);

    const hint: IndexUnionHint = {
      kind: 'union',
      branches: [
        makeBranch(idxAge, [{
          columnPosition: 0,
          comparisonType: 'EQUAL',
          value: { expressionClass: BoundExpressionClass.BOUND_CONSTANT, value: 30, returnType: 'INTEGER' },
        }]),
      ],
      originalFilter: comparison(colRef(0, 2, 'age', 'INTEGER'), constant(30), 'EQUAL'),
    };

    const scan = new PhysicalIndexUnionScan(makeGet(), rm, im, hint, noopCtx);
    const result = drainOperator(scan);
    expect(result).toEqual([[1, 'Alice', 30]]);
  });

  it('reset re-fetches union', () => {
    const searchResults = new Map([
      ['idx_age',  [1]],
      ['idx_name', [] as number[]],
    ]);
    const rm = mockRowManager();
    const im = mockIndexManager(searchResults);

    const hint: IndexUnionHint = {
      kind: 'union',
      branches: [
        makeBranch(idxAge, [{
          columnPosition: 0,
          comparisonType: 'EQUAL',
          value: { expressionClass: BoundExpressionClass.BOUND_CONSTANT, value: 30, returnType: 'INTEGER' },
        }]),
      ],
      originalFilter: comparison(colRef(0, 2, 'age', 'INTEGER'), constant(30), 'EQUAL'),
    };

    const scan = new PhysicalIndexUnionScan(makeGet(), rm, im, hint, noopCtx);
    const first = drainOperator(scan);
    expect(first).toHaveLength(1);

    scan.reset();
    const second = drainOperator(scan);
    expect(second).toEqual(first);
    expect(im.search).toHaveBeenCalledTimes(2);
  });

  it('resolves parameter-based predicate values', () => {
    const searchResults = new Map([
      ['idx_age', [2]],
    ]);
    const rm = mockRowManager();
    const im = mockIndexManager(searchResults);

    const hint: IndexUnionHint = {
      kind: 'union',
      branches: [
        makeBranch(idxAge, [{
          columnPosition: 0,
          comparisonType: 'EQUAL',
          value: { expressionClass: BoundExpressionClass.BOUND_PARAMETER, index: 0, returnType: 'INTEGER' },
        }]),
      ],
      originalFilter: comparison(colRef(0, 2, 'age', 'INTEGER'), constant(25), 'EQUAL'),
    };

    const ctx: SyncEvalContext = { ...noopCtx, params: [25] };
    const scan = new PhysicalIndexUnionScan(makeGet(), rm, im, hint, ctx);
    const result = drainOperator(scan);
    expect(result).toEqual([[2, 'Bob', 25]]);
    expect(im.search).toHaveBeenCalledWith('idx_age', [
      { columnPosition: 0, comparisonType: 'EQUAL', value: 25 },
    ]);
  });
});
