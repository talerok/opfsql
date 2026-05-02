import { describe, it, expect, vi } from 'vitest';
import type { LogicalGet, TableFilter } from '../../binder/types.js';
import { BoundExpressionClass, LogicalOperatorType } from '../../binder/types.js';
import type { SyncIRowManager, TableSchema } from '../../store/types.js';
import { PhysicalScan, PhysicalChildScan } from '../operators/scan.js';
import { drainOperator } from '../operators/utils.js';
import type { SyncEvalContext } from '../evaluate/context.js';
import { MockOperator, colRef, constant, layout, noopCtx } from './helpers.js';

// ---------------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------------

const usersSchema: TableSchema = {
  name: 'users',
  columns: [
    { name: 'id',   type: 'INTEGER', nullable: false, primaryKey: true,  unique: true,  autoIncrement: false, defaultValue: null },
    { name: 'name', type: 'TEXT',    nullable: false, primaryKey: false, unique: false, autoIncrement: false, defaultValue: null },
    { name: 'age',  type: 'INTEGER', nullable: true,  primaryKey: false, unique: false, autoIncrement: false, defaultValue: 0    },
  ],
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

function mockRowManager(rows: Array<{ rowId: number; row: Record<string, any> }>): SyncIRowManager {
  return {
    createTable: vi.fn(() => 0),
    prepareInsert: vi.fn(() => 0),
    prepareUpdate: vi.fn(() => 0),
    prepareDelete: vi.fn(),
    scanTable: vi.fn(() => rows),
    readRow: vi.fn(() => null),
    deleteTableData: vi.fn(),
  };
}

// ---------------------------------------------------------------------------
// PhysicalScan
// ---------------------------------------------------------------------------

describe('PhysicalScan', () => {
  it('returns all rows from storage', () => {
    const rows = [
      { rowId: 1, row: { id: 1, name: 'Alice', age: 30 } },
      { rowId: 2, row: { id: 2, name: 'Bob',   age: 25 } },
    ];
    const rm = mockRowManager(rows);
    const scan = new PhysicalScan(makeGet(), rm, noopCtx);
    const result = drainOperator(scan);
    expect(result).toEqual([
      [1, 'Alice', 30],
      [2, 'Bob',   25],
    ]);
  });

  it('returns empty for table with no rows', () => {
    const rm = mockRowManager([]);
    const scan = new PhysicalScan(makeGet(), rm, noopCtx);
    const result = drainOperator(scan);
    expect(result).toEqual([]);
  });

  it('__empty table returns one empty tuple', () => {
    const rm = mockRowManager([]);
    const get = makeGet({ tableName: '__empty' });
    const scan = new PhysicalScan(get, rm, noopCtx);
    const result = drainOperator(scan);
    expect(result).toEqual([[]]);
  });

  it('__empty table returns null on second call', () => {
    const rm = mockRowManager([]);
    const get = makeGet({ tableName: '__empty' });
    const scan = new PhysicalScan(get, rm, noopCtx);
    scan.next(); // consumes the single empty tuple
    expect(scan.next()).toBeNull();
  });

  it('applies tableFilters with constant', () => {
    const rows = [
      { rowId: 1, row: { id: 1, name: 'Alice', age: 30 } },
      { rowId: 2, row: { id: 2, name: 'Bob',   age: 25 } },
      { rowId: 3, row: { id: 3, name: 'Carol', age: 35 } },
    ];
    const rm = mockRowManager(rows);

    const filter: TableFilter = {
      expression: colRef(0, 2, 'age', 'INTEGER'),
      comparisonType: 'GREATER',
      constant: {
        expressionClass: BoundExpressionClass.BOUND_CONSTANT,
        value: 28,
        returnType: 'INTEGER',
      },
    };
    const get = makeGet({ tableFilters: [filter] });
    const scan = new PhysicalScan(get, rm, noopCtx);
    const result = drainOperator(scan);
    expect(result).toEqual([
      [1, 'Alice', 30],
      [3, 'Carol', 35],
    ]);
  });

  it('applies tableFilters with parameter', () => {
    const rows = [
      { rowId: 1, row: { id: 1, name: 'Alice', age: 30 } },
      { rowId: 2, row: { id: 2, name: 'Bob',   age: 25 } },
    ];
    const rm = mockRowManager(rows);

    const filter: TableFilter = {
      expression: colRef(0, 0, 'id', 'INTEGER'),
      comparisonType: 'EQUAL',
      constant: {
        expressionClass: BoundExpressionClass.BOUND_PARAMETER,
        index: 0,
        returnType: 'INTEGER',
      },
    };
    const get = makeGet({ tableFilters: [filter] });
    const ctx: SyncEvalContext = { ...noopCtx, params: [2] };
    const scan = new PhysicalScan(get, rm, ctx);
    const result = drainOperator(scan);
    expect(result).toEqual([[2, 'Bob', 25]]);
  });

  it('reset re-scans from beginning', () => {
    const rows = [
      { rowId: 1, row: { id: 1, name: 'Alice', age: 30 } },
    ];
    const rm = mockRowManager(rows);
    const scan = new PhysicalScan(makeGet(), rm, noopCtx);
    const first = drainOperator(scan);
    expect(first).toHaveLength(1);

    scan.reset();
    const second = drainOperator(scan);
    expect(second).toHaveLength(1);
    expect(second).toEqual(first);
  });

  it('uses default value when column missing from row', () => {
    const rows = [
      { rowId: 1, row: { id: 1, name: 'Alice' } }, // age missing, defaultValue=0
    ];
    const rm = mockRowManager(rows);
    const scan = new PhysicalScan(makeGet(), rm, noopCtx);
    const result = drainOperator(scan);
    expect(result).toEqual([[1, 'Alice', 0]]);
  });

  it('getLayout returns column bindings', () => {
    const rm = mockRowManager([]);
    const scan = new PhysicalScan(makeGet(), rm, noopCtx);
    expect(scan.getLayout()).toEqual([
      { tableIndex: 0, columnIndex: 0 },
      { tableIndex: 0, columnIndex: 1 },
      { tableIndex: 0, columnIndex: 2 },
    ]);
  });
});

// ---------------------------------------------------------------------------
// PhysicalChildScan
// ---------------------------------------------------------------------------

describe('PhysicalChildScan', () => {
  it('reads from child operator and remaps columns', () => {
    const child = new MockOperator(
      [[[10, 'hello', 99]]],
      layout([0, 0], [0, 1], [0, 2]),
    );
    // columnIds [2, 0] picks columns at positions 2 and 0 from child
    const get = makeGet({
      columnIds: [2, 0],
      schema: {
        ...usersSchema,
        columns: [
          // schema columns used for layout, but columnIds index into child tuple
          usersSchema.columns[2], // age
          usersSchema.columns[0], // id
        ],
      },
      types: ['INTEGER', 'INTEGER'],
      columnBindings: [
        { tableIndex: 0, columnIndex: 0 },
        { tableIndex: 0, columnIndex: 1 },
      ],
    });
    const scan = new PhysicalChildScan(get, child, noopCtx);
    const result = drainOperator(scan);
    expect(result).toEqual([[99, 10]]);
  });

  it('returns null for empty child', () => {
    const child = new MockOperator([], layout([0, 0]));
    const get = makeGet({
      columnIds: [0],
      types: ['INTEGER'],
      columnBindings: [{ tableIndex: 0, columnIndex: 0 }],
    });
    const scan = new PhysicalChildScan(get, child, noopCtx);
    expect(scan.next()).toBeNull();
  });

  it('applies tableFilters to remapped tuples', () => {
    const child = new MockOperator(
      [[[1, 'Alice'], [2, 'Bob'], [3, 'Carol']]],
      layout([0, 0], [0, 1]),
    );
    const filter: TableFilter = {
      expression: colRef(0, 0, 'id', 'INTEGER'),
      comparisonType: 'GREATER',
      constant: {
        expressionClass: BoundExpressionClass.BOUND_CONSTANT,
        value: 1,
        returnType: 'INTEGER',
      },
    };
    const get = makeGet({
      columnIds: [0, 1],
      tableFilters: [filter],
      types: ['INTEGER', 'TEXT'],
      columnBindings: [
        { tableIndex: 0, columnIndex: 0 },
        { tableIndex: 0, columnIndex: 1 },
      ],
    });
    const scan = new PhysicalChildScan(get, child, noopCtx);
    const result = drainOperator(scan);
    expect(result).toEqual([[2, 'Bob'], [3, 'Carol']]);
  });

  it('missing column index falls back to null', () => {
    const child = new MockOperator(
      [[[10, 'hello']]],
      layout([0, 0], [0, 1]),
    );
    // columnIds [0, 5] — column 5 doesn't exist in child tuple
    const get = makeGet({
      columnIds: [0, 5],
      types: ['INTEGER', 'TEXT'],
      columnBindings: [
        { tableIndex: 0, columnIndex: 0 },
        { tableIndex: 0, columnIndex: 1 },
      ],
    });
    const scan = new PhysicalChildScan(get, child, noopCtx);
    const result = drainOperator(scan);
    expect(result).toEqual([[10, null]]);
  });

  it('reset re-reads from child', () => {
    const child = new MockOperator(
      [[[1, 'a']]],
      layout([0, 0], [0, 1]),
    );
    const get = makeGet({
      columnIds: [0, 1],
      types: ['INTEGER', 'TEXT'],
      columnBindings: [
        { tableIndex: 0, columnIndex: 0 },
        { tableIndex: 0, columnIndex: 1 },
      ],
    });
    const scan = new PhysicalChildScan(get, child, noopCtx);
    const first = drainOperator(scan);
    scan.reset();
    const second = drainOperator(scan);
    expect(second).toEqual(first);
  });
});
