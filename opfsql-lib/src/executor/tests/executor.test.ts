import { describe, it, expect, vi } from 'vitest';
import type {
  LogicalGet,
  LogicalProjection,
  LogicalLimit,
} from '../../binder/types.js';
import { BoundExpressionClass, LogicalOperatorType } from '../../binder/types.js';
import type { SyncIRowManager, TableSchema, ICatalog } from '../../store/types.js';
import { execute } from '../executor.js';
import { colRef } from './helpers.js';

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

function mockRowManager(rows: Array<{ rowId: number; row: Record<string, any> }> = []): SyncIRowManager {
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

function mockCatalog(tables: TableSchema[] = [usersSchema]): ICatalog {
  const tableMap = new Map(tables.map((t) => [t.name, t]));
  const indexMap = new Map<string, any>();
  return {
    hasTable:       (n) => tableMap.has(n),
    getTable:       (n) => tableMap.get(n),
    addTable:       (s) => { tableMap.set(s.name, s); },
    removeTable:    (n) => { tableMap.delete(n); },
    updateTable:    (s) => { tableMap.set(s.name, s); },
    getAllTables:    () => [...tableMap.values()],
    hasIndex:       (n) => indexMap.has(n),
    getIndex:       (n) => indexMap.get(n),
    getTableIndexes:() => [],
    addIndex:       (i) => { indexMap.set(i.name, i); },
    removeIndex:    (n) => { indexMap.delete(n); },
    serialize:      () => ({ tables: [...tableMap.values()], indexes: [...indexMap.values()] }),
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

function makeProjection(child: LogicalGet): LogicalProjection {
  return {
    type: LogicalOperatorType.LOGICAL_PROJECTION,
    tableIndex: 1,
    children: [child],
    expressions: [colRef(0, 0, 'id', 'INTEGER'), colRef(0, 1, 'name', 'TEXT')],
    aliases: [null, null],
    types: ['INTEGER', 'TEXT'],
    estimatedCardinality: 100,
    getColumnBindings: () => [
      { tableIndex: 1, columnIndex: 0 },
      { tableIndex: 1, columnIndex: 1 },
    ],
  };
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

describe('execute', () => {
  it('SELECT returns rows with column names from projection', () => {
    const rows = [
      { rowId: 1, row: { id: 1, name: 'Alice' } },
      { rowId: 2, row: { id: 2, name: 'Bob' } },
    ];
    const rm = mockRowManager(rows);
    const catalog = mockCatalog();

    const plan = makeProjection(makeGet());
    const result = execute(plan, rm, catalog);

    expect(result.rows).toEqual([
      { id: 1, name: 'Alice' },
      { id: 2, name: 'Bob' },
    ]);
    expect(result.rowsAffected).toBe(0);
    expect(result.catalogChanges).toEqual([]);
  });

  it('SELECT without projection generates column0, column1 names', () => {
    const rows = [
      { rowId: 1, row: { id: 1, name: 'Alice' } },
    ];
    const rm = mockRowManager(rows);
    const catalog = mockCatalog();

    // Bare GET without projection
    const plan = makeGet();
    const result = execute(plan, rm, catalog);

    expect(result.rows).toEqual([
      { column0: 1, column1: 'Alice' },
    ]);
  });

  it('SELECT with aliased projection uses alias names', () => {
    const rows = [
      { rowId: 1, row: { id: 1, name: 'Alice' } },
    ];
    const rm = mockRowManager(rows);
    const catalog = mockCatalog();

    const proj = makeProjection(makeGet());
    proj.aliases = ['user_id', 'user_name'];
    const result = execute(proj, rm, catalog);

    expect(result.rows).toEqual([
      { user_id: 1, user_name: 'Alice' },
    ]);
  });

  it('SELECT with empty table returns no rows', () => {
    const rm = mockRowManager([]);
    const catalog = mockCatalog();
    const plan = makeProjection(makeGet());
    const result = execute(plan, rm, catalog);
    expect(result.rows).toEqual([]);
  });

  it('passes params to evaluation context', () => {
    const rows = [
      { rowId: 1, row: { id: 1, name: 'Alice' } },
      { rowId: 2, row: { id: 2, name: 'Bob' } },
    ];
    const rm = mockRowManager(rows);
    const catalog = mockCatalog();

    // Filter: id = $1 (param index 0)
    const get = makeGet({
      tableFilters: [{
        expression: colRef(0, 0, 'id', 'INTEGER'),
        comparisonType: 'EQUAL',
        constant: {
          expressionClass: BoundExpressionClass.BOUND_PARAMETER,
          index: 0,
          returnType: 'INTEGER',
        },
      }],
    });
    const plan = makeProjection(get);
    const result = execute(plan, rm, catalog, undefined, [2]);

    expect(result.rows).toEqual([{ id: 2, name: 'Bob' }]);
  });

  it('CREATE TABLE routes to DDL executor and returns catalog change', () => {
    const rm = mockRowManager();
    const catalog = mockCatalog([]);

    const createTable = {
      type: LogicalOperatorType.LOGICAL_CREATE_TABLE as const,
      children: [],
      expressions: [],
      types: [],
      estimatedCardinality: 0,
      getColumnBindings: () => [],
      schema: usersSchema,
      ifNotExists: false,
    };

    const result = execute(createTable as any, rm, catalog);
    expect(result.catalogChanges).toHaveLength(1);
    expect(result.catalogChanges[0].type).toBe('CREATE_TABLE');
    expect(result.rowsAffected).toBe(0);
  });
});
