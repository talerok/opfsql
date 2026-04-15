import { describe, expect, it, vi } from 'vitest';
import type {
  LogicalAlterTable,
  LogicalCreateIndex,
  LogicalCreateTable,
  LogicalDelete,
  LogicalDrop,
  LogicalFilter,
  LogicalGet,
  LogicalInsert,
  LogicalUpdate,
} from '../../binder/types.js';
import { LogicalOperatorType } from '../../binder/types.js';
import type {
  ICatalog,
  SyncIIndexManager,
  SyncIRowManager,
  Row,
  RowId,
  TableSchema,
} from '../../store/types.js';
import {
  executeAlterTable,
  executeCreateIndex,
  executeCreateTable,
  executeDrop,
} from '../ddl/index.js';
import { executeDelete, executeInsert, executeUpdate } from '../dml/index.js';
import { colRef, comparison, constant, noopCtx } from './helpers.js';

// ---------------------------------------------------------------------------
// Mock catalog
// ---------------------------------------------------------------------------

const usersSchema: TableSchema = {
  name: 'users',
  columns: [
    { name: 'id',   type: 'INTEGER', nullable: false, primaryKey: true,  unique: true,  autoIncrement: false, defaultValue: null },
    { name: 'name', type: 'TEXT',    nullable: false, primaryKey: false, unique: false, autoIncrement: false, defaultValue: null },
    { name: 'age',  type: 'INTEGER', nullable: true,  primaryKey: false, unique: false, autoIncrement: false, defaultValue: 0    },
  ],
};

function mockCatalog(tables: TableSchema[] = [usersSchema]): ICatalog {
  const tableMap = new Map(tables.map((t) => [t.name, t]));
  const indexMap = new Map<string, any>();
  return {
    hasTable:       (n) => tableMap.has(n),
    getTable:       (n) => tableMap.get(n),
    addTable:       vi.fn(),
    removeTable:    vi.fn(),
    updateTable:    vi.fn((s: TableSchema) => { tableMap.set(s.name, s); }),
    getAllTables:   () => [...tableMap.values()],
    hasIndex:       (n) => indexMap.has(n),
    getIndex:       (n) => indexMap.get(n),
    getTableIndexes: () => [],
    addIndex:       vi.fn(),
    removeIndex:    vi.fn(),
    serialize:      () => ({ tables: [...tableMap.values()], indexes: [] }),
  };
}

// ---------------------------------------------------------------------------
// DDL Tests
// ---------------------------------------------------------------------------

describe('DDL executors', () => {
  describe('executeCreateTable', () => {
    it('returns CREATE_TABLE catalog change', () => {
      const catalog = mockCatalog([]);
      const rm = mockRowManager();
      const op = {
        type: LogicalOperatorType.LOGICAL_CREATE_TABLE,
        schema: usersSchema,
        ifNotExists: false,
        children: [],
        expressions: [],
        types: [],
        estimatedCardinality: 0,
        getColumnBindings: () => [],
      } as unknown as LogicalCreateTable;

      const result = executeCreateTable(op, catalog, rm);
      expect(result.catalogChanges).toHaveLength(1);
      expect(result.catalogChanges[0].type).toBe('CREATE_TABLE');
    });

    it('throws when table already exists', () => {
      const catalog = mockCatalog([usersSchema]);
      const rm = mockRowManager();
      const op = {
        type: LogicalOperatorType.LOGICAL_CREATE_TABLE,
        schema: usersSchema,
        ifNotExists: false,
        children: [],
        expressions: [],
        types: [],
        estimatedCardinality: 0,
        getColumnBindings: () => [],
      } as unknown as LogicalCreateTable;

      expect(() => executeCreateTable(op, catalog, rm)).toThrow('already exists');
    });

    it('IF NOT EXISTS — no-op when exists', () => {
      const catalog = mockCatalog([usersSchema]);
      const rm = mockRowManager();
      const op = {
        type: LogicalOperatorType.LOGICAL_CREATE_TABLE,
        schema: usersSchema,
        ifNotExists: true,
        children: [],
        expressions: [],
        types: [],
        estimatedCardinality: 0,
        getColumnBindings: () => [],
      } as unknown as LogicalCreateTable;

      const result = executeCreateTable(op, catalog, rm);
      expect(result.catalogChanges).toHaveLength(0);
    });
  });

  describe('executeCreateIndex', () => {
    it('returns CREATE_INDEX catalog change', () => {
      const catalog = mockCatalog();
      const rm = mockRowManager();
      const im = mockIndexManager();
      const idx = {
        name: 'idx_users_name',
        tableName: 'users',
        columns: ['name'],
        unique: false,
      };
      const op = {
        type: LogicalOperatorType.LOGICAL_CREATE_INDEX,
        index: idx,
        ifNotExists: false,
        children: [],
        expressions: [],
        types: [],
        estimatedCardinality: 0,
        getColumnBindings: () => [],
      } as unknown as LogicalCreateIndex;

      const result = executeCreateIndex(op, catalog, rm, im);
      expect(result.catalogChanges[0].type).toBe('CREATE_INDEX');
      // metaPageNo should be set from bulkLoad return
      const indexChange = result.catalogChanges[0] as any;
      expect(indexChange.index.metaPageNo).toBe(100);
    });
  });

  describe('executeAlterTable', () => {
    it('ADD COLUMN', () => {
      const catalog = mockCatalog();
      const newCol = {
        name: 'email',
        type: 'TEXT' as const,
        nullable: true,
        primaryKey: false,
        unique: false,
        autoIncrement: false,
        defaultValue: null,
      };
      const op = {
        type: LogicalOperatorType.LOGICAL_ALTER_TABLE,
        tableName: 'users',
        action: { type: 'ADD_COLUMN', column: newCol },
        children: [],
        expressions: [],
        types: [],
        estimatedCardinality: 0,
        getColumnBindings: () => [],
      } as unknown as LogicalAlterTable;

      const result = executeAlterTable(op, catalog);
      const change = result.catalogChanges[0] as any;
      expect(change.type).toBe('ALTER_TABLE');
      expect(change.after.columns).toHaveLength(4);
    });

    it('DROP COLUMN', () => {
      const catalog = mockCatalog();
      const op = {
        type: LogicalOperatorType.LOGICAL_ALTER_TABLE,
        tableName: 'users',
        action: { type: 'DROP_COLUMN', columnName: 'age' },
        children: [],
        expressions: [],
        types: [],
        estimatedCardinality: 0,
        getColumnBindings: () => [],
      } as unknown as LogicalAlterTable;

      const result = executeAlterTable(op, catalog);
      const change = result.catalogChanges[0] as any;
      expect(change.after.columns).toHaveLength(2);
      expect(change.after.columns.map((c: any) => c.name)).toEqual(['id', 'name']);
    });

    it('throws if table not found', () => {
      const catalog = mockCatalog([]);
      const op = {
        type: LogicalOperatorType.LOGICAL_ALTER_TABLE,
        tableName: 'nope',
        action: { type: 'ADD_COLUMN', column: {} as any },
        children: [],
        expressions: [],
        types: [],
        estimatedCardinality: 0,
        getColumnBindings: () => [],
      } as unknown as LogicalAlterTable;

      expect(() => executeAlterTable(op, catalog)).toThrow('not found');
    });
  });

  describe('executeDrop', () => {
    it('DROP TABLE — deletes data via rowManager', () => {
      const catalog = mockCatalog();
      const rm = mockRowManager();
      const op = {
        type: LogicalOperatorType.LOGICAL_DROP,
        dropType: 'TABLE',
        name: 'users',
        ifExists: false,
        children: [],
        expressions: [],
        types: [],
        estimatedCardinality: 0,
        getColumnBindings: () => [],
      } as unknown as LogicalDrop;

      const result = executeDrop(op, catalog, rm, mockIndexManager());
      expect(result.catalogChanges[0].type).toBe('DROP_TABLE');
      expect(rm.deleteTableData).toHaveBeenCalledWith('users');
    });

    it('DROP TABLE IF EXISTS — no-op when missing', () => {
      const catalog = mockCatalog([]);
      const rm = mockRowManager();
      const op = {
        type: LogicalOperatorType.LOGICAL_DROP,
        dropType: 'TABLE',
        name: 'nope',
        ifExists: true,
        children: [],
        expressions: [],
        types: [],
        estimatedCardinality: 0,
        getColumnBindings: () => [],
      } as unknown as LogicalDrop;

      const result = executeDrop(op, catalog, rm, mockIndexManager());
      expect(result.catalogChanges).toHaveLength(0);
    });

    it('DROP TABLE — throws if not found and no IF EXISTS', () => {
      const catalog = mockCatalog([]);
      const rm = mockRowManager();
      const op = {
        type: LogicalOperatorType.LOGICAL_DROP,
        dropType: 'TABLE',
        name: 'nope',
        ifExists: false,
        children: [],
        expressions: [],
        types: [],
        estimatedCardinality: 0,
        getColumnBindings: () => [],
      } as unknown as LogicalDrop;

      expect(() => executeDrop(op, catalog, rm, mockIndexManager())).toThrow('not found');
    });
  });
});

// ---------------------------------------------------------------------------
// DML Tests
// ---------------------------------------------------------------------------

function mockRowManager(
  rows: Array<{ id: number; name: string; age: number | null }> = [],
): SyncIRowManager {
  const stored = rows.map((r, i) => ({
    rowId: i as RowId,
    row: r as unknown as Row,
  }));

  let nextId = stored.length;
  return {
    scanTable: function* () {
      for (const entry of stored) yield entry;
    },
    prepareInsert:    vi.fn((): RowId => nextId++),
    prepareUpdate:    vi.fn((_t: string, rowId: RowId): RowId => rowId),
    prepareDelete:    vi.fn(() => {}),
    readRow:          vi.fn(() => null),
    deleteTableData:  vi.fn(() => {}),
    createTable:      vi.fn((): number => 42),
  };
}

function mockIndexManager(): SyncIIndexManager {
  return {
    insert:    vi.fn(() => {}),
    delete:    vi.fn(() => {}),
    search:    vi.fn(() => []),
    bulkLoad:  vi.fn((): number => 100),
    dropIndex: vi.fn(() => {}),
  };
}

function makeGet(tableIndex = 0): LogicalGet {
  return {
    type: LogicalOperatorType.LOGICAL_GET,
    children: [],
    expressions: [],
    types: ['INTEGER', 'TEXT', 'INTEGER'],
    estimatedCardinality: 0,
    tableIndex,
    tableName: 'users',
    schema: usersSchema,
    columnIds: [0, 1, 2],
    tableFilters: [],
    getColumnBindings: () => [
      { tableIndex, columnIndex: 0 },
      { tableIndex, columnIndex: 1 },
      { tableIndex, columnIndex: 2 },
    ],
  } as LogicalGet;
}

describe('DML executors', () => {
  describe('executeInsert', () => {
    it('INSERT VALUES — single row', () => {
      const rm = mockRowManager();
      const op = {
        type: LogicalOperatorType.LOGICAL_INSERT,
        tableName: 'users',
        schema: usersSchema,
        columns: [0, 1, 2],
        children: [],
        expressions: [constant(1), constant('Alice'), constant(30)],
        types: [],
        estimatedCardinality: 0,
        getColumnBindings: () => [],
      } as unknown as LogicalInsert;

      const result = executeInsert(op, rm, noopCtx);
      expect(result.rowsAffected).toBe(1);
      expect(rm.prepareInsert).toHaveBeenCalledTimes(1);
      expect(rm.prepareInsert).toHaveBeenCalledWith('users', { id: 1, name: 'Alice', age: 30 });
    });

    it('INSERT VALUES — multi-row', () => {
      const rm = mockRowManager();
      const op = {
        type: LogicalOperatorType.LOGICAL_INSERT,
        tableName: 'users',
        schema: usersSchema,
        columns: [0, 1],
        children: [],
        expressions: [constant(1), constant('Alice'), constant(2), constant('Bob')],
        types: [],
        estimatedCardinality: 0,
        getColumnBindings: () => [],
      } as unknown as LogicalInsert;

      const result = executeInsert(op, rm, noopCtx);
      expect(result.rowsAffected).toBe(2);
      expect(rm.prepareInsert).toHaveBeenCalledTimes(2);
    });

    it('INSERT VALUES — uses defaults for missing columns', () => {
      const rm = mockRowManager();
      const op = {
        type: LogicalOperatorType.LOGICAL_INSERT,
        tableName: 'users',
        schema: usersSchema,
        columns: [0, 1], // no age column
        children: [],
        expressions: [constant(1), constant('Alice')],
        types: [],
        estimatedCardinality: 0,
        getColumnBindings: () => [],
      } as unknown as LogicalInsert;

      executeInsert(op, rm, noopCtx);
      expect(rm.prepareInsert).toHaveBeenCalledWith('users', { id: 1, name: 'Alice', age: 0 });
    });
  });

  describe('executeUpdate', () => {
    it('updates matching rows', () => {
      const rm = mockRowManager([
        { id: 1, name: 'Alice', age: 25 },
        { id: 2, name: 'Bob', age: 30 },
      ]);
      const get = makeGet();
      const op = {
        type: LogicalOperatorType.LOGICAL_UPDATE,
        tableName: 'users',
        schema: usersSchema,
        children: [get],
        updateColumns: [2],
        expressions: [constant(99)],
        types: [],
        estimatedCardinality: 0,
        getColumnBindings: () => [],
      } as unknown as LogicalUpdate;

      const result = executeUpdate(op, rm, noopCtx);
      expect(result.rowsAffected).toBe(2);
      expect(rm.prepareUpdate).toHaveBeenCalledTimes(2);
    });

    it('UPDATE with WHERE filter', () => {
      const rm = mockRowManager([
        { id: 1, name: 'Alice', age: 25 },
        { id: 2, name: 'Bob', age: 30 },
      ]);
      const get = makeGet();
      const filter: LogicalFilter = {
        type: LogicalOperatorType.LOGICAL_FILTER,
        children: [get],
        expressions: [comparison(colRef(0, 0), constant(1), 'EQUAL')],
        types: ['INTEGER', 'TEXT', 'INTEGER'],
        estimatedCardinality: 0,
        getColumnBindings: () => get.getColumnBindings(),
      };
      const op = {
        type: LogicalOperatorType.LOGICAL_UPDATE,
        tableName: 'users',
        schema: usersSchema,
        children: [filter],
        updateColumns: [1],
        expressions: [constant('UPDATED')],
        types: [],
        estimatedCardinality: 0,
        getColumnBindings: () => [],
      } as unknown as LogicalUpdate;

      const result = executeUpdate(op, rm, noopCtx);
      expect(result.rowsAffected).toBe(1);
      expect(rm.prepareUpdate).toHaveBeenCalledWith(
        'users',
        0,
        expect.objectContaining({ name: 'UPDATED' }),
      );
    });
  });

  describe('executeDelete', () => {
    it('deletes all rows (no filter)', () => {
      const rm = mockRowManager([
        { id: 1, name: 'Alice', age: 25 },
        { id: 2, name: 'Bob', age: 30 },
      ]);
      const get = makeGet();
      const op = {
        type: LogicalOperatorType.LOGICAL_DELETE,
        tableName: 'users',
        schema: usersSchema,
        children: [get],
        expressions: [],
        types: [],
        estimatedCardinality: 0,
        getColumnBindings: () => [],
      } as unknown as LogicalDelete;

      const result = executeDelete(op, rm, noopCtx);
      expect(result.rowsAffected).toBe(2);
      expect(rm.prepareDelete).toHaveBeenCalledTimes(2);
    });

    it('deletes with WHERE filter', () => {
      const rm = mockRowManager([
        { id: 1, name: 'Alice', age: 25 },
        { id: 2, name: 'Bob', age: 30 },
      ]);
      const get = makeGet();
      const filter: LogicalFilter = {
        type: LogicalOperatorType.LOGICAL_FILTER,
        children: [get],
        expressions: [comparison(colRef(0, 0), constant(2), 'EQUAL')],
        types: ['INTEGER', 'TEXT', 'INTEGER'],
        estimatedCardinality: 0,
        getColumnBindings: () => get.getColumnBindings(),
      };
      const op = {
        type: LogicalOperatorType.LOGICAL_DELETE,
        tableName: 'users',
        schema: usersSchema,
        children: [filter],
        expressions: [],
        types: [],
        estimatedCardinality: 0,
        getColumnBindings: () => [],
      } as unknown as LogicalDelete;

      const result = executeDelete(op, rm, noopCtx);
      expect(result.rowsAffected).toBe(1);
      expect(rm.prepareDelete).toHaveBeenCalledWith('users', 1);
    });
  });

  describe('executeInsert with ON CONFLICT', () => {
    it('DO NOTHING — skips conflicting row', () => {
      const rm = mockRowManager([
        { id: 1, name: 'Alice', age: 25 },
      ]);
      const op = {
        type: LogicalOperatorType.LOGICAL_INSERT,
        tableName: 'users',
        schema: usersSchema,
        columns: [0, 1, 2],
        children: [],
        expressions: [constant(1), constant('Updated'), constant(30)],
        types: [],
        estimatedCardinality: 0,
        getColumnBindings: () => [],
        onConflict: {
          conflictColumns: [0],
          action: 'NOTHING' as const,
          updateColumns: [],
          updateExpressions: [],
          whereExpression: null,
          targetTableIndex: -1,
          excludedTableIndex: -1,
        },
      } as unknown as LogicalInsert;

      const result = executeInsert(op, rm, noopCtx);
      expect(result.rowsAffected).toBe(0);
      expect(rm.prepareInsert).not.toHaveBeenCalled();
    });

    it('DO NOTHING — inserts when no conflict', () => {
      const rm = mockRowManager([
        { id: 1, name: 'Alice', age: 25 },
      ]);
      const op = {
        type: LogicalOperatorType.LOGICAL_INSERT,
        tableName: 'users',
        schema: usersSchema,
        columns: [0, 1, 2],
        children: [],
        expressions: [constant(2), constant('Bob'), constant(30)],
        types: [],
        estimatedCardinality: 0,
        getColumnBindings: () => [],
        onConflict: {
          conflictColumns: [0],
          action: 'NOTHING' as const,
          updateColumns: [],
          updateExpressions: [],
          whereExpression: null,
          targetTableIndex: -1,
          excludedTableIndex: -1,
        },
      } as unknown as LogicalInsert;

      const result = executeInsert(op, rm, noopCtx);
      expect(result.rowsAffected).toBe(1);
      expect(rm.prepareInsert).toHaveBeenCalledTimes(1);
    });

    it('DO UPDATE SET — updates conflicting row', () => {
      const rm = mockRowManager([
        { id: 1, name: 'Alice', age: 25 },
      ]);
      const TARGET_TI = 10;
      const EXCLUDED_TI = 11;
      const op = {
        type: LogicalOperatorType.LOGICAL_INSERT,
        tableName: 'users',
        schema: usersSchema,
        columns: [0, 1, 2],
        children: [],
        expressions: [constant(1), constant('AliceNew'), constant(30)],
        types: [],
        estimatedCardinality: 0,
        getColumnBindings: () => [],
        onConflict: {
          conflictColumns: [0],
          action: 'UPDATE' as const,
          updateColumns: [1],
          updateExpressions: [
            colRef(EXCLUDED_TI, 1, 'name', 'TEXT'),
          ],
          whereExpression: null,
          targetTableIndex: TARGET_TI,
          excludedTableIndex: EXCLUDED_TI,
        },
      } as unknown as LogicalInsert;

      const result = executeInsert(op, rm, noopCtx);
      expect(result.rowsAffected).toBe(1);
      expect(rm.prepareInsert).not.toHaveBeenCalled();
      expect(rm.prepareUpdate).toHaveBeenCalledWith(
        'users',
        0,
        expect.objectContaining({ id: 1, name: 'AliceNew', age: 25 }),
      );
    });

    it('DO UPDATE with WHERE — skips update when WHERE is false', () => {
      const rm = mockRowManager([
        { id: 1, name: 'Alice', age: 25 },
      ]);
      const TARGET_TI = 10;
      const EXCLUDED_TI = 11;
      const op = {
        type: LogicalOperatorType.LOGICAL_INSERT,
        tableName: 'users',
        schema: usersSchema,
        columns: [0, 1, 2],
        children: [],
        expressions: [constant(1), constant('AliceNew'), constant(20)],
        types: [],
        estimatedCardinality: 0,
        getColumnBindings: () => [],
        onConflict: {
          conflictColumns: [0],
          action: 'UPDATE' as const,
          updateColumns: [2],
          updateExpressions: [colRef(EXCLUDED_TI, 2, 'age', 'INTEGER')],
          whereExpression: comparison(
            colRef(EXCLUDED_TI, 2, 'age', 'INTEGER'),
            colRef(TARGET_TI, 2, 'age', 'INTEGER'),
            'GREATER',
          ),
          targetTableIndex: TARGET_TI,
          excludedTableIndex: EXCLUDED_TI,
        },
      } as unknown as LogicalInsert;

      const result = executeInsert(op, rm, noopCtx);
      expect(result.rowsAffected).toBe(0);
      expect(rm.prepareUpdate).not.toHaveBeenCalled();
    });

    it('DO UPDATE with WHERE — applies update when WHERE is true', () => {
      const rm = mockRowManager([
        { id: 1, name: 'Alice', age: 25 },
      ]);
      const TARGET_TI = 10;
      const EXCLUDED_TI = 11;
      const op = {
        type: LogicalOperatorType.LOGICAL_INSERT,
        tableName: 'users',
        schema: usersSchema,
        columns: [0, 1, 2],
        children: [],
        expressions: [constant(1), constant('AliceNew'), constant(50)],
        types: [],
        estimatedCardinality: 0,
        getColumnBindings: () => [],
        onConflict: {
          conflictColumns: [0],
          action: 'UPDATE' as const,
          updateColumns: [2],
          updateExpressions: [colRef(EXCLUDED_TI, 2, 'age', 'INTEGER')],
          whereExpression: comparison(
            colRef(EXCLUDED_TI, 2, 'age', 'INTEGER'),
            colRef(TARGET_TI, 2, 'age', 'INTEGER'),
            'GREATER',
          ),
          targetTableIndex: TARGET_TI,
          excludedTableIndex: EXCLUDED_TI,
        },
      } as unknown as LogicalInsert;

      const result = executeInsert(op, rm, noopCtx);
      expect(result.rowsAffected).toBe(1);
      expect(rm.prepareUpdate).toHaveBeenCalledWith(
        'users',
        0,
        expect.objectContaining({ id: 1, name: 'Alice', age: 50 }),
      );
    });

    it('multi-row insert with mixed conflicts and non-conflicts', () => {
      const rm = mockRowManager([
        { id: 1, name: 'Alice', age: 25 },
      ]);
      const op = {
        type: LogicalOperatorType.LOGICAL_INSERT,
        tableName: 'users',
        schema: usersSchema,
        columns: [0, 1, 2],
        children: [],
        expressions: [
          constant(1), constant('AliceNew'), constant(30),
          constant(2), constant('Bob'), constant(35),
        ],
        types: [],
        estimatedCardinality: 0,
        getColumnBindings: () => [],
        onConflict: {
          conflictColumns: [0],
          action: 'NOTHING' as const,
          updateColumns: [],
          updateExpressions: [],
          whereExpression: null,
          targetTableIndex: -1,
          excludedTableIndex: -1,
        },
      } as unknown as LogicalInsert;

      const result = executeInsert(op, rm, noopCtx);
      expect(result.rowsAffected).toBe(1);
      expect(rm.prepareInsert).toHaveBeenCalledTimes(1);
      expect(rm.prepareInsert).toHaveBeenCalledWith(
        'users',
        expect.objectContaining({ id: 2, name: 'Bob' }),
      );
    });

    it('no onConflict — normal insert preserved', () => {
      const rm = mockRowManager();
      const op = {
        type: LogicalOperatorType.LOGICAL_INSERT,
        tableName: 'users',
        schema: usersSchema,
        columns: [0, 1, 2],
        children: [],
        expressions: [constant(1), constant('Alice'), constant(30)],
        types: [],
        estimatedCardinality: 0,
        getColumnBindings: () => [],
      } as unknown as LogicalInsert;

      const result = executeInsert(op, rm, noopCtx);
      expect(result.rowsAffected).toBe(1);
      expect(rm.prepareInsert).toHaveBeenCalledTimes(1);
    });
  });
});

// ---------------------------------------------------------------------------
// PK index — CREATE TABLE produces __pk_ index, INSERT calls indexManager
// ---------------------------------------------------------------------------

describe('PK index via executeCreateTable', () => {
  it('CREATE TABLE with PK columns emits __pk_ index with unique=true', () => {
    const catalog = mockCatalog([]);
    const rm = mockRowManager();
    const im = mockIndexManager();
    const op = {
      type: LogicalOperatorType.LOGICAL_CREATE_TABLE,
      schema: usersSchema,
      ifNotExists: false,
      children: [],
      expressions: [],
      types: [],
      estimatedCardinality: 0,
      getColumnBindings: () => [],
    } as unknown as LogicalCreateTable;

    const result = executeCreateTable(op, catalog, rm, im);
    expect(result.catalogChanges).toHaveLength(2);
    const idxChange = result.catalogChanges[1] as any;
    expect(idxChange.type).toBe('CREATE_INDEX');
    expect(idxChange.index.name).toBe('__pk_users');
    expect(idxChange.index.unique).toBe(true);
    expect(idxChange.index.columns).toEqual(['id']);
    expect(idxChange.index.metaPageNo).toBe(100);
  });

  it('CREATE TABLE without PK does not emit __pk_ index', () => {
    const noPkSchema: TableSchema = {
      name: 'logs',
      columns: [
        { name: 'msg', type: 'TEXT', nullable: true, primaryKey: false, unique: false, autoIncrement: false, defaultValue: null },
      ],
    };
    const catalog = mockCatalog([]);
    const rm = mockRowManager();
    const im = mockIndexManager();
    const op = {
      type: LogicalOperatorType.LOGICAL_CREATE_TABLE,
      schema: noPkSchema,
      ifNotExists: false,
      children: [],
      expressions: [],
      types: [],
      estimatedCardinality: 0,
      getColumnBindings: () => [],
    } as unknown as LogicalCreateTable;

    const result = executeCreateTable(op, catalog, rm, im);
    expect(result.catalogChanges).toHaveLength(1);
    expect(result.catalogChanges[0].type).toBe('CREATE_TABLE');
  });

  it('INSERT with indexManager calls indexManager.insert for each index', () => {
    const rm = mockRowManager();
    const im = mockIndexManager();
    const pkIdx = {
      name: '__pk_users',
      tableName: 'users',
      columns: ['id'],
      unique: true,
      metaPageNo: 100,
    };
    const catalog = mockCatalog([usersSchema]);
    // Override getTableIndexes to return the PK index
    (catalog as any).getTableIndexes = () => [pkIdx];

    const op = {
      type: LogicalOperatorType.LOGICAL_INSERT,
      tableName: 'users',
      schema: usersSchema,
      columns: [0, 1, 2],
      children: [],
      expressions: [constant(1), constant('Alice'), constant(30)],
      types: [],
      estimatedCardinality: 0,
      getColumnBindings: () => [],
    } as unknown as LogicalInsert;

    executeInsert(op, rm, noopCtx, catalog, im);
    expect(im.insert).toHaveBeenCalledTimes(1);
    expect(im.insert).toHaveBeenCalledWith('__pk_users', [1], expect.anything());
  });
});

// ---------------------------------------------------------------------------
// AUTOINCREMENT Tests
// ---------------------------------------------------------------------------

function freshAutoSchema(seq?: number): TableSchema {
  return {
    name: 'items',
    columns: [
      { name: 'id',   type: 'INTEGER', nullable: false, primaryKey: true,  unique: true,  autoIncrement: true, defaultValue: null },
      { name: 'name', type: 'TEXT',    nullable: false, primaryKey: false, unique: false, autoIncrement: false, defaultValue: null },
    ],
    autoIncrementSeq: seq,
  };
}

describe('AUTOINCREMENT', () => {
  it('auto-generates id = 1 on empty table when column is null', () => {
    const rm = mockRowManager();
    const schema = freshAutoSchema();
    const catalog = mockCatalog([schema]);
    const op = {
      type: LogicalOperatorType.LOGICAL_INSERT,
      tableName: 'items',
      schema,
      columns: [1],
      children: [],
      expressions: [constant('Alice')],
      types: [],
      estimatedCardinality: 0,
      getColumnBindings: () => [],
    } as unknown as LogicalInsert;

    const result = executeInsert(op, rm, noopCtx, catalog);
    expect(result.rowsAffected).toBe(1);
    expect(rm.prepareInsert).toHaveBeenCalledWith(
      'items',
      expect.objectContaining({ id: 1, name: 'Alice' }),
    );
    expect(catalog.getTable('items')!.autoIncrementSeq).toBe(1);
  });

  it('auto-generates sequential ids across multiple inserts', () => {
    const rm = mockRowManager();
    const schema = freshAutoSchema();
    const catalog = mockCatalog([schema]);
    const op = {
      type: LogicalOperatorType.LOGICAL_INSERT,
      tableName: 'items',
      schema,
      columns: [1],
      children: [],
      expressions: [constant('Alice'), constant('Bob'), constant('Charlie')],
      types: [],
      estimatedCardinality: 0,
      getColumnBindings: () => [],
    } as unknown as LogicalInsert;

    const result = executeInsert(op, rm, noopCtx, catalog);
    expect(result.rowsAffected).toBe(3);
    expect(rm.prepareInsert).toHaveBeenCalledWith(
      'items',
      expect.objectContaining({ id: 1 }),
    );
    expect(rm.prepareInsert).toHaveBeenCalledWith(
      'items',
      expect.objectContaining({ id: 2 }),
    );
    expect(rm.prepareInsert).toHaveBeenCalledWith(
      'items',
      expect.objectContaining({ id: 3 }),
    );
    expect(catalog.getTable('items')!.autoIncrementSeq).toBe(3);
  });

  it('uses explicit value and updates seq', () => {
    const rm = mockRowManager();
    const schema = freshAutoSchema();
    const catalog = mockCatalog([schema]);
    const op = {
      type: LogicalOperatorType.LOGICAL_INSERT,
      tableName: 'items',
      schema,
      columns: [0, 1],
      children: [],
      expressions: [constant(42), constant('Alice')],
      types: [],
      estimatedCardinality: 0,
      getColumnBindings: () => [],
    } as unknown as LogicalInsert;

    const result = executeInsert(op, rm, noopCtx, catalog);
    expect(result.rowsAffected).toBe(1);
    expect(rm.prepareInsert).toHaveBeenCalledWith(
      'items',
      expect.objectContaining({ id: 42, name: 'Alice' }),
    );
    expect(catalog.getTable('items')!.autoIncrementSeq).toBe(42);
  });

  it('next auto value is seq + 1 after explicit insert', () => {
    const rm = mockRowManager();
    const schema = freshAutoSchema(10);
    const catalog = mockCatalog([schema]);
    const op = {
      type: LogicalOperatorType.LOGICAL_INSERT,
      tableName: 'items',
      schema,
      columns: [1],
      children: [],
      expressions: [constant('Dave')],
      types: [],
      estimatedCardinality: 0,
      getColumnBindings: () => [],
    } as unknown as LogicalInsert;

    const result = executeInsert(op, rm, noopCtx, catalog);
    expect(result.rowsAffected).toBe(1);
    expect(rm.prepareInsert).toHaveBeenCalledWith(
      'items',
      expect.objectContaining({ id: 11, name: 'Dave' }),
    );
    expect(catalog.getTable('items')!.autoIncrementSeq).toBe(11);
  });

  it('NULL explicit value triggers auto-generation', () => {
    const rm = mockRowManager();
    const schema = freshAutoSchema();
    const catalog = mockCatalog([schema]);
    const op = {
      type: LogicalOperatorType.LOGICAL_INSERT,
      tableName: 'items',
      schema,
      columns: [0, 1],
      children: [],
      expressions: [constant(null), constant('Alice')],
      types: [],
      estimatedCardinality: 0,
      getColumnBindings: () => [],
    } as unknown as LogicalInsert;

    const result = executeInsert(op, rm, noopCtx, catalog);
    expect(result.rowsAffected).toBe(1);
    expect(rm.prepareInsert).toHaveBeenCalledWith(
      'items',
      expect.objectContaining({ id: 1, name: 'Alice' }),
    );
  });

  it('sets catalogDirty when autoincrement is used', () => {
    const rm = mockRowManager();
    const schema = freshAutoSchema();
    const catalog = mockCatalog([schema]);
    const op = {
      type: LogicalOperatorType.LOGICAL_INSERT,
      tableName: 'items',
      schema,
      columns: [1],
      children: [],
      expressions: [constant('Alice')],
      types: [],
      estimatedCardinality: 0,
      getColumnBindings: () => [],
    } as unknown as LogicalInsert;

    const result = executeInsert(op, rm, noopCtx, catalog);
    expect(result.catalogDirty).toBe(true);
  });
});

// ---------------------------------------------------------------------------
// DML with multi-expression filter (extractConditions)
// ---------------------------------------------------------------------------

describe('DML with multi-expression filter', () => {
  it('DELETE with filter containing two expressions applies both', () => {
    const rm = mockRowManager([
      { id: 1, name: 'Alice', age: 25 },
      { id: 2, name: 'Bob', age: 30 },
      { id: 3, name: 'Charlie', age: 35 },
    ]);
    const get = makeGet();
    const filter: LogicalFilter = {
      type: LogicalOperatorType.LOGICAL_FILTER,
      children: [get],
      expressions: [
        comparison(colRef(0, 2), constant(20), 'GREATER'),
        comparison(colRef(0, 2), constant(30), 'LESS'),
      ],
      types: ['INTEGER', 'TEXT', 'INTEGER'],
      estimatedCardinality: 0,
      getColumnBindings: () => get.getColumnBindings(),
    };
    const op = {
      type: LogicalOperatorType.LOGICAL_DELETE,
      tableName: 'users',
      schema: usersSchema,
      children: [filter],
      expressions: [],
      types: [],
      estimatedCardinality: 0,
      getColumnBindings: () => [],
    } as unknown as LogicalDelete;

    const result = executeDelete(op, rm, noopCtx);
    expect(result.rowsAffected).toBe(1);
    expect(rm.prepareDelete).toHaveBeenCalledTimes(1);
    expect(rm.prepareDelete).toHaveBeenCalledWith('users', 0);
  });

  it('UPDATE with filter containing two expressions applies both', () => {
    const rm = mockRowManager([
      { id: 1, name: 'Alice', age: 25 },
      { id: 2, name: 'Bob', age: 30 },
      { id: 3, name: 'Charlie', age: 35 },
    ]);
    const get = makeGet();
    const filter: LogicalFilter = {
      type: LogicalOperatorType.LOGICAL_FILTER,
      children: [get],
      expressions: [
        comparison(colRef(0, 2), constant(20), 'GREATER'),
        comparison(colRef(0, 2), constant(30), 'LESS'),
      ],
      types: ['INTEGER', 'TEXT', 'INTEGER'],
      estimatedCardinality: 0,
      getColumnBindings: () => get.getColumnBindings(),
    };
    const op = {
      type: LogicalOperatorType.LOGICAL_UPDATE,
      tableName: 'users',
      schema: usersSchema,
      children: [filter],
      updateColumns: [1],
      expressions: [constant('UPDATED')],
      types: [],
      estimatedCardinality: 0,
      getColumnBindings: () => [],
    } as unknown as LogicalUpdate;

    const result = executeUpdate(op, rm, noopCtx);
    expect(result.rowsAffected).toBe(1);
    expect(rm.prepareUpdate).toHaveBeenCalledWith(
      'users',
      0,
      expect.objectContaining({ name: 'UPDATED', age: 25 }),
    );
  });
});
