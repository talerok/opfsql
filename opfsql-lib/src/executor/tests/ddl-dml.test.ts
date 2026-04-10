import { describe, it, expect, vi } from 'vitest';
import { LogicalOperatorType, BoundExpressionClass } from '../../binder/types.js';
import type {
  LogicalCreateTable,
  LogicalCreateIndex,
  LogicalAlterTable,
  LogicalDrop,
  LogicalInsert,
  LogicalUpdate,
  LogicalDelete,
  LogicalGet,
  LogicalFilter,
  BoundExpression,
} from '../../binder/types.js';
import type { ICatalog, IPageManager, IRowManager, TableSchema, Row, RowId } from '../../store/types.js';
import type { IIndexManager } from '../../store/index-manager.js';
import {
  executeCreateTable,
  executeCreateIndex,
  executeAlterTable,
  executeDrop,
} from '../ddl.js';
import { executeInsert, executeUpdate, executeDelete } from '../dml.js';
import { constant, comparison, colRef } from './helpers.js';
import { noopCtx } from './helpers.js';

// ---------------------------------------------------------------------------
// Mock catalog
// ---------------------------------------------------------------------------

const usersSchema: TableSchema = {
  name: 'users',
  columns: [
    { name: 'id', type: 'INTEGER', nullable: false, primaryKey: true, unique: true, defaultValue: null },
    { name: 'name', type: 'TEXT', nullable: false, primaryKey: false, unique: false, defaultValue: null },
    { name: 'age', type: 'INTEGER', nullable: true, primaryKey: false, unique: false, defaultValue: 0 },
  ],
};

function mockCatalog(tables: TableSchema[] = [usersSchema]): ICatalog {
  const tableMap = new Map(tables.map((t) => [t.name, t]));
  const indexMap = new Map<string, any>();
  return {
    hasTable: (n) => tableMap.has(n),
    getTable: (n) => tableMap.get(n),
    addTable: vi.fn(),
    removeTable: vi.fn(),
    updateTable: vi.fn(),
    getAllTables: () => [...tableMap.values()],
    hasIndex: (n) => indexMap.has(n),
    getIndex: (n) => indexMap.get(n),
    getTableIndexes: () => [],
    addIndex: vi.fn(),
    removeIndex: vi.fn(),
    serialize: () => ({ tables: [...tableMap.values()], indexes: [] }),
  };
}

function mockPageManager(): IPageManager {
  return {
    prepareInsert: vi.fn(async () => ({ pageId: 0, slotId: 0 })),
    prepareUpdate: vi.fn(async () => ({ pageId: 0, slotId: 0 })),
    prepareDelete: vi.fn(async () => {}),
    scanTable: async function* () {},
    readRow: vi.fn(async () => null),
    getPageMeta: vi.fn(async () => ({ lastPageId: -1, totalRowCount: 0, deadRowCount: 0 })),
    getAllPageKeys: vi.fn(async () => []),
    compactTable: vi.fn(async () => []),
    deleteTableData: vi.fn(async () => {}),
    readKey: vi.fn(async () => null),
    getAllKeys: vi.fn(async () => []),
    writeKey: vi.fn(),
    deleteKey: vi.fn(),
    commit: vi.fn(async () => {}),
    rollback: vi.fn(),
  };
}

// ---------------------------------------------------------------------------
// DDL Tests
// ---------------------------------------------------------------------------

describe('DDL executors', () => {
  describe('executeCreateTable', () => {
    it('returns CREATE_TABLE catalog change', async () => {
      const catalog = mockCatalog([]);
      const op = {
        type: LogicalOperatorType.LOGICAL_CREATE_TABLE,
        schema: usersSchema,
        ifNotExists: false,
        children: [], expressions: [], types: [], estimatedCardinality: 0,
        getColumnBindings: () => [],
      } as unknown as LogicalCreateTable;

      const result = await executeCreateTable(op, catalog);
      expect(result.catalogChanges).toHaveLength(1);
      expect(result.catalogChanges[0].type).toBe('CREATE_TABLE');
    });

    it('throws when table already exists', async () => {
      const catalog = mockCatalog([usersSchema]);
      const op = {
        type: LogicalOperatorType.LOGICAL_CREATE_TABLE,
        schema: usersSchema,
        ifNotExists: false,
        children: [], expressions: [], types: [], estimatedCardinality: 0,
        getColumnBindings: () => [],
      } as unknown as LogicalCreateTable;

      await expect(executeCreateTable(op, catalog)).rejects.toThrow('already exists');
    });

    it('IF NOT EXISTS — no-op when exists', async () => {
      const catalog = mockCatalog([usersSchema]);
      const op = {
        type: LogicalOperatorType.LOGICAL_CREATE_TABLE,
        schema: usersSchema,
        ifNotExists: true,
        children: [], expressions: [], types: [], estimatedCardinality: 0,
        getColumnBindings: () => [],
      } as unknown as LogicalCreateTable;

      const result = await executeCreateTable(op, catalog);
      expect(result.catalogChanges).toHaveLength(0);
    });
  });

  describe('executeCreateIndex', () => {
    it('returns CREATE_INDEX catalog change', async () => {
      const catalog = mockCatalog();
      const rm = mockRowManager();
      const im = mockIndexManager();
      const idx = { name: 'idx_users_name', tableName: 'users', columns: ['name'], unique: false };
      const op = {
        type: LogicalOperatorType.LOGICAL_CREATE_INDEX,
        index: idx,
        ifNotExists: false,
        children: [], expressions: [], types: [], estimatedCardinality: 0,
        getColumnBindings: () => [],
      } as unknown as LogicalCreateIndex;

      const result = await executeCreateIndex(op, catalog, rm, im);
      expect(result.catalogChanges[0]).toEqual({ type: 'CREATE_INDEX', index: idx });
    });
  });

  describe('executeAlterTable', () => {
    it('ADD COLUMN', () => {
      const catalog = mockCatalog();
      const newCol = { name: 'email', type: 'TEXT' as const, nullable: true, primaryKey: false, unique: false, defaultValue: null };
      const op = {
        type: LogicalOperatorType.LOGICAL_ALTER_TABLE,
        tableName: 'users',
        action: { type: 'ADD_COLUMN', column: newCol },
        children: [], expressions: [], types: [], estimatedCardinality: 0,
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
        children: [], expressions: [], types: [], estimatedCardinality: 0,
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
        children: [], expressions: [], types: [], estimatedCardinality: 0,
        getColumnBindings: () => [],
      } as unknown as LogicalAlterTable;

      expect(() => executeAlterTable(op, catalog)).toThrow('not found');
    });
  });

  describe('executeDrop', () => {
    it('DROP TABLE — deletes keys via pageManager', async () => {
      const catalog = mockCatalog();
      const pm = mockPageManager();
      const op = {
        type: LogicalOperatorType.LOGICAL_DROP,
        dropType: 'TABLE',
        name: 'users',
        ifExists: false,
        children: [], expressions: [], types: [], estimatedCardinality: 0,
        getColumnBindings: () => [],
      } as unknown as LogicalDrop;

      const result = await executeDrop(op, catalog, pm, mockIndexManager());
      expect(result.catalogChanges[0].type).toBe('DROP_TABLE');
      expect(pm.deleteTableData).toHaveBeenCalledWith('users');
    });

    it('DROP TABLE IF EXISTS — no-op when missing', async () => {
      const catalog = mockCatalog([]);
      const pm = mockPageManager();
      const op = {
        type: LogicalOperatorType.LOGICAL_DROP,
        dropType: 'TABLE',
        name: 'nope',
        ifExists: true,
        children: [], expressions: [], types: [], estimatedCardinality: 0,
        getColumnBindings: () => [],
      } as unknown as LogicalDrop;

      const result = await executeDrop(op, catalog, pm, mockIndexManager());
      expect(result.catalogChanges).toHaveLength(0);
    });

    it('DROP TABLE — throws if not found and no IF EXISTS', async () => {
      const catalog = mockCatalog([]);
      const pm = mockPageManager();
      const op = {
        type: LogicalOperatorType.LOGICAL_DROP,
        dropType: 'TABLE',
        name: 'nope',
        ifExists: false,
        children: [], expressions: [], types: [], estimatedCardinality: 0,
        getColumnBindings: () => [],
      } as unknown as LogicalDrop;

      await expect(executeDrop(op, catalog, pm, mockIndexManager())).rejects.toThrow('not found');
    });
  });
});

// ---------------------------------------------------------------------------
// DML Tests
// ---------------------------------------------------------------------------

function mockRowManager(
  rows: Array<{ id: number; name: string; age: number | null }> = [],
): IRowManager {
  const stored = rows.map((r, i) => ({
    rowId: { pageId: 0, slotId: i },
    row: r as unknown as Row,
  }));

  let nextSlot = stored.length;
  return {
    scanTable: async function* () {
      for (const entry of stored) {
        yield entry;
      }
    },
    prepareInsert: vi.fn(async (): Promise<RowId> => ({ pageId: 0, slotId: nextSlot++ })),
    prepareUpdate: vi.fn(async (): Promise<RowId> => ({ pageId: 0, slotId: nextSlot++ })),
    prepareDelete: vi.fn(async () => {}),
    readRow: vi.fn(async () => null),
  };
}

function mockIndexManager(): IIndexManager {
  return {
    insert: vi.fn(async () => {}),
    delete: vi.fn(async () => {}),
    search: vi.fn(async () => []),
    bulkLoad: vi.fn(async () => {}),
    dropIndex: vi.fn(async () => {}),
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
    it('INSERT VALUES — single row', async () => {
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

      const result = await executeInsert(op, rm, noopCtx);
      expect(result.rowsAffected).toBe(1);
      expect(rm.prepareInsert).toHaveBeenCalledTimes(1);
      expect(rm.prepareInsert).toHaveBeenCalledWith('users', {
        id: 1,
        name: 'Alice',
        age: 30,
      });
    });

    it('INSERT VALUES — multi-row', async () => {
      const rm = mockRowManager();
      const op = {
        type: LogicalOperatorType.LOGICAL_INSERT,
        tableName: 'users',
        schema: usersSchema,
        columns: [0, 1],
        children: [],
        expressions: [
          constant(1), constant('Alice'),
          constant(2), constant('Bob'),
        ],
        types: [],
        estimatedCardinality: 0,
        getColumnBindings: () => [],
      } as unknown as LogicalInsert;

      const result = await executeInsert(op, rm, noopCtx);
      expect(result.rowsAffected).toBe(2);
      expect(rm.prepareInsert).toHaveBeenCalledTimes(2);
    });

    it('INSERT VALUES — uses defaults for missing columns', async () => {
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

      await executeInsert(op, rm, noopCtx);
      expect(rm.prepareInsert).toHaveBeenCalledWith('users', {
        id: 1,
        name: 'Alice',
        age: 0, // default from schema
      });
    });
  });

  describe('executeUpdate', () => {
    it('updates matching rows', async () => {
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
        updateColumns: [2], // update age
        expressions: [constant(99)],
        types: [],
        estimatedCardinality: 0,
        getColumnBindings: () => [],
      } as unknown as LogicalUpdate;

      const result = await executeUpdate(op, rm, noopCtx);
      expect(result.rowsAffected).toBe(2);
      expect(rm.prepareUpdate).toHaveBeenCalledTimes(2);
    });

    it('UPDATE with WHERE filter', async () => {
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
        updateColumns: [1], // update name
        expressions: [constant('UPDATED')],
        types: [],
        estimatedCardinality: 0,
        getColumnBindings: () => [],
      } as unknown as LogicalUpdate;

      const result = await executeUpdate(op, rm, noopCtx);
      expect(result.rowsAffected).toBe(1);
      expect(rm.prepareUpdate).toHaveBeenCalledWith('users', { pageId: 0, slotId: 0 }, expect.objectContaining({ name: 'UPDATED' }));
    });
  });

  describe('executeDelete', () => {
    it('deletes all rows (no filter)', async () => {
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

      const result = await executeDelete(op, rm, noopCtx);
      expect(result.rowsAffected).toBe(2);
      expect(rm.prepareDelete).toHaveBeenCalledTimes(2);
    });

    it('deletes with WHERE filter', async () => {
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

      const result = await executeDelete(op, rm, noopCtx);
      expect(result.rowsAffected).toBe(1);
      expect(rm.prepareDelete).toHaveBeenCalledWith('users', { pageId: 0, slotId: 1 });
    });
  });
});
