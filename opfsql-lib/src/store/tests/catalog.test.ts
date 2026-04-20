import { describe, it, expect, vi } from 'vitest';
import { Catalog, initCatalog, writeCatalog } from '../catalog.js';
import type { TableSchema, IndexDef, SyncIPageStore } from '../types.js';

// ---------------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------------

const usersSchema: TableSchema = {
  name: 'users',
  columns: [
    { name: 'id', type: 'INTEGER', nullable: false, primaryKey: true, unique: true, autoIncrement: false, defaultValue: null },
    { name: 'name', type: 'TEXT', nullable: false, primaryKey: false, unique: false, autoIncrement: false, defaultValue: null },
  ],
};

const ordersSchema: TableSchema = {
  name: 'orders',
  columns: [
    { name: 'id', type: 'INTEGER', nullable: false, primaryKey: true, unique: true, autoIncrement: false, defaultValue: null },
  ],
};

const idxUsersId: IndexDef = {
  name: 'idx_users_id',
  tableName: 'users',
  expressions: [{ type: 'column', name: 'id', returnType: 'INTEGER' }],
  unique: true,
};

const idxUsersName: IndexDef = {
  name: 'idx_users_name',
  tableName: 'users',
  expressions: [{ type: 'column', name: 'name', returnType: 'TEXT' }],
  unique: false,
};

const idxOrdersId: IndexDef = {
  name: 'idx_orders_id',
  tableName: 'orders',
  expressions: [{ type: 'column', name: 'id', returnType: 'INTEGER' }],
  unique: true,
};

// ---------------------------------------------------------------------------
// Catalog
// ---------------------------------------------------------------------------

describe('Catalog', () => {
  it('starts empty', () => {
    const c = new Catalog();
    expect(c.getAllTables()).toEqual([]);
    expect(c.hasTable('users')).toBe(false);
  });

  it('addTable and getTable', () => {
    const c = new Catalog();
    c.addTable(usersSchema);
    expect(c.hasTable('users')).toBe(true);
    expect(c.getTable('users')).toBe(usersSchema);
  });

  it('table lookup is case-insensitive', () => {
    const c = new Catalog();
    c.addTable(usersSchema);
    expect(c.hasTable('USERS')).toBe(true);
    expect(c.getTable('Users')).toBe(usersSchema);
  });

  it('removeTable deletes table and its indexes', () => {
    const c = new Catalog();
    c.addTable(usersSchema);
    c.addIndex(idxUsersId);
    c.addIndex(idxUsersName);
    c.addTable(ordersSchema);
    c.addIndex(idxOrdersId);

    c.removeTable('users');
    expect(c.hasTable('users')).toBe(false);
    expect(c.hasIndex('idx_users_id')).toBe(false);
    expect(c.hasIndex('idx_users_name')).toBe(false);
    // orders unaffected
    expect(c.hasTable('orders')).toBe(true);
    expect(c.hasIndex('idx_orders_id')).toBe(true);
  });

  it('updateTable replaces schema', () => {
    const c = new Catalog();
    c.addTable(usersSchema);
    const updated = { ...usersSchema, columns: [...usersSchema.columns, { name: 'age', type: 'INTEGER' as const, nullable: true, primaryKey: false, unique: false, autoIncrement: false, defaultValue: null }] };
    c.updateTable(updated);
    expect(c.getTable('users')!.columns).toHaveLength(3);
  });

  it('getAllTables returns all tables', () => {
    const c = new Catalog();
    c.addTable(usersSchema);
    c.addTable(ordersSchema);
    expect(c.getAllTables()).toHaveLength(2);
  });

  it('getTable returns undefined for missing table', () => {
    const c = new Catalog();
    expect(c.getTable('nonexistent')).toBeUndefined();
  });

  // --- Index operations ---

  it('addIndex and getIndex', () => {
    const c = new Catalog();
    c.addIndex(idxUsersId);
    expect(c.hasIndex('idx_users_id')).toBe(true);
    expect(c.getIndex('idx_users_id')).toBe(idxUsersId);
  });

  it('index lookup is case-insensitive', () => {
    const c = new Catalog();
    c.addIndex(idxUsersId);
    expect(c.hasIndex('IDX_USERS_ID')).toBe(true);
    expect(c.getIndex('Idx_Users_Id')).toBe(idxUsersId);
  });

  it('getTableIndexes returns indexes for table', () => {
    const c = new Catalog();
    c.addIndex(idxUsersId);
    c.addIndex(idxUsersName);
    c.addIndex(idxOrdersId);
    const userIndexes = c.getTableIndexes('users');
    expect(userIndexes).toHaveLength(2);
    expect(userIndexes.map(i => i.name).sort()).toEqual(['idx_users_id', 'idx_users_name']);
  });

  it('getTableIndexes is case-insensitive', () => {
    const c = new Catalog();
    c.addIndex(idxUsersId);
    expect(c.getTableIndexes('USERS')).toHaveLength(1);
  });

  it('removeIndex', () => {
    const c = new Catalog();
    c.addIndex(idxUsersId);
    c.removeIndex('idx_users_id');
    expect(c.hasIndex('idx_users_id')).toBe(false);
  });

  it('getIndex returns undefined for missing index', () => {
    const c = new Catalog();
    expect(c.getIndex('nonexistent')).toBeUndefined();
  });

  // --- Serialization ---

  it('serialize and deserialize round-trip', () => {
    const c = new Catalog();
    c.addTable(usersSchema);
    c.addTable(ordersSchema);
    c.addIndex(idxUsersId);
    c.addIndex(idxOrdersId);

    const data = c.serialize();
    const restored = Catalog.deserialize(data);

    expect(restored.hasTable('users')).toBe(true);
    expect(restored.hasTable('orders')).toBe(true);
    expect(restored.hasIndex('idx_users_id')).toBe(true);
    expect(restored.hasIndex('idx_orders_id')).toBe(true);
    expect(restored.getAllTables()).toHaveLength(2);
  });

  it('snapshot returns deep clone', () => {
    const c = new Catalog();
    c.addTable(usersSchema);
    const snap = c.snapshot();
    snap.tables[0].name = 'mutated';
    expect(c.getTable('users')!.name).toBe('users');
  });
});

// ---------------------------------------------------------------------------
// initCatalog / writeCatalog
// ---------------------------------------------------------------------------

describe('initCatalog', () => {
  it('returns empty catalog when page is null', () => {
    const ps = { readPage: vi.fn(() => null) } as unknown as SyncIPageStore;
    const c = initCatalog(ps);
    expect(c.getAllTables()).toEqual([]);
  });

  it('deserializes from stored page', () => {
    const data = {
      tables: [usersSchema],
      indexes: [idxUsersId],
    };
    const ps = { readPage: vi.fn(() => data) } as unknown as SyncIPageStore;
    const c = initCatalog(ps);
    expect(c.hasTable('users')).toBe(true);
    expect(c.hasIndex('idx_users_id')).toBe(true);
  });
});

describe('writeCatalog', () => {
  it('writes serialized catalog to page 1', () => {
    const c = new Catalog();
    c.addTable(usersSchema);
    const ps = { writePage: vi.fn() } as unknown as SyncIPageStore;
    writeCatalog(c, ps);
    expect(ps.writePage).toHaveBeenCalledWith(1, {
      tables: [usersSchema],
      indexes: [],
    });
  });
});
