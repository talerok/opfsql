import type {
  LogicalCreateTable,
  LogicalCreateIndex,
  LogicalAlterTable,
  LogicalDrop,
} from '../binder/types.js';
import type { ICatalog, IPageManager } from '../store/types.js';
import type { ExecuteResult } from './types.js';
import { ExecutorError } from './errors.js';

const EMPTY: ExecuteResult = {
  rows: [],
  rowsAffected: 0,
  catalogChanges: [],
};

export function executeCreateTable(
  op: LogicalCreateTable,
  catalog: ICatalog,
): ExecuteResult {
  if (catalog.hasTable(op.schema.name)) {
    if (op.ifNotExists) return { ...EMPTY };
    throw new ExecutorError(`Table "${op.schema.name}" already exists`);
  }

  return {
    ...EMPTY,
    catalogChanges: [{ type: 'CREATE_TABLE', schema: op.schema }],
  };
}

export function executeCreateIndex(
  op: LogicalCreateIndex,
  catalog: ICatalog,
): ExecuteResult {
  if (catalog.hasIndex(op.index.name)) {
    if (op.ifNotExists) return { ...EMPTY };
    throw new ExecutorError(`Index "${op.index.name}" already exists`);
  }

  return {
    ...EMPTY,
    catalogChanges: [{ type: 'CREATE_INDEX', index: op.index }],
  };
}

export function executeAlterTable(
  op: LogicalAlterTable,
  catalog: ICatalog,
): ExecuteResult {
  const before = catalog.getTable(op.tableName);
  if (!before) {
    throw new ExecutorError(`Table "${op.tableName}" not found`);
  }

  let after;
  if (op.action.type === 'ADD_COLUMN') {
    after = {
      ...before,
      columns: [...before.columns, op.action.column],
    };
  } else {
    const dropName = op.action.columnName;
    after = {
      ...before,
      columns: before.columns.filter(
        (c) => c.name.toLowerCase() !== dropName.toLowerCase(),
      ),
    };
  }

  return {
    ...EMPTY,
    catalogChanges: [
      { type: 'ALTER_TABLE', name: op.tableName, before, after },
    ],
  };
}

export async function executeDrop(
  op: LogicalDrop,
  catalog: ICatalog,
  pageManager: IPageManager,
): Promise<ExecuteResult> {
  if (op.dropType === 'TABLE') {
    return executeDropTable(op, catalog, pageManager);
  }
  return executeDropIndex(op, catalog);
}

async function executeDropTable(
  op: LogicalDrop,
  catalog: ICatalog,
  pageManager: IPageManager,
): Promise<ExecuteResult> {
  const schema = catalog.getTable(op.name);
  if (!schema) {
    if (op.ifExists) return { ...EMPTY };
    throw new ExecutorError(`Table "${op.name}" not found`);
  }

  const pageKeys = await pageManager.getAllPageKeys(op.name);
  const metaKey = pageManager.getMetaKey(op.name);

  for (const key of pageKeys) {
    pageManager.deleteKey(key);
  }
  pageManager.deleteKey(metaKey);

  return {
    ...EMPTY,
    catalogChanges: [{ type: 'DROP_TABLE', name: op.name, schema }],
  };
}

function executeDropIndex(
  op: LogicalDrop,
  catalog: ICatalog,
): ExecuteResult {
  const index = catalog.getIndex(op.name);
  if (!index) {
    if (op.ifExists) return { ...EMPTY };
    throw new ExecutorError(`Index "${op.name}" not found`);
  }

  return {
    ...EMPTY,
    catalogChanges: [{ type: 'DROP_INDEX', name: op.name, index }],
  };
}
