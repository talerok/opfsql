import { describe, it, expect, vi } from 'vitest';
import { findColumnIndex, findColumnIndexOrThrow, getPrimaryKeyColumns } from '../core/utils/find-column.js';
import { evalConstantInt, evalConstantValue } from '../core/utils/eval-constant.js';
import { findProjection, extractColumnsFromPlan } from '../core/utils/extract-columns.js';
import { requireTable } from '../core/utils/require-table.js';
import { ExpressionClass } from '../../parser/types.js';
import type { TableSchema } from '../../store/types.js';
import type { ColumnDef } from '../../types.js';
import { BoundExpressionClass, LogicalOperatorType } from '../types.js';
import type { LogicalGet, LogicalProjection, LogicalFilter } from '../types.js';
import type { BindContext } from '../core/context.js';
import { BindError } from '../core/errors.js';

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

const compositeSchema: TableSchema = {
  name: 'order_items',
  columns: [
    { name: 'order_id', type: 'INTEGER', nullable: false, primaryKey: true, unique: false, autoIncrement: false, defaultValue: null },
    { name: 'item_id',  type: 'INTEGER', nullable: false, primaryKey: true, unique: false, autoIncrement: false, defaultValue: null },
    { name: 'qty',      type: 'INTEGER', nullable: true,  primaryKey: false, unique: false, autoIncrement: false, defaultValue: null },
  ],
};

// ---------------------------------------------------------------------------
// find-column
// ---------------------------------------------------------------------------

describe('findColumnIndex', () => {
  it('finds column by exact name', () => {
    expect(findColumnIndex(usersSchema, 'name')).toBe(1);
  });

  it('case-insensitive lookup', () => {
    expect(findColumnIndex(usersSchema, 'NAME')).toBe(1);
    expect(findColumnIndex(usersSchema, 'Id')).toBe(0);
  });

  it('returns -1 for missing column', () => {
    expect(findColumnIndex(usersSchema, 'email')).toBe(-1);
  });
});

describe('findColumnIndexOrThrow', () => {
  it('returns index for existing column', () => {
    expect(findColumnIndexOrThrow(usersSchema, 'age')).toBe(2);
  });

  it('throws BindError for missing column', () => {
    expect(() => findColumnIndexOrThrow(usersSchema, 'email')).toThrow(BindError);
    expect(() => findColumnIndexOrThrow(usersSchema, 'email')).toThrow(/not found/);
  });
});

describe('getPrimaryKeyColumns', () => {
  it('returns single PK column index', () => {
    expect(getPrimaryKeyColumns(usersSchema)).toEqual([0]);
  });

  it('returns multiple PK column indexes for composite key', () => {
    expect(getPrimaryKeyColumns(compositeSchema)).toEqual([0, 1]);
  });

  it('returns empty array for table without PK', () => {
    const noPK: TableSchema = {
      name: 'logs',
      columns: [
        { name: 'msg', type: 'TEXT', nullable: true, primaryKey: false, unique: false, autoIncrement: false, defaultValue: null },
      ],
    };
    expect(getPrimaryKeyColumns(noPK)).toEqual([]);
  });
});

// ---------------------------------------------------------------------------
// eval-constant
// ---------------------------------------------------------------------------

describe('evalConstantInt', () => {
  it('extracts integer from constant expression', () => {
    const expr = {
      expression_class: ExpressionClass.CONSTANT,
      value: { type: 'INTEGER' as const, value: 42 },
    };
    expect(evalConstantInt(expr as any)).toBe(42);
  });

  it('throws for negative integer', () => {
    const expr = {
      expression_class: ExpressionClass.CONSTANT,
      value: { type: 'INTEGER' as const, value: -5 },
    };
    expect(() => evalConstantInt(expr as any)).toThrow(BindError);
  });

  it('throws for non-constant expression', () => {
    const expr = {
      expression_class: ExpressionClass.COLUMN_REF,
    };
    expect(() => evalConstantInt(expr as any)).toThrow(BindError);
  });

  it('throws for string constant', () => {
    const expr = {
      expression_class: ExpressionClass.CONSTANT,
      value: { type: 'TEXT' as const, value: 'hello' },
    };
    expect(() => evalConstantInt(expr as any)).toThrow(BindError);
  });
});

describe('evalConstantValue', () => {
  it('extracts value from constant expression', () => {
    const expr = {
      expression_class: ExpressionClass.CONSTANT,
      value: { type: 'TEXT' as const, value: 'hello' },
    };
    expect(evalConstantValue(expr as any)).toBe('hello');
  });

  it('returns null for non-constant expression', () => {
    const expr = {
      expression_class: ExpressionClass.COLUMN_REF,
    };
    expect(evalConstantValue(expr as any)).toBeNull();
  });
});

// ---------------------------------------------------------------------------
// extract-columns
// ---------------------------------------------------------------------------

describe('findProjection', () => {
  it('returns projection at root', () => {
    const proj: LogicalProjection = {
      type: LogicalOperatorType.LOGICAL_PROJECTION,
      tableIndex: 1,
      children: [{
        type: LogicalOperatorType.LOGICAL_GET,
        children: [],
        expressions: [],
        types: [],
        estimatedCardinality: 0,
        getColumnBindings: () => [],
      } as any],
      expressions: [],
      aliases: [],
      types: [],
      estimatedCardinality: 0,
      getColumnBindings: () => [],
    };
    expect(findProjection(proj)).toBe(proj);
  });

  it('finds projection in child', () => {
    const proj: LogicalProjection = {
      type: LogicalOperatorType.LOGICAL_PROJECTION,
      tableIndex: 1,
      children: [{
        type: LogicalOperatorType.LOGICAL_GET,
        children: [],
        expressions: [],
        types: [],
        estimatedCardinality: 0,
        getColumnBindings: () => [],
      } as any],
      expressions: [],
      aliases: [],
      types: [],
      estimatedCardinality: 0,
      getColumnBindings: () => [],
    };
    const filter: LogicalFilter = {
      type: LogicalOperatorType.LOGICAL_FILTER,
      children: [proj],
      expressions: [],
      types: [],
      estimatedCardinality: 0,
      getColumnBindings: () => [],
    };
    expect(findProjection(filter)).toBe(proj);
  });

  it('returns null when no projection exists', () => {
    const get = {
      type: LogicalOperatorType.LOGICAL_GET,
      children: [],
      expressions: [],
      types: [],
      estimatedCardinality: 0,
      getColumnBindings: () => [],
    };
    expect(findProjection(get as any)).toBeNull();
  });
});

describe('extractColumnsFromPlan', () => {
  it('uses projection aliases', () => {
    const proj: LogicalProjection = {
      type: LogicalOperatorType.LOGICAL_PROJECTION,
      tableIndex: 1,
      children: [{ type: LogicalOperatorType.LOGICAL_GET, children: [], expressions: [], types: [], estimatedCardinality: 0, getColumnBindings: () => [] } as any],
      expressions: [
        { expressionClass: BoundExpressionClass.BOUND_COLUMN_REF, binding: { tableIndex: 0, columnIndex: 0 }, tableName: 't0', columnName: 'id', returnType: 'INTEGER' },
      ],
      aliases: ['user_id'],
      types: ['INTEGER'],
      estimatedCardinality: 0,
      getColumnBindings: () => [],
    };
    const cols = extractColumnsFromPlan(proj, ['INTEGER']);
    expect(cols[0].name).toBe('user_id');
  });

  it('uses column name when no alias', () => {
    const proj: LogicalProjection = {
      type: LogicalOperatorType.LOGICAL_PROJECTION,
      tableIndex: 1,
      children: [{ type: LogicalOperatorType.LOGICAL_GET, children: [], expressions: [], types: [], estimatedCardinality: 0, getColumnBindings: () => [] } as any],
      expressions: [
        { expressionClass: BoundExpressionClass.BOUND_COLUMN_REF, binding: { tableIndex: 0, columnIndex: 0 }, tableName: 't0', columnName: 'id', returnType: 'INTEGER' },
      ],
      aliases: [null],
      types: ['INTEGER'],
      estimatedCardinality: 0,
      getColumnBindings: () => [],
    };
    const cols = extractColumnsFromPlan(proj, ['INTEGER']);
    expect(cols[0].name).toBe('id');
  });

  it('generates column0..N when no projection', () => {
    const get = {
      type: LogicalOperatorType.LOGICAL_GET,
      children: [],
      expressions: [],
      types: ['INTEGER', 'TEXT'],
      estimatedCardinality: 0,
      getColumnBindings: () => [],
    };
    const cols = extractColumnsFromPlan(get as any, ['INTEGER', 'TEXT']);
    expect(cols[0].name).toBe('column0');
    expect(cols[1].name).toBe('column1');
    expect(cols[0].type).toBe('INTEGER');
    expect(cols[1].type).toBe('TEXT');
  });

  it('aggregate expression generates function-based name', () => {
    const proj: LogicalProjection = {
      type: LogicalOperatorType.LOGICAL_PROJECTION,
      tableIndex: 1,
      children: [{ type: LogicalOperatorType.LOGICAL_GET, children: [], expressions: [], types: [], estimatedCardinality: 0, getColumnBindings: () => [] } as any],
      expressions: [
        { expressionClass: BoundExpressionClass.BOUND_AGGREGATE, functionName: 'COUNT', isStar: true, distinct: false, aggregateIndex: 0, children: [], returnType: 'INTEGER' },
      ],
      aliases: [null],
      types: ['INTEGER'],
      estimatedCardinality: 0,
      getColumnBindings: () => [],
    };
    const cols = extractColumnsFromPlan(proj, ['INTEGER']);
    expect(cols[0].name).toBe('count_star');
  });
});

// ---------------------------------------------------------------------------
// require-table
// ---------------------------------------------------------------------------

describe('requireTable', () => {
  it('returns schema when table exists', () => {
    const ctx = {
      catalog: {
        getTable: vi.fn((name: string) => name === 'users' ? usersSchema : undefined),
      },
    } as unknown as BindContext;
    expect(requireTable(ctx, 'users')).toBe(usersSchema);
  });

  it('throws BindError when table not found', () => {
    const ctx = {
      catalog: {
        getTable: vi.fn(() => undefined),
      },
    } as unknown as BindContext;
    expect(() => requireTable(ctx, 'nonexistent')).toThrow(BindError);
    expect(() => requireTable(ctx, 'nonexistent')).toThrow(/not found/);
  });
});
