import { describe, it, expect, vi } from 'vitest';
import type { LogicalAggregate, BoundAggregateExpression, MinMaxHint } from '../../binder/types.js';
import { BoundExpressionClass, LogicalOperatorType } from '../../binder/types.js';
import type { SyncIIndexManager, IndexDef } from '../../store/types.js';
import { PhysicalIndexMinMax } from '../operators/index-min-max.js';

// ---------------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------------

const idxAge: IndexDef = {
  name: 'idx_age',
  tableName: 'users',
  expressions: [{ type: 'column', name: 'age', returnType: 'INTEGER' }],
  unique: false,
};

const idxComposite: IndexDef = {
  name: 'idx_name_age',
  tableName: 'users',
  expressions: [
    { type: 'column', name: 'name', returnType: 'TEXT' },
    { type: 'column', name: 'age', returnType: 'INTEGER' },
  ],
  unique: false,
};

function makeAgg(hint: MinMaxHint): LogicalAggregate {
  const aggExpr: BoundAggregateExpression = {
    expressionClass: BoundExpressionClass.BOUND_AGGREGATE,
    functionName: hint.functionName,
    distinct: false,
    isStar: false,
    aggregateIndex: 0,
    children: [],
    returnType: 'INTEGER',
  };
  return {
    type: LogicalOperatorType.LOGICAL_AGGREGATE_AND_GROUP_BY,
    groupIndex: 0,
    aggregateIndex: 1,
    children: [null as any], // not used by PhysicalIndexMinMax
    expressions: [aggExpr],
    groups: [],
    havingExpression: null,
    types: ['INTEGER'],
    estimatedCardinality: 1,
    minMaxHint: hint,
    columnBindings: [{ tableIndex: 1, columnIndex: 0 }],
  };
}

function mockIndexManager(
  firstResult: { key: any[]; rowId: number } | null,
  lastResult: { key: any[]; rowId: number } | null,
): SyncIIndexManager {
  return {
    insert: vi.fn(),
    delete: vi.fn(),
    search: vi.fn(() => []),
    bulkLoad: vi.fn(() => 0),
    dropIndex: vi.fn(),
    first: vi.fn(() => firstResult),
    last: vi.fn(() => lastResult),
  };
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

describe('PhysicalIndexMinMax', () => {
  it('MIN returns first key from index', () => {
    const im = mockIndexManager({ key: [18], rowId: 5 }, null);
    const hint: MinMaxHint = { indexDef: idxAge, functionName: 'MIN', keyPosition: 0 };
    const op = new PhysicalIndexMinMax(makeAgg(hint), im, hint);
    const batch = op.next();
    expect(batch).toEqual([[18]]);
    expect(im.first).toHaveBeenCalledWith('idx_age');
  });

  it('MAX returns last key from index', () => {
    const im = mockIndexManager(null, { key: [65], rowId: 10 });
    const hint: MinMaxHint = { indexDef: idxAge, functionName: 'MAX', keyPosition: 0 };
    const op = new PhysicalIndexMinMax(makeAgg(hint), im, hint);
    const batch = op.next();
    expect(batch).toEqual([[65]]);
    expect(im.last).toHaveBeenCalledWith('idx_age');
  });

  it('returns [[null]] when index is empty (MIN)', () => {
    const im = mockIndexManager(null, null);
    const hint: MinMaxHint = { indexDef: idxAge, functionName: 'MIN', keyPosition: 0 };
    const op = new PhysicalIndexMinMax(makeAgg(hint), im, hint);
    expect(op.next()).toEqual([[null]]);
  });

  it('returns [[null]] when index is empty (MAX)', () => {
    const im = mockIndexManager(null, null);
    const hint: MinMaxHint = { indexDef: idxAge, functionName: 'MAX', keyPosition: 0 };
    const op = new PhysicalIndexMinMax(makeAgg(hint), im, hint);
    expect(op.next()).toEqual([[null]]);
  });

  it('extracts correct keyPosition from composite key', () => {
    const im = mockIndexManager(null, { key: ['Zelda', 42], rowId: 7 });
    const hint: MinMaxHint = { indexDef: idxComposite, functionName: 'MAX', keyPosition: 1 };
    const op = new PhysicalIndexMinMax(makeAgg(hint), im, hint);
    expect(op.next()).toEqual([[42]]);
  });

  it('emits only once', () => {
    const im = mockIndexManager({ key: [10], rowId: 1 }, null);
    const hint: MinMaxHint = { indexDef: idxAge, functionName: 'MIN', keyPosition: 0 };
    const op = new PhysicalIndexMinMax(makeAgg(hint), im, hint);
    expect(op.next()).toEqual([[10]]);
    expect(op.next()).toBeNull();
    expect(op.next()).toBeNull();
  });

  it('reset allows re-emission', () => {
    const im = mockIndexManager({ key: [10], rowId: 1 }, null);
    const hint: MinMaxHint = { indexDef: idxAge, functionName: 'MIN', keyPosition: 0 };
    const op = new PhysicalIndexMinMax(makeAgg(hint), im, hint);
    expect(op.next()).toEqual([[10]]);
    expect(op.next()).toBeNull();

    op.reset();
    expect(op.next()).toEqual([[10]]);
  });

  it('getLayout returns column bindings from aggregate', () => {
    const im = mockIndexManager(null, null);
    const hint: MinMaxHint = { indexDef: idxAge, functionName: 'MIN', keyPosition: 0 };
    const op = new PhysicalIndexMinMax(makeAgg(hint), im, hint);
    expect(op.getLayout()).toEqual([{ tableIndex: 1, columnIndex: 0 }]);
  });
});
