import type { LogicalOperator } from '../binder/types.js';
import { LogicalOperatorType, BoundExpressionClass } from '../binder/types.js';
import type {
  BoundColumnRefExpression,
  BoundAggregateExpression,
  LogicalProjection,
} from '../binder/types.js';
import type { ICatalog, IPageManager, Row } from '../store/types.js';
import type { IIndexManager } from '../store/index-manager.js';
import type { ExecuteResult, Tuple, CTECacheEntry, Value } from './types.js';
import type { EvalContext } from './evaluate/context.js';
import { createPhysicalPlan } from './planner.js';
import { drainOperator } from './operators/utils.js';
import {
  executeCreateTable,
  executeCreateIndex,
  executeAlterTable,
  executeDrop,
} from './ddl.js';
import { executeInsert, executeUpdate, executeDelete } from './dml.js';
import { ExecutorError } from './errors.js';

// ---------------------------------------------------------------------------
// Main entry point
// ---------------------------------------------------------------------------

export async function execute(
  plan: LogicalOperator,
  pageManager: IPageManager,
  catalog: ICatalog,
  indexManager?: IIndexManager,
): Promise<ExecuteResult> {
  // Build eval context for subquery support
  const ctx: EvalContext = {
    executeSubplan: (subplan, outerTuple, outerResolver, limit) =>
      executeSubplan(subplan, pageManager, catalog, outerTuple, outerResolver, limit),
  };

  switch (plan.type) {
    // DDL
    case LogicalOperatorType.LOGICAL_CREATE_TABLE:
      return executeCreateTable(plan, catalog, indexManager);
    case LogicalOperatorType.LOGICAL_CREATE_INDEX:
      return executeCreateIndex(plan, catalog, pageManager, indexManager!);
    case LogicalOperatorType.LOGICAL_ALTER_TABLE:
      return executeAlterTable(plan, catalog);
    case LogicalOperatorType.LOGICAL_DROP:
      return executeDrop(plan, catalog, pageManager, indexManager!);

    // DML
    case LogicalOperatorType.LOGICAL_INSERT:
      return executeInsert(plan, pageManager, ctx, catalog, indexManager);
    case LogicalOperatorType.LOGICAL_UPDATE:
      return executeUpdate(plan, pageManager, ctx, catalog, indexManager);
    case LogicalOperatorType.LOGICAL_DELETE:
      return executeDelete(plan, pageManager, ctx, catalog, indexManager);

    // SELECT (physical pipeline)
    default:
      return executeSelect(plan, pageManager, catalog, ctx, indexManager);
  }
}

// ---------------------------------------------------------------------------
// SELECT execution
// ---------------------------------------------------------------------------

async function executeSelect(
  plan: LogicalOperator,
  pageManager: IPageManager,
  _catalog: ICatalog,
  ctx: EvalContext,
  indexManager?: IIndexManager,
): Promise<ExecuteResult> {
  const cteCache = new Map<number, CTECacheEntry>();
  const physical = createPhysicalPlan(plan, pageManager, cteCache, ctx, indexManager);
  const tuples = await drainOperator(physical);
  const columnNames = extractColumnNames(plan);
  const rows = tuplesToRows(tuples, columnNames);

  return {
    rows,
    rowsAffected: 0,
    catalogChanges: [],
  };
}

// ---------------------------------------------------------------------------
// Subplan execution (for subquery evaluation)
// ---------------------------------------------------------------------------

async function executeSubplan(
  plan: LogicalOperator,
  pageManager: IPageManager,
  catalog: ICatalog,
  outerTuple?: Tuple,
  outerResolver?: import('./resolve.js').Resolver,
  limit?: number,
): Promise<Tuple[]> {
  const ctx: EvalContext = {
    executeSubplan: (sub, ot, or_, lim) =>
      executeSubplan(sub, pageManager, catalog, ot, or_, lim),
    outerTuple,
    outerResolver,
  };
  const cteCache = new Map<number, CTECacheEntry>();
  const physical = createPhysicalPlan(plan, pageManager, cteCache, ctx);
  if (limit !== undefined) {
    // Early termination — collect at most `limit` tuples then stop.
    const result: Tuple[] = [];
    while (result.length < limit) {
      const batch = await physical.next();
      if (!batch) break;
      for (const tuple of batch) {
        result.push(tuple);
        if (result.length >= limit) break;
      }
    }
    return result;
  }
  return drainOperator(physical);
}

// ---------------------------------------------------------------------------
// Tuple → Row conversion
// ---------------------------------------------------------------------------

function extractColumnNames(plan: LogicalOperator): string[] {
  const proj = findProjection(plan);
  if (!proj) {
    return plan.types.map((_, i) => `column${i}`);
  }

  return proj.expressions.map((expr, i) => {
    if (proj.aliases[i]) {
      return proj.aliases[i]!;
    }
    if (expr.expressionClass === BoundExpressionClass.BOUND_COLUMN_REF) {
      return (expr as BoundColumnRefExpression).columnName;
    }
    if (expr.expressionClass === BoundExpressionClass.BOUND_AGGREGATE) {
      const agg = expr as BoundAggregateExpression;
      return agg.isStar
        ? `${agg.functionName.toLowerCase()}_star`
        : `${agg.functionName.toLowerCase()}_${i}`;
    }
    return `column${i}`;
  });
}

function findProjection(plan: LogicalOperator): LogicalProjection | null {
  if (plan.type === LogicalOperatorType.LOGICAL_PROJECTION) {
    return plan as LogicalProjection;
  }
  // For CTE, only search the outer query (children[1]), not the CTE definition
  if (plan.type === LogicalOperatorType.LOGICAL_MATERIALIZED_CTE) {
    return findProjection(plan.children[1]);
  }
  for (const child of plan.children) {
    const found = findProjection(child);
    if (found) return found;
  }
  return null;
}

function tuplesToRows(tuples: Tuple[], columnNames: string[]): Row[] {
  return tuples.map((tuple) => {
    const row: Row = {};
    for (let i = 0; i < columnNames.length; i++) {
      row[columnNames[i]] = tuple[i] ?? null;
    }
    return row;
  });
}
