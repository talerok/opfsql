import type {
  BoundAggregateExpression,
  BoundColumnRefExpression,
  LogicalOperator,
  LogicalProjection,
} from "../binder/types.js";
import { BoundExpressionClass, LogicalOperatorType } from "../binder/types.js";
import type {
  SyncIIndexManager,
  SyncIRowManager,
} from "../store/types.js";
import type { ICatalog, Row } from "../store/types.js";
import {
  executeAlterTable,
  executeCreateIndex,
  executeCreateTable,
  executeDrop,
} from "./ddl/index.js";
import { executeDelete, executeInsert, executeUpdate } from "./dml/index.js";
import type { SyncEvalContext } from "./evaluate/context.js";
import { drainOperator } from "./operators/utils.js";
import { createPhysicalPlan } from "./planner/index.js";
import type { CTECacheEntry, ExecuteResult, Tuple, Value } from "./types.js";

export function execute(
  plan: LogicalOperator,
  rowManager: SyncIRowManager,
  catalog: ICatalog,
  indexManager?: SyncIIndexManager,
  params?: readonly Value[],
): ExecuteResult {
  const ctx: SyncEvalContext = {
    executeSubplan: (subplan, outerTuple, outerResolver, limit) =>
      executeSubplan(
        subplan,
        rowManager,
        catalog,
        outerTuple,
        outerResolver,
        limit,
        params,
      ),
    params,
  };

  switch (plan.type) {
    case LogicalOperatorType.LOGICAL_CREATE_TABLE:
      return executeCreateTable(plan, catalog, rowManager, indexManager);
    case LogicalOperatorType.LOGICAL_CREATE_INDEX:
      return executeCreateIndex(plan, catalog, rowManager, indexManager!);
    case LogicalOperatorType.LOGICAL_ALTER_TABLE:
      return executeAlterTable(plan, catalog);
    case LogicalOperatorType.LOGICAL_DROP:
      return executeDrop(plan, catalog, rowManager, indexManager!);
    case LogicalOperatorType.LOGICAL_INSERT:
      return executeInsert(plan, rowManager, ctx, catalog, indexManager);
    case LogicalOperatorType.LOGICAL_UPDATE:
      return executeUpdate(plan, rowManager, ctx, catalog, indexManager);
    case LogicalOperatorType.LOGICAL_DELETE:
      return executeDelete(plan, rowManager, ctx, catalog, indexManager);
    default:
      return executeSelect(plan, rowManager, ctx, indexManager);
  }
}

function executeSelect(
  plan: LogicalOperator,
  rowManager: SyncIRowManager,
  ctx: SyncEvalContext,
  indexManager?: SyncIIndexManager,
): ExecuteResult {
  const cteCache = new Map<number, CTECacheEntry>();
  const physical = createPhysicalPlan(
    plan,
    rowManager,
    cteCache,
    ctx,
    indexManager,
  );
  const tuples = drainOperator(physical);
  const columnNames = extractColumnNames(plan);
  const rows = tuplesToRows(tuples, columnNames);

  return { rows, rowsAffected: 0, catalogChanges: [] };
}

function executeSubplan(
  plan: LogicalOperator,
  rowManager: SyncIRowManager,
  catalog: ICatalog,
  outerTuple?: Tuple,
  outerResolver?: import("./resolve.js").Resolver,
  limit?: number,
  params?: readonly Value[],
): Tuple[] {
  const ctx: SyncEvalContext = {
    executeSubplan: (sub, ot, or_, lim) =>
      executeSubplan(sub, rowManager, catalog, ot, or_, lim, params),
    outerTuple,
    outerResolver,
    params,
  };
  const cteCache = new Map<number, CTECacheEntry>();
  const physical = createPhysicalPlan(plan, rowManager, cteCache, ctx);

  if (limit !== undefined) {
    const result: Tuple[] = [];
    while (result.length < limit) {
      const batch = physical.next();
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
// Tuple → Row conversion (shared logic from async executor)
// ---------------------------------------------------------------------------

function extractColumnNames(plan: LogicalOperator): string[] {
  const proj = findProjection(plan);
  if (!proj) return plan.types.map((_, i) => `column${i}`);

  return proj.expressions.map((expr, i) => {
    if (proj.aliases[i]) return proj.aliases[i]!;
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
  if (plan.type === LogicalOperatorType.LOGICAL_PROJECTION)
    return plan as LogicalProjection;
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
