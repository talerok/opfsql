import type {
  BoundExpression,
  ColumnBinding,
  LogicalFilter,
  LogicalGet,
  LogicalOperator,
} from "../../../binder/types.js";
import { BoundExpressionClass, LogicalOperatorType } from "../../../binder/types.js";
import type { Row } from "../../../store/types.js";
import type { CompiledFilter } from "../../evaluate/compile.js";
import { compileFilter } from "../../evaluate/compile.js";
import type { SyncEvalContext } from "../../evaluate/context.js";
import { isTruthy } from "../../evaluate/utils/compare.js";
import { evaluateExpression } from "../../evaluate/index.js";
import { passesCompiledFilters } from "../../operators/utils.js";
import { buildResolver, type Resolver } from "../../resolve.js";
import { ExecutorError } from "../../errors.js";
import type { Tuple } from "../../types.js";

export interface DmlScanInfo {
  get: LogicalGet;
  condition: BoundExpression | null;
  layout: ColumnBinding[];
  resolver: Resolver;
  compiledFilters: CompiledFilter[];
}

export function extractDmlScan(
  child: LogicalOperator,
  ctx: SyncEvalContext,
): DmlScanInfo {
  const { get, condition } = extractConditions(child);
  const layout = get.schema.columns.map((_, i) => ({
    tableIndex: get.tableIndex,
    columnIndex: i,
  }));
  const resolver = buildResolver(layout);
  const compiledFilters = get.tableFilters.map((f) =>
    compileFilter(f, resolver, ctx),
  );
  return { get, condition, layout, resolver, compiledFilters };
}

function extractConditions(child: LogicalOperator): {
  get: LogicalGet;
  condition: BoundExpression | null;
} {
  if (child.type === LogicalOperatorType.LOGICAL_FILTER) {
    const filter = child as LogicalFilter;
    const inner = extractConditions(filter.children[0]);
    const filterCond: BoundExpression =
      filter.expressions.length === 1
        ? filter.expressions[0]
        : {
            expressionClass: BoundExpressionClass.BOUND_CONJUNCTION,
            conjunctionType: "AND" as const,
            children: filter.expressions,
            returnType: "BOOLEAN" as const,
          };
    if (inner.condition) {
      const combined: BoundExpression = {
        expressionClass: BoundExpressionClass.BOUND_CONJUNCTION,
        conjunctionType: "AND" as const,
        children: [filterCond, inner.condition],
        returnType: "BOOLEAN" as const,
      };
      return { get: inner.get, condition: combined };
    }
    return { get: inner.get, condition: filterCond };
  }
  if (child.type === LogicalOperatorType.LOGICAL_GET) {
    return { get: child as LogicalGet, condition: null };
  }
  throw new ExecutorError(`Unexpected node ${child.type} in DML scan tree`);
}

export function rowToTuple(row: Row, get: LogicalGet): Tuple {
  return get.schema.columns.map((col) => {
    const val = row[col.name];
    return val !== undefined ? val : (col.defaultValue ?? null);
  });
}

export function passesFilter(
  tuple: Tuple,
  scan: DmlScanInfo,
  ctx: SyncEvalContext,
): boolean {
  if (!passesCompiledFilters(tuple, scan.compiledFilters, ctx.params)) {
    return false;
  }
  if (scan.condition) {
    const val = evaluateExpression(scan.condition, tuple, scan.resolver, ctx);
    if (!isTruthy(val)) return false;
  }
  return true;
}
