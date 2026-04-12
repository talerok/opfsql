import { type LogicalOperator, LogicalOperatorType } from "../binder/types.js";
import type { ICatalog } from "../store/types.js";
import { optimizeBuildProbeSide } from "./build_probe_side.js";
import { decorrelateExists } from "./decorrelate_exists.js";
import { rewriteExpressions } from "./expression_rewriter.js";
import { pullupFilters } from "./filter_pullup.js";
import { pushdownFilters } from "./filter_pushdown.js";
import { rewriteInClauses } from "./in_clause_rewriter.js";
import { selectIndexes } from "./index_selection.js";
import { optimizeJoinOrder } from "./join_order.js";
import { pushdownLimit } from "./limit_pushdown.js";
import { removeUnusedColumns } from "./remove_unused_columns.js";
import { reorderFilters } from "./reorder_filter.js";

// ============================================================================
// Optimizer — orchestrates all optimization passes
//
// Pipeline order (based on DuckDB's optimizer.cpp):
// 1. Expression rewriting (constant folding, simplifications)
// 2. EXISTS decorrelation (correlated EXISTS → SEMI/ANTI join)
// 3. Filter pullup (extract join conditions for re-optimization)
// 4. Filter pushdown (push WHERE close to scan)
// 5. IN clause rewriting
// 6. Join order optimization
// 7. Remove unused columns
// 8. Build/probe side optimization
// 9. Limit pushdown
// 10. Reorder filter conditions by cost
// 11. Index selection (annotate LogicalGet with IndexHint)
// ============================================================================

const SKIP_OPTIMIZE = new Set([
  LogicalOperatorType.LOGICAL_INSERT,
  LogicalOperatorType.LOGICAL_UPDATE,
  LogicalOperatorType.LOGICAL_DELETE,
  LogicalOperatorType.LOGICAL_CREATE_TABLE,
  LogicalOperatorType.LOGICAL_CREATE_INDEX,
  LogicalOperatorType.LOGICAL_ALTER_TABLE,
  LogicalOperatorType.LOGICAL_DROP,
]);

export function optimize(
  plan: LogicalOperator,
  catalog?: ICatalog,
): LogicalOperator {
  if (SKIP_OPTIMIZE.has(plan.type)) return plan;

  let result = plan;

  // Phase 1: Expression simplification
  result = rewriteExpressions(result);

  // Phase 2: EXISTS decorrelation — correlated EXISTS/NOT EXISTS → SEMI/ANTI join
  result = decorrelateExists(result);

  // Phase 3: Filter pullup — extract join conditions for better placement
  result = pullupFilters(result);

  // Phase 4: Filter pushdown — push conditions close to scan
  result = pushdownFilters(result);

  // Phase 5: IN clause rewriting
  result = rewriteInClauses(result);

  // Phase 6: Join order optimization
  result = optimizeJoinOrder(result);

  // Phase 7: Remove unused columns
  result = removeUnusedColumns(result);

  // Phase 8: Build/probe side selection
  result = optimizeBuildProbeSide(result);

  // Phase 9: Limit pushdown
  result = pushdownLimit(result);

  // Phase 10: Reorder filter conditions (cheap first)
  result = reorderFilters(result);

  // Phase 11: Index selection
  if (catalog) {
    result = selectIndexes(result, catalog);
  }

  return result;
}

// Re-export individual passes for fine-grained usage
export { optimizeBuildProbeSide } from "./build_probe_side.js";
export { decorrelateExists } from "./decorrelate_exists.js";
export { rewriteExpressions } from "./expression_rewriter.js";
export { FilterCombiner } from "./filter_combiner.js";
export { pullupFilters } from "./filter_pullup.js";
export { pushdownFilters } from "./filter_pushdown.js";
export { rewriteInClauses } from "./in_clause_rewriter.js";
export { selectIndexes } from "./index_selection.js";
export { optimizeJoinOrder } from "./join_order.js";
export { pushdownLimit } from "./limit_pushdown.js";
export { removeUnusedColumns } from "./remove_unused_columns.js";
export { reorderFilters } from "./reorder_filter.js";
