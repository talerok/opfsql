import type { LogicalOperator } from '../binder/types.js';
import type { ICatalog } from '../store/types.js';
import { rewriteExpressions } from './expression_rewriter.js';
import { pullupFilters } from './filter_pullup.js';
import { pushdownFilters } from './filter_pushdown.js';
import { rewriteInClauses } from './in_clause_rewriter.js';
import { optimizeJoinOrder } from './join_order.js';
import { removeUnusedColumns } from './remove_unused_columns.js';
import { optimizeBuildProbeSide } from './build_probe_side.js';
import { pushdownLimit } from './limit_pushdown.js';
import { reorderFilters } from './reorder_filter.js';
import { selectIndexes } from './index_selection.js';

// ============================================================================
// Optimizer — orchestrates all optimization passes
//
// Pipeline order (based on DuckDB's optimizer.cpp):
// 1. Expression rewriting (constant folding, simplifications)
// 2. Filter pullup (extract join conditions for re-optimization)
// 3. Filter pushdown (push WHERE close to scan)
// 4. IN clause rewriting
// 5. Join order optimization
// 6. Remove unused columns
// 7. Build/probe side optimization
// 8. Limit pushdown
// 9. Reorder filter conditions by cost
// 10. Index selection (annotate LogicalGet with IndexHint)
// ============================================================================

export function optimize(
  plan: LogicalOperator,
  catalog?: ICatalog,
): LogicalOperator {
  let result = plan;

  // Phase 1: Expression simplification
  result = rewriteExpressions(result);

  // Phase 2: Filter pullup — extract join conditions for better placement
  result = pullupFilters(result);

  // Phase 3: Filter pushdown — push conditions close to scan
  result = pushdownFilters(result);

  // Phase 4: IN clause rewriting
  result = rewriteInClauses(result);

  // Phase 5: Join order optimization
  result = optimizeJoinOrder(result);

  // Phase 6: Remove unused columns
  result = removeUnusedColumns(result);

  // Phase 7: Build/probe side selection
  result = optimizeBuildProbeSide(result);

  // Phase 8: Limit pushdown
  result = pushdownLimit(result);

  // Phase 9: Reorder filter conditions (cheap first)
  result = reorderFilters(result);

  // Phase 10: Index selection
  if (catalog) {
    result = selectIndexes(result, catalog);
  }

  return result;
}

// Re-export individual passes for fine-grained usage
export { rewriteExpressions } from './expression_rewriter.js';
export { pullupFilters } from './filter_pullup.js';
export { pushdownFilters } from './filter_pushdown.js';
export { rewriteInClauses } from './in_clause_rewriter.js';
export { optimizeJoinOrder } from './join_order.js';
export { removeUnusedColumns } from './remove_unused_columns.js';
export { optimizeBuildProbeSide } from './build_probe_side.js';
export { pushdownLimit } from './limit_pushdown.js';
export { reorderFilters } from './reorder_filter.js';
export { FilterCombiner } from './filter_combiner.js';
export { selectIndexes } from './index_selection.js';
