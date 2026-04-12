import type {
  IndexHint,
  IndexSearchPredicate,
  LogicalGet,
  LogicalOperator,
  TableFilter,
} from "../binder/types.js";
import { LogicalOperatorType } from "../binder/types.js";
import type { ICatalog, IndexDef } from "../store/types.js";

// ---------------------------------------------------------------------------
// Index selection optimizer pass
//
// Walks the plan tree. For each LogicalGet with tableFilters, checks if any
// available index can accelerate the scan. Annotates the LogicalGet with an
// IndexHint if a suitable index is found.
// ---------------------------------------------------------------------------

export function selectIndexes(
  plan: LogicalOperator,
  catalog: ICatalog,
): LogicalOperator {
  return walkAndAnnotate(plan, catalog);
}

function walkAndAnnotate(
  node: LogicalOperator,
  catalog: ICatalog,
): LogicalOperator {
  // Recurse into children first
  for (let i = 0; i < node.children.length; i++) {
    node.children[i] = walkAndAnnotate(node.children[i], catalog);
  }

  if (node.type !== LogicalOperatorType.LOGICAL_GET) return node;

  const get = node as LogicalGet;
  if (get.tableName === "__empty") return node;
  if (get.tableFilters.length === 0) return node;

  const indexes = catalog.getTableIndexes(get.tableName);
  if (indexes.length === 0) return node;

  const best = findBestIndex(get, indexes);
  if (best) {
    get.indexHint = best;
  }

  return node;
}

// ---------------------------------------------------------------------------
// Index matching
// ---------------------------------------------------------------------------

interface MatchResult {
  score: number;
  hint: IndexHint;
}

function findBestIndex(get: LogicalGet, indexes: IndexDef[]): IndexHint | null {
  let bestScore = 0;
  let bestHint: IndexHint | null = null;

  for (const idx of indexes) {
    const result = matchIndex(get, idx);
    if (result && result.score > bestScore) {
      bestScore = result.score;
      bestHint = result.hint;
    }
  }

  return bestHint;
}

function matchIndex(get: LogicalGet, idx: IndexDef): MatchResult | null {
  const schema = get.schema;
  const covered: TableFilter[] = [];
  const predicates: IndexSearchPredicate[] = [];
  let prefixMatched = 0;

  // For each column in the index (in order), try to match with equality
  // filters first, then one range filter.
  for (let i = 0; i < idx.columns.length; i++) {
    const colName = idx.columns[i].toLowerCase();
    const colIndex = schema.columns.findIndex(
      (c) => c.name.toLowerCase() === colName,
    );
    if (colIndex === -1) break;

    // Find equality filter
    const eqFilter = get.tableFilters.find(
      (f) => f.columnIndex === colIndex && f.comparisonType === "EQUAL",
    );

    if (eqFilter) {
      predicates.push({
        columnPosition: i,
        comparisonType: "EQUAL",
        value: eqFilter.constant,
      });
      covered.push(eqFilter);
      prefixMatched++;
    } else {
      // Check for range filters on this column
      const rangeFilters = get.tableFilters.filter(
        (f) =>
          f.columnIndex === colIndex &&
          (f.comparisonType === "LESS" ||
            f.comparisonType === "GREATER" ||
            f.comparisonType === "LESS_EQUAL" ||
            f.comparisonType === "GREATER_EQUAL"),
      );
      for (const rf of rangeFilters) {
        predicates.push({
          columnPosition: i,
          comparisonType:
            rf.comparisonType as IndexSearchPredicate["comparisonType"],
          value: rf.constant,
        });
        covered.push(rf);
      }
      break; // Can't match further columns after a range
    }
  }

  if (predicates.length === 0) return null;

  // Everything not covered is residual
  const residual: TableFilter[] = [];
  for (const f of get.tableFilters) {
    if (!covered.includes(f)) {
      residual.push(f);
    }
  }

  // Score: number of covered predicates, with bonus for unique + full key match
  let score = covered.length;
  if (idx.unique && prefixMatched === idx.columns.length) {
    score += 10; // strong bonus for unique full-key match (returns at most 1 row)
  }

  return {
    score,
    hint: {
      indexDef: idx,
      predicates,
      residualFilters: residual,
      coveredFilters: covered,
    },
  };
}
