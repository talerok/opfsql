import type {
  LogicalOperator,
  LogicalComparisonJoin,
  LogicalCrossProduct,
  JoinCondition,
  BoundExpression,
} from '../binder/types.js';
import { LogicalOperatorType } from '../binder/types.js';
import { getOperatorTables, getExpressionTables, collectColumnRefs } from './utils/index.js';

// ============================================================================
// Join Order Optimizer — reorders joins for minimal intermediate cardinality
//
// Simplified version of DuckDB's join_order_optimizer.cpp:
// 1. Extract all base relations and join conditions from a join tree
// 2. For ≤6 relations: exhaustive enumeration (all permutations)
//    For >6 relations: greedy heuristic (always pick smallest next join)
// 3. Reconstruct the join tree in optimal order
// ============================================================================

interface Relation {
  plan: LogicalOperator;
  tables: Set<number>;
  cardinality: number;
}

interface JoinEdge {
  conditions: JoinCondition[];
  leftTables: Set<number>;
  rightTables: Set<number>;
}

export function optimizeJoinOrder(plan: LogicalOperator): LogicalOperator {
  // Recurse into non-join children first
  for (let i = 0; i < plan.children.length; i++) {
    plan.children[i] = optimizeJoinOrder(plan.children[i]);
  }

  // Only optimize join trees
  if (
    plan.type !== LogicalOperatorType.LOGICAL_COMPARISON_JOIN &&
    plan.type !== LogicalOperatorType.LOGICAL_CROSS_PRODUCT
  ) {
    return plan;
  }

  // Only reorder INNER joins and cross products
  if (
    plan.type === LogicalOperatorType.LOGICAL_COMPARISON_JOIN &&
    (plan as LogicalComparisonJoin).joinType !== 'INNER'
  ) {
    return plan;
  }

  // Extract all relations and join edges
  const relations: Relation[] = [];
  const edges: JoinEdge[] = [];
  extractJoinTree(plan, relations, edges);

  if (relations.length <= 1) return plan;

  // Find optimal join order
  const order =
    relations.length <= 6
      ? exhaustiveJoinOrder(relations, edges)
      : greedyJoinOrder(relations, edges);

  // Reconstruct the join tree
  return reconstructJoinTree(order, edges);
}

// ============================================================================
// Extract relations and edges from a join tree
// ============================================================================

function extractJoinTree(
  op: LogicalOperator,
  relations: Relation[],
  edges: JoinEdge[],
): void {
  if (op.type === LogicalOperatorType.LOGICAL_COMPARISON_JOIN) {
    const join = op as LogicalComparisonJoin;
    if (join.joinType === 'INNER') {
      // Recurse into both sides
      extractJoinTree(join.children[0], relations, edges);
      extractJoinTree(join.children[1], relations, edges);

      // Collect join conditions as edges
      if (join.conditions.length > 0) {
        const leftTables = getOperatorTables(join.children[0]);
        const rightTables = getOperatorTables(join.children[1]);
        edges.push({
          conditions: join.conditions,
          leftTables,
          rightTables,
        });
      }
      return;
    }
  }

  if (op.type === LogicalOperatorType.LOGICAL_CROSS_PRODUCT) {
    const cross = op as LogicalCrossProduct;
    extractJoinTree(cross.children[0], relations, edges);
    extractJoinTree(cross.children[1], relations, edges);
    return;
  }

  // Leaf relation (scan, subquery, etc.)
  relations.push({
    plan: op,
    tables: getOperatorTables(op),
    cardinality: Math.max(op.estimatedCardinality, 1),
  });
}

// ============================================================================
// Exhaustive enumeration for ≤6 relations
// ============================================================================

function exhaustiveJoinOrder(
  relations: Relation[],
  edges: JoinEdge[],
): Relation[] {
  if (relations.length <= 2) return relations;

  let bestOrder = relations;
  let bestCost = estimateJoinTreeCost(relations, edges);

  const permutations = getPermutations(relations);
  for (const perm of permutations) {
    const cost = estimateJoinTreeCost(perm, edges);
    if (cost < bestCost) {
      bestCost = cost;
      bestOrder = perm;
    }
  }

  return bestOrder;
}

// ============================================================================
// Greedy heuristic for >6 relations
// ============================================================================

function greedyJoinOrder(
  relations: Relation[],
  edges: JoinEdge[],
): Relation[] {
  const result: Relation[] = [];
  const remaining = [...relations];

  // Start with the smallest relation
  remaining.sort((a, b) => a.cardinality - b.cardinality);
  result.push(remaining.shift()!);

  while (remaining.length > 0) {
    const currentTables = new Set<number>();
    for (const r of result) {
      for (const t of r.tables) currentTables.add(t);
    }

    // Find the best next relation to join
    let bestIdx = 0;
    let bestCost = Infinity;

    for (let i = 0; i < remaining.length; i++) {
      const candidate = remaining[i];
      // Prefer relations that have join conditions with current set
      const hasEdge = edges.some(
        (e) =>
          (setsOverlap(e.leftTables, currentTables) &&
            setsOverlap(e.rightTables, candidate.tables)) ||
          (setsOverlap(e.rightTables, currentTables) &&
            setsOverlap(e.leftTables, candidate.tables)),
      );

      const cost = hasEdge
        ? candidate.cardinality
        : candidate.cardinality * 1000; // Penalize cross products

      if (cost < bestCost) {
        bestCost = cost;
        bestIdx = i;
      }
    }

    result.push(remaining.splice(bestIdx, 1)[0]);
  }

  return result;
}

// ============================================================================
// Cost estimation
// ============================================================================

function estimateJoinTreeCost(
  order: Relation[],
  edges: JoinEdge[],
): number {
  if (order.length <= 1) return 0;

  let totalCost = 0;
  let currentTables = new Set(order[0].tables);
  let currentCardinality = order[0].cardinality;

  for (let i = 1; i < order.length; i++) {
    const next = order[i];

    // Check if there's a join condition between current set and next
    const hasJoinCondition = edges.some(
      (e) =>
        (setsOverlap(e.leftTables, currentTables) &&
          setsOverlap(e.rightTables, next.tables)) ||
        (setsOverlap(e.rightTables, currentTables) &&
          setsOverlap(e.leftTables, next.tables)),
    );

    // Estimate join result cardinality
    let joinCardinality: number;
    if (hasJoinCondition) {
      // Count applicable join conditions between current set and next
      const numConditions = edges.reduce((count, e) => {
        const connects =
          (setsOverlap(e.leftTables, currentTables) &&
            setsOverlap(e.rightTables, next.tables)) ||
          (setsOverlap(e.rightTables, currentTables) &&
            setsOverlap(e.leftTables, next.tables));
        return connects ? count + e.conditions.length : count;
      }, 0);

      // Standard selectivity heuristic: each equi-join condition has
      // selectivity 1/max(left, right). Result ≈ (left * right) / max(left, right)^n
      // For single condition: ≈ min(left, right)
      const maxCard = Math.max(currentCardinality, next.cardinality);
      const crossProduct = currentCardinality * next.cardinality;
      const divisor = Math.pow(maxCard, numConditions);
      joinCardinality = Math.max(1, Math.round(crossProduct / divisor));
    } else {
      // Cross product: multiply cardinalities
      joinCardinality = currentCardinality * next.cardinality;
    }

    totalCost += joinCardinality;

    for (const t of next.tables) currentTables.add(t);
    currentCardinality = joinCardinality;
  }

  return totalCost;
}

// ============================================================================
// Reconstruct join tree from ordered relations
// ============================================================================

function reconstructJoinTree(
  order: Relation[],
  edges: JoinEdge[],
): LogicalOperator {
  if (order.length === 0) {
    throw new Error('Empty relation set in join order reconstruction');
  }
  if (order.length === 1) return order[0].plan;

  let result = order[0].plan;
  let resultTables = new Set(order[0].tables);

  for (let i = 1; i < order.length; i++) {
    const next = order[i];

    // Find applicable join conditions
    const applicableConditions: JoinCondition[] = [];
    for (const edge of edges) {
      const allLeft = [...edge.leftTables];
      const allRight = [...edge.rightTables];
      const combinedTables = new Set([...resultTables, ...next.tables]);

      // Check if all tables referenced by this edge are now available
      const leftAvailable =
        allLeft.every((t) => combinedTables.has(t));
      const rightAvailable =
        allRight.every((t) => combinedTables.has(t));

      if (leftAvailable && rightAvailable) {
        // Check that the edge actually connects the current result with the new relation
        const leftInResult = allLeft.some((t) => resultTables.has(t));
        const rightInNext = allRight.some((t) => next.tables.has(t));
        const rightInResult = allRight.some((t) => resultTables.has(t));
        const leftInNext = allLeft.some((t) => next.tables.has(t));

        if ((leftInResult && rightInNext) || (rightInResult && leftInNext)) {
          applicableConditions.push(...edge.conditions);
        }
      }
    }

    if (applicableConditions.length > 0) {
      const join: LogicalComparisonJoin = {
        type: LogicalOperatorType.LOGICAL_COMPARISON_JOIN,
        joinType: 'INNER',
        children: [result, next.plan],
        conditions: applicableConditions,
        expressions: [],
        types: [...result.types, ...next.plan.types],
        estimatedCardinality: Math.max(
          1,
          Math.round(
            (result.estimatedCardinality * next.plan.estimatedCardinality) /
              Math.pow(
                Math.max(result.estimatedCardinality, next.plan.estimatedCardinality),
                applicableConditions.length,
              ),
          ),
        ),
        getColumnBindings: () => {
          return [
            ...join.children[0].getColumnBindings(),
            ...join.children[1].getColumnBindings(),
          ];
        },
      };
      result = join;
    } else {
      // No join condition — cross product
      const cross: LogicalCrossProduct = {
        type: LogicalOperatorType.LOGICAL_CROSS_PRODUCT,
        children: [result, next.plan],
        expressions: [],
        types: [...result.types, ...next.plan.types],
        estimatedCardinality:
          result.estimatedCardinality * next.plan.estimatedCardinality,
        getColumnBindings: () => {
          return [
            ...cross.children[0].getColumnBindings(),
            ...cross.children[1].getColumnBindings(),
          ];
        },
      };
      result = cross;
    }

    for (const t of next.tables) resultTables.add(t);
  }

  return result;
}

// ============================================================================
// Helpers
// ============================================================================

function setsOverlap(a: Set<number>, b: Set<number>): boolean {
  for (const item of a) {
    if (b.has(item)) return true;
  }
  return false;
}

function getPermutations<T>(arr: T[]): T[][] {
  if (arr.length <= 1) return [arr];
  const result: T[][] = [];
  for (let i = 0; i < arr.length; i++) {
    const rest = [...arr.slice(0, i), ...arr.slice(i + 1)];
    const perms = getPermutations(rest);
    for (const perm of perms) {
      result.push([arr[i], ...perm]);
    }
  }
  return result;
}
