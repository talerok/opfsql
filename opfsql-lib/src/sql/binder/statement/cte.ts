import type {
  SelectNode,
  SelectStatement,
  SetOperationNode,
  TableRef,
} from "../../parser/types.js";
import {
  ResultModifierType,
  SetOperationType,
  TableRefType,
} from "../../parser/types.js";
import type { BindContext } from "../core/context.js";
import { BindError } from "../core/errors.js";
import type { BindScope } from "../core/scope.js";
import { detectAggregates } from "../expression/aggregate.js";
import type { LogicalOperator } from "../types.js";
import { LogicalOperatorType } from "../types.js";
import { bindQueryNode } from "./query-node.js";
import { bindSelect } from "./select.js";

export interface CTECollected {
  name: string;
  plan: LogicalOperator;
  index: number;
}

export function collectCTEs(
  ctx: BindContext,
  cteMap: {
    map: Record<string, { query: SelectStatement; aliases: string[] }>;
    recursive: boolean;
  },
  scope: BindScope,
): CTECollected[] {
  const entries: CTECollected[] = [];
  for (const [name, cteNode] of Object.entries(cteMap.map)) {
    const isRecursive =
      cteMap.recursive && isSelfReferencing(name, cteNode.query);

    const cteIndex = ctx.nextTableIndex();
    const ctePlan = isRecursive
      ? bindRecursiveCTE(ctx, name, cteNode, cteIndex, scope)
      : bindNonRecursiveCTE(ctx, cteNode, scope);

    const aliases = cteNode.aliases;
    if (aliases.length > 0 && aliases.length !== ctePlan.types.length) {
      throw new BindError(
        `CTE "${name}" has ${ctePlan.types.length} columns but ${aliases.length} column aliases were provided`,
      );
    }

    scope.addCTE(name, ctePlan, cteIndex, aliases);
    entries.push({ name, plan: ctePlan, index: cteIndex });
  }
  return entries;
}

// ---------------------------------------------------------------------------
// Non-recursive CTE binding (unchanged from before)
// ---------------------------------------------------------------------------

function bindNonRecursiveCTE(
  ctx: BindContext,
  cteNode: { query: SelectStatement; aliases: string[] },
  scope: BindScope,
): LogicalOperator {
  const cteScope = scope.createChildScope();
  return bindQueryNode(ctx, cteNode.query.node, cteScope);
}

// ---------------------------------------------------------------------------
// Recursive CTE binding
// ---------------------------------------------------------------------------

function bindRecursiveCTE(
  ctx: BindContext,
  name: string,
  cteNode: { query: SelectStatement; aliases: string[] },
  cteIndex: number,
  scope: BindScope,
): LogicalOperator {
  const body = cteNode.query.node;
  if (body.type !== "SET_OPERATION_NODE") {
    throw new BindError(`Recursive CTE "${name}" must use UNION or UNION ALL`);
  }
  const setOp = body as SetOperationNode;
  const isUnionAll = setOp.set_op_type === SetOperationType.UNION_ALL;

  // 1. Bind anchor (left side) — no self-reference visible
  const anchorScope = scope.createChildScope();
  const anchorPlan = bindQueryNode(ctx, setOp.left, anchorScope);

  // 2. Register CTE in scope with anchor types so recursive term can reference it
  scope.addCTE(name, anchorPlan, cteIndex, cteNode.aliases);

  // 3. Validate recursive term structure before binding
  validateRecursiveTerm(name, setOp.right);

  // 4. Bind recursive term (right side) — self-reference visible via scope
  const recScope = scope.createChildScope();
  const recPlan = bindSelect(ctx, setOp.right, recScope);

  // 5. Validate column count and type compatibility
  if (anchorPlan.types.length !== recPlan.types.length) {
    throw new BindError(
      `Recursive CTE "${name}" anchor has ${anchorPlan.types.length} columns but recursive term has ${recPlan.types.length}`,
    );
  }
  for (let i = 0; i < anchorPlan.types.length; i++) {
    if (!areTypesCompatible(anchorPlan.types[i], recPlan.types[i])) {
      throw new BindError(
        `Recursive CTE "${name}" column ${i + 1}: anchor type ${anchorPlan.types[i]} is incompatible with recursive term type ${recPlan.types[i]}`,
      );
    }
  }

  const bindings = anchorPlan.columnBindings;

  return {
    type: LogicalOperatorType.LOGICAL_RECURSIVE_CTE,
    cteName: name,
    cteIndex,
    children: [anchorPlan, recPlan],
    isUnionAll,
    expressions: [],
    types: anchorPlan.types,
    estimatedCardinality: 0,
    columnBindings: bindings,
  };
}

// ---------------------------------------------------------------------------
// Self-reference detection
// ---------------------------------------------------------------------------

function isSelfReferencing(cteName: string, query: SelectStatement): boolean {
  const node = query.node;
  if (node.type !== "SET_OPERATION_NODE") return false;
  // Check if the right side of the UNION references the CTE name
  return selectNodeRefs(node.right, cteName);
}

function selectNodeRefs(node: SelectNode, name: string): boolean {
  if (node.from_table && tableRefRefs(node.from_table, name)) return true;
  return false;
}

function tableRefRefs(ref: TableRef, name: string): boolean {
  switch (ref.type) {
    case TableRefType.BASE_TABLE:
      return ref.table_name.toLowerCase() === name.toLowerCase();
    case TableRefType.JOIN:
      return tableRefRefs(ref.left, name) || tableRefRefs(ref.right, name);
    case TableRefType.SUBQUERY:
      return false; // Subqueries have their own scope
  }
}

// ---------------------------------------------------------------------------
// Recursive term validation (SQL standard restrictions)
// ---------------------------------------------------------------------------

function validateRecursiveTerm(cteName: string, node: SelectNode): void {
  if (node.groups.group_expressions.length > 0) {
    throw new BindError(
      `Recursive CTE "${cteName}": GROUP BY is not allowed in the recursive term`,
    );
  }
  if (node.having) {
    throw new BindError(
      `Recursive CTE "${cteName}": HAVING is not allowed in the recursive term`,
    );
  }
  for (const mod of node.modifiers) {
    if (mod.type === ResultModifierType.DISTINCT_MODIFIER) {
      throw new BindError(
        `Recursive CTE "${cteName}": DISTINCT is not allowed in the recursive term`,
      );
    }
    if (mod.type === ResultModifierType.ORDER_MODIFIER) {
      throw new BindError(
        `Recursive CTE "${cteName}": ORDER BY is not allowed in the recursive term`,
      );
    }
    if (mod.type === ResultModifierType.LIMIT_MODIFIER) {
      throw new BindError(
        `Recursive CTE "${cteName}": LIMIT is not allowed in the recursive term`,
      );
    }
  }
  if (detectAggregates(node.select_list)) {
    throw new BindError(
      `Recursive CTE "${cteName}": aggregate functions are not allowed in the recursive term`,
    );
  }
}

// ---------------------------------------------------------------------------
// Type compatibility for anchor vs recursive term
// ---------------------------------------------------------------------------

const NUMERIC_TYPES = new Set(["INTEGER", "BIGINT", "REAL"]);

function areTypesCompatible(anchor: string, recursive: string): boolean {
  if (anchor === recursive) return true;
  if (anchor === "NULL" || anchor === "ANY") return true;
  if (recursive === "NULL" || recursive === "ANY") return true;
  if (NUMERIC_TYPES.has(anchor) && NUMERIC_TYPES.has(recursive)) return true;
  return false;
}
