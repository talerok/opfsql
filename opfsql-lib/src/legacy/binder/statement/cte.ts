import type {
  CTENode,
  SelectNode,
  SelectStatement,
  SetOperationNode,
  TableReference,
  WithClause,
} from "../../../sql/parser/types.js";
import {
  SetOperationType,
  TableReferenceType,
} from "../../../sql/parser/types.js";
import { SemanticError } from "../../../errors.js";
import type { BindContext } from "../core/context.js";
import { BindError } from "../core/errors.js";
import type { BindScope } from "../core/scope.js";
import { detectAggregates } from "../expression/aggregate.js";
import type { LogicalOperator } from "../types.js";
import { LogicalOperatorType } from "../types.js";
import { NO_CLAUSES } from "./query-clauses.js";
import { bindQueryBody, bindQueryNode } from "./query-node.js";
import { bindSelect } from "./select.js";
import { isNumericType } from "../../../values/index.js";

export interface CTECollected {
  name: string;
  plan: LogicalOperator;
  index: number;
}

export function collectCTEs(
  ctx: BindContext,
  withClause: WithClause | null,
  scope: BindScope,
): CTECollected[] {
  const entries: CTECollected[] = [];
  const ctes = withClause?.ctes ?? [];
  for (const cteNode of ctes) {
    const name = cteNode.name;
    if (entries.some((e) => e.name.toLowerCase() === name.toLowerCase())) {
      throw new SemanticError(`WITH query name ${name} is used twice`);
    }
    const isRecursive =
      withClause?.isRecursive === true &&
      isSelfReferencing(name, cteNode.query);

    const cteIndex = ctx.nextTableIndex();
    const ctePlan = isRecursive
      ? bindRecursiveCTE(ctx, name, cteNode, cteIndex, scope)
      : bindNonRecursiveCTE(ctx, cteNode, scope);

    const aliases = cteNode.columnNames;
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
  cteNode: CTENode,
  scope: BindScope,
): LogicalOperator {
  const cteScope = scope.createChildScope();
  return bindQueryNode(ctx, cteNode.query, cteScope);
}

// ---------------------------------------------------------------------------
// Recursive CTE binding
// ---------------------------------------------------------------------------

function bindRecursiveCTE(
  ctx: BindContext,
  name: string,
  cteNode: CTENode,
  cteIndex: number,
  scope: BindScope,
): LogicalOperator {
  const body = cteNode.query.body;
  if (
    body.type !== "SET_OPERATION_NODE" ||
    (body.setOperationType !== SetOperationType.UNION &&
      body.setOperationType !== SetOperationType.UNION_ALL)
  ) {
    throw new BindError(`Recursive CTE "${name}" must use UNION or UNION ALL`);
  }
  const setOp = body as SetOperationNode;
  const isUnionAll = setOp.setOperationType === SetOperationType.UNION_ALL;

  // 1. Bind anchor (left side) — no self-reference visible; it sees a WITH
  // in the body, which belongs to the body's outermost node.
  const anchorScope = scope.createChildScope();
  const anchorClauses = { ...NO_CLAUSES, with: cteNode.query.with };
  const anchorPlan = bindQueryBody(ctx, setOp.left, anchorScope, anchorClauses);

  // 2. Register CTE in scope with anchor types so recursive term can reference it
  scope.addCTE(name, anchorPlan, cteIndex, cteNode.columnNames);

  // 3. Validate recursive term structure before binding
  validateRecursiveTerm(name, setOp.right);

  // 4. Bind recursive term (right side) — self-reference visible via scope
  const recScope = scope.createChildScope();
  const recPlan = bindSelect(ctx, setOp.right, recScope, NO_CLAUSES);

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
  const node = query.body;
  if (node.type !== "SET_OPERATION_NODE") return false;
  // Check if the right side of the UNION references the CTE name
  return selectNodeRefs(node.right, cteName);
}

function selectNodeRefs(node: SelectNode, name: string): boolean {
  if (node.from && tableRefRefs(node.from, name)) return true;
  return false;
}

function tableRefRefs(ref: TableReference, name: string): boolean {
  switch (ref.type) {
    case TableReferenceType.BASE_TABLE:
      return ref.table.toLowerCase() === name.toLowerCase();
    case TableReferenceType.JOIN:
      return tableRefRefs(ref.left, name) || tableRefRefs(ref.right, name);
    case TableReferenceType.SUBQUERY:
      return false; // Subqueries have their own scope
  }
}

// ---------------------------------------------------------------------------
// Recursive term validation (SQL standard restrictions)
// ---------------------------------------------------------------------------

function validateRecursiveTerm(cteName: string, node: SelectNode): void {
  if (node.groupBy.length > 0) {
    throw new BindError(
      `Recursive CTE "${cteName}": GROUP BY is not allowed in the recursive term`,
    );
  }
  if (node.having) {
    throw new BindError(
      `Recursive CTE "${cteName}": HAVING is not allowed in the recursive term`,
    );
  }
  // ORDER BY and LIMIT belong to the whole CTE body, never to a term.
  if (node.isDistinct) {
    throw new BindError(
      `Recursive CTE "${cteName}": DISTINCT is not allowed in the recursive term`,
    );
  }
  if (detectAggregates(node.selectList)) {
    throw new BindError(
      `Recursive CTE "${cteName}": aggregate functions are not allowed in the recursive term`,
    );
  }
}

// ---------------------------------------------------------------------------
// Type compatibility for anchor vs recursive term
// ---------------------------------------------------------------------------

function areTypesCompatible(anchor: string, recursive: string): boolean {
  if (anchor === recursive) return true;
  if (anchor === "NULL" || anchor === "ANY") return true;
  if (recursive === "NULL" || recursive === "ANY") return true;
  return isNumericType(anchor) && isNumericType(recursive);
}
