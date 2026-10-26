import { NotSupportedError, SemanticError } from "../../errors.js";
import { nameKey } from "../../schema/index.js";
import {
  JoinType,
  SetOperationType,
  StatementType,
  TableReferenceType,
  type CTENode,
  type SelectNode,
  type SelectStatement,
  type SetOperationNode,
  type TableReference,
} from "../parser/index.js";
import { cteColumns, type BoundCte } from "./cte-definition.js";
import { hasAggregateCall } from "./grouping.js";
import { expressionsOf, mentions, mentionsInExpression } from "./references.js";
import type { OutputColumn } from "../plan/index.js";
import type { BoundQuery, CteDefinition, Scope } from "./scope.js";
import { sharedType } from "./type-rules.js";

// ---------------------------------------------------------------------------
// A recursive WITH query: `anchor UNION [ALL] step`. The anchor must not
// name the query; the step names it exactly once, in its own FROM — not in
// a subquery, not on the side of an outer join that is filled with NULLs —
// and reads there the rows the last run added: the working set, which the
// `recursive-cte` names with its own index. The `cte` around it names the
// full result. ORDER BY, LIMIT and aggregates in the step are not supported.
// A WITH clause of its own is bound before it (cte.ts).
// ---------------------------------------------------------------------------

/** A bound step whose working set was read with the types it produces. */
interface Step {
  readonly working: CteDefinition;
  readonly query: BoundQuery;
}

export function bindRecursiveCte(cte: CTENode, scope: Scope): BoundCte {
  const body = recursiveBody(cte);
  checkStep(body.right, cte.name);
  const { context } = scope;
  const definitionScope = scope.sibling();
  const anchorScope = definitionScope.sibling();
  const anchor = context.bindQuery(asQuery(body.left), anchorScope);
  const columns = cteColumns(cte, anchor.columns);
  const step = bindStep(body.right, cte, definitionScope, columns);
  const recursive = {
    kind: "recursive-cte" as const,
    cteIndex: step.working.cteIndex,
    name: cte.name,
    tableIndex: context.newIndex(),
    anchor: anchor.plan,
    step: step.query.plan,
    isUnionAll: body.setOperationType === SetOperationType.UNION_ALL,
    types: step.working.columns.map((column) => column.type),
  };
  const definition = {
    name: cte.name,
    cteIndex: context.newIndex(),
    columns: step.working.columns,
    reach: definitionScope.reach,
  };
  return { definition, plan: recursive };
}

/** The `anchor UNION [ALL] step` a recursive query must be. */
function recursiveBody(cte: CTENode): SetOperationNode {
  const { query, name } = cte;
  if (query.orderBy.length > 0) {
    throw new NotSupportedError("ORDER BY in a recursive WITH query");
  }
  if (query.limit !== null) {
    throw new NotSupportedError("LIMIT in a recursive WITH query");
  }
  const { body } = query;
  const isUnion =
    body.type === "SET_OPERATION_NODE" &&
    (body.setOperationType === SetOperationType.UNION ||
      body.setOperationType === SetOperationType.UNION_ALL);
  if (!isUnion) {
    throw new SemanticError(
      `recursive WITH query ${name} must have the form anchor UNION [ALL] step`,
    );
  }
  if (mentions(asQuery(body.left), name)) {
    throw new SemanticError(
      `the anchor of recursive WITH query ${name} names ${name}`,
    );
  }
  return body;
}

/**
 * Binds the step reading a working set of `columns`, again with wider types
 * while what it produces does not fit them: a step that adds 0.5 to an
 * INTEGER anchor makes the column REAL, and the step must read it so.
 */
function bindStep(
  node: SelectNode,
  cte: CTENode,
  scope: Scope,
  columns: readonly OutputColumn[],
): Step {
  const { context } = scope;
  // The working set is read only by the step, never by a subquery in it.
  const working = {
    name: cte.name,
    cteIndex: context.newIndex(),
    columns,
    reach: null,
  };
  const stepScope = scope.sibling().withCte(working);
  const query = context.bindQuery(asQuery(node), stepScope);
  if (query.columns.length !== columns.length) {
    throw new SemanticError(
      `the anchor and step of recursive WITH query ${cte.name} must have as many columns: ${String(columns.length)} and ${String(query.columns.length)}`,
    );
  }
  const widened = columns.map((column, position) => ({
    name: column.name,
    type: sharedType([
      column.type,
      query.columns[position]?.type ?? column.type,
    ]),
  }));
  const isStable = widened.every(
    (column, position) => column.type === columns[position]?.type,
  );
  return isStable ? { working, query } : bindStep(node, cte, scope, widened);
}

/** The step's shape: aggregates aside, it names the query once, plainly. */
function checkStep(step: SelectNode, name: string): void {
  const hasAggregate =
    step.groupBy.length > 0 ||
    step.having !== null ||
    step.selectList.some((item) => hasAggregateCall(item));
  if (hasAggregate) {
    throw new NotSupportedError(
      "an aggregate in the step of a recursive WITH query",
    );
  }
  if (step.isDistinct) {
    throw new NotSupportedError(
      "DISTINCT in the step of a recursive WITH query",
    );
  }
  for (const expression of expressionsOf(step)) {
    if (mentionsInExpression(expression, name)) throw inSubquery(name);
  }
  const references =
    step.from === null ? [] : [...referencesIn(step.from, name, false)];
  const [reference, other] = references;
  if (reference === undefined || other !== undefined) {
    throw new SemanticError(
      `the step of recursive WITH query ${name} must name ${name} exactly once`,
    );
  }
  if (reference.isNullable) {
    throw new SemanticError(
      `the step of recursive WITH query ${name} names it on the side of an outer join that is filled with NULLs`,
    );
  }
}

/** Each FROM item named `name`, and whether an outer join fills it with NULLs. */
function* referencesIn(
  from: TableReference,
  name: string,
  isNullable: boolean,
): Generator<{ readonly isNullable: boolean }> {
  switch (from.type) {
    case TableReferenceType.BASE_TABLE: {
      if (nameKey(from.table) === nameKey(name)) yield { isNullable };
      return;
    }
    case TableReferenceType.SUBQUERY: {
      if (mentions(from.subquery, name)) throw inSubquery(name);
      return;
    }
    case TableReferenceType.JOIN: {
      const isRight = from.joinType === JoinType.RIGHT;
      const isLeft = from.joinType === JoinType.LEFT;
      yield* referencesIn(from.left, name, isNullable || isRight);
      yield* referencesIn(from.right, name, isNullable || isLeft);
    }
  }
}

/** The step reads only the working set its FROM names: a subquery would keep the first. */
function inSubquery(name: string): SemanticError {
  return new SemanticError(`recursive reference in a subquery: ${name}`);
}

/** A compound's member as a query of its own. */
function asQuery(body: SelectNode | SetOperationNode): SelectStatement {
  return {
    type: StatementType.SELECT_STATEMENT,
    with: null,
    body,
    orderBy: [],
    limit: null,
  };
}
