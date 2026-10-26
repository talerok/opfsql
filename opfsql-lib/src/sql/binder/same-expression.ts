import { areEqual } from "../../values/index.js";
import { partsOf } from "./walk.js";
import type {
  AggregateCall,
  ColumnReference,
  JsonPathStep,
  logical,
} from "../plan/index.js";

// ---------------------------------------------------------------------------
// Structural equality of bound expressions: how a select item is matched to
// a GROUP BY expression, an ORDER BY key to an output column, and an
// aggregate call to one met before. A column is the same column by its
// binding alone — table indexes are unique in a statement — so a column met
// inside a subquery, a level further in, still matches. Subqueries never
// match: two are not known to give the same rows.
// ---------------------------------------------------------------------------

type Expression = logical.Expression;

export function isSameExpression(a: Expression, b: Expression): boolean {
  if (a.kind !== b.kind || a.type !== b.type) return false;
  return sameChildren(a, b) && sameDetails(a, b);
}

export function isSameAggregate(
  a: AggregateCall<logical.Plan>,
  b: AggregateCall<logical.Plan>,
): boolean {
  return (
    a.function === b.function &&
    a.isDistinct === b.isDistinct &&
    areSame(a.arguments, b.arguments)
  );
}

/** Pairwise the same, and as many. */
function areSame(a: readonly Expression[], b: readonly Expression[]): boolean {
  return (
    a.length === b.length &&
    a.every((expression, index) => {
      const other = b[index];
      return other !== undefined && isSameExpression(expression, other);
    })
  );
}

function sameChildren(a: Expression, b: Expression): boolean {
  const left = partsOf(a);
  const right = partsOf(b);
  return (
    left.length === right.length &&
    left.every((part, index) => samePart(part, right[index] ?? null))
  );
}

function samePart(a: Expression | null, b: Expression | null): boolean {
  if (a === null || b === null) return a === b;
  return isSameExpression(a, b);
}

/** What sets two nodes of one kind apart besides their sub-expressions. */
function sameDetails(a: Expression, b: Expression): boolean {
  switch (a.kind) {
    case "column": {
      return b.kind === "column" && sameBinding(a, b);
    }
    case "constant": {
      return b.kind === "constant" && areEqual(a.value, b.value);
    }
    case "parameter": {
      return b.kind === "parameter" && a.index === b.index;
    }
    case "comparison":
    case "arithmetic":
    case "conjunction": {
      return "operator" in b && a.operator === b.operator;
    }
    case "function": {
      return b.kind === "function" && a.function === b.function;
    }
    case "json-access": {
      return b.kind === "json-access" && sameJsonAccess(a, b);
    }
    case "scalar-subquery":
    case "exists":
    case "quantified-comparison": {
      return false;
    }
    case "not":
    case "is-null":
    case "negate":
    case "concatenate":
    case "in-list":
    case "between":
    case "case":
    case "cast": {
      return true;
    }
  }
}

function sameBinding(a: ColumnReference, b: ColumnReference): boolean {
  return (
    a.binding.tableIndex === b.binding.tableIndex &&
    a.binding.columnIndex === b.binding.columnIndex
  );
}

function sameJsonAccess(
  a: Extract<Expression, { kind: "json-access" }>,
  b: Extract<Expression, { kind: "json-access" }>,
): boolean {
  const isSamePath =
    a.path.length === b.path.length &&
    a.path.every((step, index) => sameStep(step, b.path[index]));
  return isSamePath && sameBinding(a.column, b.column);
}

function sameStep(a: JsonPathStep, b: JsonPathStep | undefined): boolean {
  if (a.kind === "key") return b?.kind === "key" && a.key === b.key;
  return b?.kind === "index" && a.index === b.index;
}
