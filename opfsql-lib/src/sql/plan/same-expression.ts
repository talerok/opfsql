import type { JsonPathStep } from "../../functions/index.js";
import { compareValues, kindOf, type Value } from "../../values/index.js";
import {
  type ColumnReference,
  type Expression,
  isSameColumn,
  type Parameter,
} from "./expression.js";
import { childrenOf } from "./walk.js";

// ---------------------------------------------------------------------------
// Structural equality of bound expressions: the binder matches a select item
// to a GROUP BY expression and an ORDER BY key to an output column, the
// optimizer an index key to a condition or an ORDER BY key. Names as written
// are ignored (the binding says which column); types are compared, since
// they decide conversions, and so is a parameter's affinity. A subquery is
// never the same as anything: its plan is not compared, so claiming
// sameness would be a guess.
// ---------------------------------------------------------------------------

/** When two column references are one column. */
export type ColumnMatch = (a: ColumnReference, b: ColumnReference) => boolean;

/** The same binding, at the same query level. */
export function isSameColumnAtLevel(
  a: ColumnReference,
  b: ColumnReference,
): boolean {
  return isSameColumn(a.binding, b.binding) && a.depth === b.depth;
}

/** The same binding, at any level: table indexes are unique in a statement. */
export function isSameColumnAnyLevel(
  a: ColumnReference,
  b: ColumnReference,
): boolean {
  return isSameColumn(a.binding, b.binding);
}

export function isSameExpression<A, B>(
  a: Expression<A>,
  b: Expression<B>,
  areSameColumn: ColumnMatch = isSameColumnAtLevel,
): boolean {
  if (!sameNode(a, b, areSameColumn)) return false;
  const left = childrenOf(a);
  const right = childrenOf(b);
  if (left.length !== right.length) return false;
  return left.every((child, position) => {
    const other = right[position];
    return other !== undefined && isSameExpression(child, other, areSameColumn);
  });
}

/** True if the nodes themselves agree, their children aside. */
function sameNode<A, B>(
  a: Expression<A>,
  b: Expression<B>,
  areSameColumn: ColumnMatch,
): boolean {
  if (a.kind !== b.kind || a.type !== b.type) return false;
  switch (a.kind) {
    case "column": {
      return b.kind === "column" && areSameColumn(a, b);
    }
    case "constant": {
      return b.kind === "constant" && sameValue(a.value, b.value);
    }
    case "parameter": {
      return b.kind === "parameter" && isSameParameter(a, b);
    }
    case "comparison": {
      return b.kind === "comparison" && a.operator === b.operator;
    }
    case "arithmetic": {
      return b.kind === "arithmetic" && a.operator === b.operator;
    }
    case "conjunction": {
      return b.kind === "conjunction" && a.operator === b.operator;
    }
    case "function": {
      return b.kind === "function" && a.function.name === b.function.name;
    }
    case "case": {
      return b.kind === "case" && sameCaseShape(a, b);
    }
    case "json-access": {
      return b.kind === "json-access" && samePath(a.path, b.path);
    }
    case "not":
    case "is-null":
    case "negate":
    case "concatenate":
    case "in-list":
    case "between":
    case "cast": {
      return true;
    }
    case "scalar-subquery":
    case "exists":
    case "quantified-comparison": {
      return false;
    }
  }
}

/** One parameter, converted the same way. */
function isSameParameter(a: Parameter, b: Parameter): boolean {
  return a.index === b.index && a.affinity === b.affinity;
}

/** Equal and of one kind: `1` and `1.0` are one value, `true` and `1` two. */
function sameValue(a: Value, b: Value): boolean {
  return kindOf(a) === kindOf(b) && compareValues(a, b) === 0;
}

/** Children line up only when the optional parts are present in both. */
function sameCaseShape<A, B>(
  a: Extract<Expression<A>, { readonly kind: "case" }>,
  b: Extract<Expression<B>, { readonly kind: "case" }>,
): boolean {
  return (
    (a.operand === null) === (b.operand === null) &&
    a.branches.length === b.branches.length &&
    (a.otherwise === null) === (b.otherwise === null)
  );
}

function samePath(
  a: readonly JsonPathStep[],
  b: readonly JsonPathStep[],
): boolean {
  if (a.length !== b.length) return false;
  return a.every((step, position) => {
    const other = b[position];
    if (other === undefined) return false;
    if (step.kind === "key") {
      return other.kind === "key" && other.key === step.key;
    }
    return other.kind === "index" && other.index === step.index;
  });
}
