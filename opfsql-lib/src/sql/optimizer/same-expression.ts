import {
  childrenOf,
  type ColumnReference,
  type Expression,
  type JsonPathStep,
} from "../plan/index.js";
import { compareValues, kindOf, type Value } from "../../values/index.js";

// ---------------------------------------------------------------------------
// Structural equality of expressions: an index key matches a condition or an
// ORDER BY key written the same way. Names as written are ignored (the
// binding says which column); types are compared, since they decide
// conversions. A subquery is never the same as anything: its plan is not
// compared, so claiming sameness would be a guess.
// ---------------------------------------------------------------------------

export function sameExpression<A, B>(
  a: Expression<A>,
  b: Expression<B>,
): boolean {
  if (!sameNode(a, b)) return false;
  const left = childrenOf(a);
  const right = childrenOf(b);
  if (left.length !== right.length) return false;
  return left.every((child, position) => {
    const other = right[position];
    return other !== undefined && sameExpression(child, other);
  });
}

/** True if the nodes themselves agree, their children aside. */
function sameNode<A, B>(a: Expression<A>, b: Expression<B>): boolean {
  if (a.kind !== b.kind || a.type !== b.type) return false;
  switch (a.kind) {
    case "column": {
      return b.kind === "column" && sameColumn(a, b);
    }
    case "constant": {
      return b.kind === "constant" && sameValue(a.value, b.value);
    }
    case "parameter": {
      return b.kind === "parameter" && a.index === b.index;
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

function sameColumn(a: ColumnReference, b: ColumnReference): boolean {
  const isSameBinding =
    a.binding.tableIndex === b.binding.tableIndex &&
    a.binding.columnIndex === b.binding.columnIndex;
  return isSameBinding && a.depth === b.depth;
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
