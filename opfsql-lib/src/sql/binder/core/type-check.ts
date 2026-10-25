import { SemanticError } from "../../../errors.js";
import { argumentTypes, commonType } from "../../../functions/index.js";
import type { LogicalType } from "../../../store/types.js";
import { isNumericType } from "../../../values/index.js";

// ---------------------------------------------------------------------------
// Static types of operators. Function types come from the function
// registry (src/functions); these are the rules for what is not a function.
// ---------------------------------------------------------------------------

/** Types that say nothing about the value: it may be anything. */
const UNTYPED: ReadonlySet<LogicalType> = new Set<LogicalType>(["NULL", "ANY"]);

/** Numeric types from the widest down; INTEGER, the narrowest, is implied. */
const WIDEST_FIRST: readonly LogicalType[] = ["REAL", "BIGINT"];

/** The type of two operands compared or combined by UNION; fails if none. */
export function checkTypeCompatibility(
  left: LogicalType,
  right: LogicalType,
): LogicalType {
  if (UNTYPED.has(left)) return right;
  if (left === right || UNTYPED.has(right)) return left;
  if (isNumericType(left) && isNumericType(right)) {
    return numericType(left, right);
  }
  // JSON access may hold a scalar of any kind at run time.
  if (left === "JSON" || right === "JSON") return "JSON";
  throw new SemanticError(`Type mismatch: cannot compare ${left} and ${right}`);
}

/**
 * The type of `left <op> right` for + - * / %: INTEGER for two INTEGERs,
 * REAL with a REAL, ANY when an operand's type is only known at run time
 * (the values then decide, see docs/semantics.md, Arithmetic).
 */
export function resolveArithmeticType(
  left: LogicalType,
  right: LogicalType,
): LogicalType {
  for (const type of [left, right]) {
    if (!isNumericType(type) && !UNTYPED.has(type)) {
      throw new SemanticError(`Cannot perform arithmetic on ${type} type`);
    }
  }
  if (left === "ANY" || right === "ANY") return "ANY";
  // NULL operands make the result NULL: the other side gives the type.
  if (left === "NULL") return right;
  if (right === "NULL") return left;
  return numericType(left, right);
}

/** The type of one of several expressions: CASE branches, COALESCE. */
export function resultType(
  expressions: readonly { readonly returnType: LogicalType }[],
): LogicalType {
  return commonType(argumentTypes(expressions));
}

/** The wider of two numeric types: REAL over BIGINT over INTEGER. */
function numericType(left: LogicalType, right: LogicalType): LogicalType {
  const types = new Set([left, right]);
  return WIDEST_FIRST.find((type) => types.has(type)) ?? "INTEGER";
}
