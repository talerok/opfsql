import { SemanticError } from "../../../errors.js";
import { commonType, staticType } from "../../../functions/index.js";
import type { LogicalType } from "../../../store/types.js";

// ---------------------------------------------------------------------------
// Static types of operators. Function types come from the function
// registry (src/functions); these are the rules for what is not a function.
// ---------------------------------------------------------------------------

const NUMERIC_TYPES: ReadonlySet<LogicalType> = new Set<LogicalType>([
  "INTEGER",
  "BIGINT",
  "REAL",
]);

/** Types that say nothing about the value: it may be anything. */
const UNTYPED: ReadonlySet<LogicalType> = new Set<LogicalType>(["NULL", "ANY"]);

/** The type of two operands compared or combined by UNION; fails if none. */
export function checkTypeCompatibility(
  left: LogicalType,
  right: LogicalType,
): LogicalType {
  if (UNTYPED.has(left)) return right;
  if (left === right || UNTYPED.has(right)) return left;
  if (NUMERIC_TYPES.has(left) && NUMERIC_TYPES.has(right)) {
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
    if (!NUMERIC_TYPES.has(type) && !UNTYPED.has(type)) {
      throw new SemanticError(`Cannot perform arithmetic on ${type} type`);
    }
  }
  const [first = "NULL", second = first] = [left, right].filter(
    (type) => type !== "NULL",
  );
  if (first === "ANY" || second === "ANY") return "ANY";
  return first === "NULL" ? "NULL" : numericType(first, second);
}

/** The type of one of several expressions: CASE branches, COALESCE. */
export function resultType(types: readonly LogicalType[]): LogicalType {
  return commonType(types.map((type) => staticType(type)));
}

/** The wider of two numeric types: REAL over BIGINT over INTEGER. */
function numericType(left: LogicalType, right: LogicalType): LogicalType {
  const types = new Set([left, right]);
  return WIDEST_FIRST.find((type) => types.has(type)) ?? "INTEGER";
}

const WIDEST_FIRST: readonly LogicalType[] = ["REAL", "BIGINT"];
