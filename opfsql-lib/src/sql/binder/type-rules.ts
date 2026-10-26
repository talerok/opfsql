import { SemanticError } from "../../errors.js";
import type { StaticType } from "../../functions/index.js";
import { typeFromName, type SqlType } from "../../values/index.js";

// ---------------------------------------------------------------------------
// Static types of what is not a function call (functions type their calls
// in the registry). One rule each, and they agree with the expression
// compiler: typed operands of different kinds are a bind error; a side whose
// type is known only at run time (ANY) takes the other side's affinity
// there, and a JSON value compares as it is (docs/semantics.md, Comparisons).
// ---------------------------------------------------------------------------

/** Types that say nothing about the value: it may be anything. */
const UNTYPED: ReadonlySet<StaticType> = new Set<StaticType>(["NULL", "ANY"]);

const NUMBERS: ReadonlySet<StaticType> = new Set<StaticType>([
  "INTEGER",
  "REAL",
]);

/** What a condition may be: a truth, or a value whose kind only a row tells. */
const CONDITIONS: ReadonlySet<StaticType> = new Set<StaticType>([
  "BOOLEAN",
  "NULL",
  "ANY",
  "JSON",
]);

/**
 * Two operands that are compared, or meet in one column (CASE, UNION, IN):
 * numbers with numbers, a kind with itself, anything with an untyped or JSON
 * side. A JSON value at a path may hold any scalar at run time.
 */
export function checkComparable(left: StaticType, right: StaticType): void {
  const isCompatible =
    UNTYPED.has(left) ||
    UNTYPED.has(right) ||
    left === right ||
    (NUMBERS.has(left) && NUMBERS.has(right)) ||
    left === "JSON" ||
    right === "JSON";
  if (!isCompatible) {
    throw new SemanticError(
      `type mismatch: cannot compare ${left} with ${right}`,
    );
  }
}

/**
 * `left <op> right` for + - * / %: INTEGER for two INTEGERs, REAL with a
 * REAL, ANY when an operand's type is known only at run time (the values
 * decide then); a NULL operand leaves the other's type.
 */
export function arithmeticType(
  left: StaticType,
  right: StaticType,
): StaticType {
  checkNumeric(left);
  checkNumeric(right);
  const isDynamic = [left, right].some(
    (type) => type === "ANY" || type === "JSON",
  );
  if (isDynamic) return "ANY";
  if (left === "NULL") return right;
  if (right === "NULL") return left;
  return left === "INTEGER" && right === "INTEGER" ? "INTEGER" : "REAL";
}

/** `-x`: a number keeps its type; an untyped value is decided per row. */
export function negationType(type: StaticType): StaticType {
  checkNumeric(type);
  return type === "JSON" ? "ANY" : type;
}

/** `a || b` is TEXT; a BLOB has no text to join. */
export function concatenationType(
  left: StaticType,
  right: StaticType,
): StaticType {
  if (left === "BLOB" || right === "BLOB") {
    throw new SemanticError("cannot concatenate a BLOB with ||");
  }
  return "TEXT";
}

/**
 * A condition — WHERE, ON, HAVING, a searched CASE's WHEN, an operand of
 * AND, OR and NOT — is BOOLEAN; an untyped value is checked per row.
 */
export function checkCondition(type: StaticType, where: string): void {
  if (!CONDITIONS.has(type)) {
    throw new SemanticError(`${where} must be BOOLEAN, not ${type}`);
  }
}

/** A declared type (docs/semantics.md, Column types): none is ANY. */
export function declaredType(name: string | null): SqlType {
  if (name === null) return "ANY";
  const type = typeFromName(name);
  if (type === null) throw new SemanticError(`unknown type: ${name}`);
  return type;
}

/** The type stored for a value of static type `type`: NULL says nothing, so ANY. */
export function storedType(type: StaticType): SqlType {
  return type === "NULL" ? "ANY" : type;
}

function checkNumeric(type: StaticType): void {
  const isNumeric = NUMBERS.has(type) || UNTYPED.has(type) || type === "JSON";
  if (!isNumeric) {
    throw new SemanticError(`cannot do arithmetic on ${type}`);
  }
}
