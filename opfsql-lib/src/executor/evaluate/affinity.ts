import type { LogicalType } from "../../sql/binder/types.js";
import {
  type Affinity,
  applyAffinity,
  type Value,
} from "../../values/index.js";

// ---------------------------------------------------------------------------
// Comparison affinity (docs/semantics.md, Comparisons): an operand whose
// type is known only at run time (ANY: a parameter, a mixed CASE) is
// converted to the type of the side it is compared with — `id = $1` with
// '5' finds id 5, `name = $1` with 5 finds '5'. Typed operands of different
// kinds are a bind error, and JSON values compare as they are.
// ---------------------------------------------------------------------------

/** A conversion of one side's values before comparing them. */
export type Conversion = (value: Value) => Value;

const unchanged: Conversion = (value) => value;

/** The affinity a static type gives the other side of a comparison. */
export function affinityOf(type: LogicalType): Affinity | null {
  switch (type) {
    case "INTEGER":
    case "BIGINT":
    case "REAL": {
      return "numeric";
    }
    case "TEXT": {
      return "text";
    }
    case "BLOB":
    case "BOOLEAN":
    case "JSON":
    case "NULL":
    case "ANY": {
      return null;
    }
  }
}

/** How to convert a side of type `type` compared with a side of `other`. */
export function comparisonConversion(
  type: LogicalType,
  other: LogicalType,
): Conversion {
  const affinity = type === "ANY" ? affinityOf(other) : null;
  if (affinity === null) return unchanged;
  return (value) => applyAffinity(value, affinity);
}
