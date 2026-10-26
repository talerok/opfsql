import type { StaticType } from "../../functions/index.js";
import type { logical } from "../plan/index.js";
import type { Affinity } from "../../values/index.js";

// ---------------------------------------------------------------------------
// Parameter affinity (docs/semantics.md, Comparisons). Comparisons compare
// values as they are; the one conversion is a parameter's, put on the
// parameter node here, from the type of what it is compared with: a number
// type makes text that is exactly a number that number, TEXT makes a number
// text. Compared with several operands at once, a parameter converts only
// when they all agree. Nothing else converts: an ANY column compares as
// stored.
// ---------------------------------------------------------------------------

type Expression = logical.Expression;

/** `operand`, a parameter converted for comparison with values of `types`. */
export function comparedWith(
  operand: Expression,
  types: readonly StaticType[],
): Expression {
  if (operand.kind !== "parameter") return operand;
  return { ...operand, affinity: sharedAffinity(types) };
}

/** The affinity every one of `types` gives, or null if they disagree. */
function sharedAffinity(types: readonly StaticType[]): Affinity | null {
  const affinities = new Set(types.map((type) => affinityOf(type)));
  const [only = null] = affinities;
  return affinities.size === 1 ? only : null;
}

function affinityOf(type: StaticType): Affinity | null {
  switch (type) {
    case "INTEGER":
    case "REAL": {
      return "numeric";
    }
    case "TEXT": {
      return "text";
    }
    case "BLOB":
    case "BOOLEAN":
    case "JSON":
    case "ANY":
    case "NULL": {
      return null;
    }
  }
}
