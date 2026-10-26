import { BOOLEANS, INTEGERS, REALS, TEXTS } from "./data.js";
import {
  constant,
  type Expression,
  type ExpressionType,
  type Leaf,
  node,
} from "./expression.js";
import type { Random } from "./random.js";

// ---------------------------------------------------------------------------
// Conditions an access path can use: keys of indexes, the rowid and UNIQUE
// columns, compared with constants — one key, or two of them together.
// ---------------------------------------------------------------------------

const RANGES = ["<", "<=", ">", ">="];

/** A condition on `keys`, or null when there are none. */
export function seekableCondition(
  random: Random,
  keys: readonly Leaf[],
): Expression | null {
  if (keys.length === 0) return null;
  const first = keyCondition(random, random.pick(keys));
  if (!random.chance(0.4)) return first;
  const operator = random.chance(0.75) ? " AND " : " OR ";
  return node(
    "boolean",
    first,
    operator,
    keyCondition(random, random.pick(keys)),
  );
}

function keyCondition(random: Random, key: Leaf): Expression {
  const value = (): Expression =>
    constant(key.type, constantSql(random, key.type));
  switch (random.below(5)) {
    case 0: {
      return node("boolean", key, " = ", value());
    }
    case 1: {
      return node("boolean", key, ` ${random.pick(RANGES)} `, value());
    }
    case 2: {
      return node("boolean", key, " BETWEEN ", value(), " AND ", value());
    }
    case 3: {
      return node("boolean", key, " IN (", value(), ", ", value(), ")");
    }
    default: {
      return node(
        "boolean",
        key,
        random.chance(0.5) ? " IS NULL" : " IS NOT NULL",
      );
    }
  }
}

/** A constant from the values the rows hold, of a type `=` takes beside `type`. */
function constantSql(random: Random, type: ExpressionType): string {
  switch (type) {
    case "integer":
    case "real": {
      return random.pick([...INTEGERS, ...REALS]);
    }
    case "text": {
      return random.pick(TEXTS);
    }
    case "boolean": {
      return random.pick(BOOLEANS);
    }
    case "untyped": {
      return random.pick([...INTEGERS, ...TEXTS, ...BOOLEANS]);
    }
  }
}
