import type { Value } from "../../../values/index.js";
import type { ArithmeticOperator, logical } from "../../plan/index.js";
import {
  between,
  call,
  cast,
  column,
  compare,
  constant,
  inList,
  not,
  nullCheck,
  parameter,
  users,
} from "./builders.js";
import type { Random } from "./random.js";

// Random well-typed expressions over users, for the printer's round trip
// (print-round-trip.test.ts): every operator the printer knows, nested in
// every order, so every precedence and associativity decision is met. Types
// are kept as the binder would accept them — numbers with numbers, text
// with text, conditions where a condition goes.

type Expression = logical.Expression;

/** users as `u`, so columns print as `u.age`. */
const u = { tableIndex: 1, table: users, alias: "u" };

const NUMBERS: readonly Value[] = [0, 1, 5, -3, 2.5, -1.5, 1e300];
const TEXTS: readonly Value[] = ["bob", "it's", "", "5"];
const COMPARISONS = ["=", "<>", "<", "<=", ">", ">="] as const;
const ARITHMETIC: readonly ArithmeticOperator[] = [
  "ADD",
  "SUBTRACT",
  "MULTIPLY",
  "DIVIDE",
  "MOD",
];

/** A condition, nested at most `depth` levels. */
export function booleanExpression(random: Random, depth: number): Expression {
  if (depth <= 0) return comparison(random, 0);
  const next = (): Expression => booleanExpression(random, depth - 1);
  switch (random.below(8)) {
    case 0: {
      return conjoin("AND", next(), next());
    }
    case 1: {
      return conjoin("OR", next(), next());
    }
    case 2: {
      return not(next());
    }
    case 3: {
      return random.chance(0.5)
        ? nullCheck(value(random, depth))
        : not(nullCheck(value(random, depth)));
    }
    case 4: {
      return listed(random, depth);
    }
    case 5: {
      return ranged(random, depth);
    }
    case 6: {
      return compare(next(), random.pick(["=", "<>"] as const), next());
    }
    default: {
      return comparison(random, depth);
    }
  }
}

/** `left AND right` or `left OR right`, flattened as the binder flattens them. */
function conjoin(
  operator: "AND" | "OR",
  left: Expression,
  right: Expression,
): Expression {
  const operands = [left, right].flatMap((operand) =>
    operand.kind === "conjunction" && operand.operator === operator
      ? operand.operands
      : [operand],
  );
  return { kind: "conjunction", operator, operands, type: "BOOLEAN" };
}

function comparison(random: Random, depth: number): Expression {
  const symbol = random.pick(COMPARISONS);
  if (random.chance(0.5))
    return compare(number(random, depth), symbol, number(random, depth));
  return compare(text(random, depth), symbol, text(random, depth));
}

function listed(random: Random, depth: number): Expression {
  const items = Array.from({ length: 1 + random.below(3) }, () =>
    number(random, depth - 1),
  );
  const listing = inList(number(random, depth - 1), ...items);
  return random.chance(0.5) ? listing : not(listing);
}

function ranged(random: Random, depth: number): Expression {
  const range = between(
    number(random, depth - 1),
    number(random, depth - 1),
    number(random, depth - 1),
  );
  return random.chance(0.5) ? range : not(range);
}

/** A number or a text value. */
function value(random: Random, depth: number): Expression {
  return random.chance(0.5)
    ? number(random, depth - 1)
    : text(random, depth - 1);
}

/** A number, nested at most `depth` levels. */
export function number(random: Random, depth: number): Expression {
  if (depth <= 0 || random.chance(0.3)) return numberLeaf(random);
  const next = (): Expression => number(random, depth - 1);
  switch (random.below(5)) {
    case 0: {
      return negate(next());
    }
    case 1: {
      return call("abs", next());
    }
    case 2: {
      return caseWhen(booleanExpression(random, depth - 1), next(), next());
    }
    default: {
      return arithmetic(random.pick(ARITHMETIC), next(), next());
    }
  }
}

function numberLeaf(random: Random): Expression {
  switch (random.below(4)) {
    case 0: {
      return column(u, "age");
    }
    case 1: {
      return column(u, "id");
    }
    case 2: {
      return parameter(random.below(3));
    }
    default: {
      return constant(random.pick(NUMBERS));
    }
  }
}

/** A text value, nested at most `depth` levels. */
function text(random: Random, depth: number): Expression {
  if (depth <= 0 || random.chance(0.4)) return textLeaf(random);
  const next = (): Expression => text(random, depth - 1);
  switch (random.below(3)) {
    case 0: {
      return call("lower", next());
    }
    case 1: {
      return cast(number(random, depth - 1), "TEXT");
    }
    default: {
      return { kind: "concatenate", left: next(), right: next(), type: "TEXT" };
    }
  }
}

function textLeaf(random: Random): Expression {
  if (random.chance(0.4)) return column(u, random.pick(["name", "email"]));
  return constant(random.pick(TEXTS));
}

function arithmetic(
  operator: ArithmeticOperator,
  left: Expression,
  right: Expression,
): Expression {
  return { kind: "arithmetic", operator, left, right, type: "ANY" };
}

function negate(operand: Expression): Expression {
  return { kind: "negate", operand, type: operand.type };
}

function caseWhen(
  when: Expression,
  result: Expression,
  otherwise: Expression,
): Expression {
  return {
    kind: "case",
    operand: null,
    branches: [{ when, result }],
    otherwise,
    type: result.type,
  };
}
