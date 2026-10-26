import type { Affinity, Value } from "../../../values/index.js";
import type { logical, StaticType } from "../../plan/index.js";
import {
  and,
  between,
  call,
  column,
  compare,
  constant,
  get,
  inList,
  not,
  nullCheck,
  or,
  orders,
  parameter,
  users,
} from "./builders.js";
import type { Random } from "./random.js";

// Random conditions over users and orders (builders.ts), for the
// differential test: comparisons, BETWEEN, IN, IS NULL, AND, OR and NOT of
// the columns, the ANY column and the expression key among them, against
// constants of every kind, NULL, and parameters converted as the binder
// would have them converted.

type Expression = logical.Expression;

export const u = { tableIndex: 1, table: users, alias: "u" };
export const o = { tableIndex: 2, table: orders, alias: "o" };

/** users with every column, then the rowid, as a DML source reads it too. */
export function usersRead(): logical.Get {
  return get(u, "id", "name", "age", "email", "tag", "rowid");
}

export function userSubjects(): Expression[] {
  const names = ["id", "rowid", "name", "age", "email", "tag"];
  const columns = names.map((name) => column(u, name));
  return [...columns, call("lower", column(u, "name"))];
}

export function orderSubjects(): Expression[] {
  const names = ["id", "user_id", "total", "status", "paid"];
  return names.map((name) => column(o, name));
}

const CONSTANTS: readonly Value[] = [
  0,
  1,
  5,
  18,
  30,
  -3,
  100,
  1.5,
  2.5,
  1e300,
  "bob",
  "b",
  "5",
  "",
  "z",
  "e1x1",
  "Bob",
  "abc",
  true,
  false,
  null,
];

/** Values parameters $1 to $3 are given. */
export const PARAMETER_VALUES: readonly Value[] = [
  "5",
  5,
  null,
  "bob",
  18,
  "x",
  2.5,
  "18",
  1,
  "e1x1",
  true,
  "abc",
];

const OPERATORS = ["=", "<>", "<", "<=", ">", ">="] as const;

/** A condition on `subjects`, nested at most two levels. */
export function condition(
  random: Random,
  subjects: readonly Expression[],
  depth = 0,
): Expression {
  const subject = random.pick(subjects);
  const roll = random.next();
  if (roll < 0.45) return comparisonOf(random, subject, subjects);
  if (roll < 0.55) {
    return between(
      subject,
      seekValue(random, subject),
      seekValue(random, subject),
    );
  }
  if (roll < 0.65) {
    const items = Array.from({ length: 1 + random.below(3) }, () =>
      seekValue(random, subject),
    );
    return inList(subject, ...items);
  }
  if (roll < 0.72) return nullCheck(subject);
  if (depth >= 2) return comparisonOf(random, subject, subjects);
  return compound(random, subjects, depth + 1);
}

/** Up to `most` conditions. */
export function conditions(
  random: Random,
  subjects: readonly Expression[],
  most = 3,
): Expression[] {
  return Array.from({ length: random.below(most + 1) }, () =>
    condition(random, subjects),
  );
}

function compound(
  random: Random,
  subjects: readonly Expression[],
  depth: number,
): Expression {
  const next = (): Expression => condition(random, subjects, depth);
  const roll = random.next();
  if (roll < 0.4) return or(next(), next());
  if (roll < 0.7) return and(next(), next());
  return not(next());
}

/** `subject op value`, either way round; now and then a column on both sides. */
function comparisonOf(
  random: Random,
  subject: Expression,
  subjects: readonly Expression[],
): Expression {
  const operator = random.pick(OPERATORS);
  const isColumns = random.chance(0.1);
  const value = isColumns ? random.pick(subjects) : seekValue(random, subject);
  if (random.chance(0.5)) return compare(subject, operator, value);
  return compare(value, operator, subject);
}

/** A constant of any kind, or a parameter with the affinity `subject` gives it. */
function seekValue(random: Random, subject: Expression): Expression {
  if (random.chance(0.7)) return constant(random.pick(CONSTANTS));
  return parameter(random.below(3), subject.type, affinityOf(subject.type));
}

function affinityOf(type: StaticType): Affinity | null {
  if (type === "INTEGER" || type === "REAL") return "numeric";
  return type === "TEXT" ? "text" : null;
}

/** Three parameter values. */
export function parametersFor(random: Random): Value[] {
  return [0, 1, 2].map(() => random.pick(PARAMETER_VALUES));
}
