import { beforeAll, describe, expect, it } from "vitest";
import type { Tuple } from "../../../expressions/index.js";
import { rowKey, type Value } from "../../../values/index.js";
import {
  formatPhysicalPlan,
  type logical,
  type physical,
} from "../../plan/index.js";
import {
  aggregate,
  aggregateCall,
  asc,
  between,
  bindKey,
  call,
  column,
  compare,
  desc,
  distinct,
  exists,
  filter,
  get,
  join,
  limit,
  optimizedPlan,
  constant,
  order,
  output,
  plus,
  project,
  scalarSubquery,
  setOperation,
} from "./builders.js";
import { naivePlan } from "./naive.js";
import { Random, sampleTables } from "./random.js";
import {
  condition,
  conditions,
  o,
  orderSubjects,
  parametersFor,
  u,
  userSubjects,
  usersRead,
} from "./random-plans.js";
import { SampleDatabase } from "./executed.js";
import { inLayoutOf, multiset, runPlan } from "./reference.js";

// Random plans over sample tables with an ANY column, each run three ways:
// lowered naively (naive.ts) and interpreted, optimized and interpreted
// (reference.ts), and optimized and run by the real executor over the same
// rows in a database (executed.ts). All three give the same rows, whatever
// the optimizer chose. Seeded, so a failure repeats; the message carries the
// plan the optimizer made.

const SEED = 20_261_026;
const CASES = 60;

const SAMPLES = [1, 2, 3].map((seed) => sampleTables(new Random(seed)));
let databases: readonly SampleDatabase[] = [];

beforeAll(async () => {
  databases = await Promise.all(
    SAMPLES.map(async (rows) => SampleDatabase.open(rows)),
  );
});

type Shape = (random: Random) => logical.Plan;

/** The rows of a plan run the three ways, over one sample. */
interface Runs {
  readonly naive: readonly Tuple[];
  readonly interpreted: readonly Tuple[];
  readonly executed: readonly Tuple[];
}

function runThreeWays(
  plan: logical.Plan,
  optimized: physical.Plan,
  random: Random,
): Runs {
  const sample = random.below(SAMPLES.length);
  const tables = SAMPLES[sample]!;
  const parameters = parametersFor(random);
  const interpreted = runPlan(optimized, tables, parameters, bindKey);
  const executed = databases[sample]!.query(optimized, parameters);
  return {
    naive: runPlan(naivePlan(plan), tables, parameters, bindKey),
    interpreted: inLayoutOf(plan, optimized, interpreted),
    executed: inLayoutOf(plan, optimized, executed),
  };
}

/** For CASES plans of `shape`, the three runs give the same rows; the plans, printed. */
function expectSameRowsFor(shape: Shape): string[] {
  const random = new Random(SEED);
  const plans: string[] = [];
  for (let count = 0; count < CASES; count += 1) {
    const plan = shape(random);
    const optimized = optimizedPlan(plan);
    const runs = runThreeWays(plan, optimized, random);
    const printed = formatPhysicalPlan(optimized);
    expect(multiset(runs.interpreted), printed).toEqual(multiset(runs.naive));
    expect(multiset(runs.executed), printed).toEqual(multiset(runs.naive));
    plans.push(printed);
  }
  return plans;
}

/** How many of the printed plans hold a node of `kind`. */
function countWith(plans: readonly string[], kind: string): number {
  return plans.filter((plan) => plan.includes(kind)).length;
}

/** Rows as a list of keys, in order. */
function sequence(rows: readonly Tuple[]): string[] {
  return rows.map((row) => rowKey(row));
}

describe("optimized plans give the naive plans' rows", () => {
  it("for filters on one table", () => {
    expectSameRowsFor((random) =>
      filter(
        usersRead(),
        condition(random, userSubjects()),
        ...conditions(random, userSubjects()),
      ),
    );
  });

  it("for MIN and MAX over filters", () => {
    const operands = [
      column(u, "age"),
      column(u, "name"),
      column(u, "email"),
      column(u, "tag"),
      call("lower", column(u, "name")),
    ];
    expectSameRowsFor((random) => {
      const read = random.chance(0.3)
        ? usersRead()
        : filter(
            usersRead(),
            ...conditions(random, userSubjects(), 2),
            condition(random, userSubjects()),
          );
      const extreme = aggregateCall(
        random.pick(["min", "max"]),
        random.pick(operands),
      );
      return aggregate({ groupIndex: 3, aggregateIndex: 4 }, read, [], extreme);
    });
  });

  it("for joins, with conditions in ON and WHERE", () => {
    expectSameRowsFor((random) => {
      const key = random.pick([
        column(u, "id"),
        column(u, "age"),
        column(u, "tag"),
      ]);
      const on = [
        compare(column(o, "user_id"), "=", key),
        ...conditions(random, bothSubjects(), 2),
      ];
      const joined = join(
        random.pick(["inner", "left"] as const),
        usersRead(),
        get(o),
        ...on,
      );
      const where = conditions(random, bothSubjects(), 2);
      return where.length === 0 ? joined : filter(joined, ...where);
    });
  });

  it("for joins that find one side by the other's key, either side leading", () => {
    const plans = expectSameRowsFor((random) => {
      const isUsersFirst = random.chance(0.5);
      const left = isUsersFirst ? usersRead() : get(o);
      const right = isUsersFirst ? get(o) : usersRead();
      const on = [
        keyCondition(random),
        ...conditions(random, bothSubjects(), 1),
      ];
      const joined = join(
        random.pick(["inner", "left"] as const),
        left,
        right,
        ...on,
      );
      const where = conditions(random, selectiveSubjects(), 2);
      return where.length === 0 ? joined : filter(joined, ...where);
    });
    // Lookups, and hash joins, come up often enough to be tested.
    expect(countWith(plans, "lookup-join")).toBeGreaterThan(CASES / 4);
    expect(countWith(plans, "hash-join")).toBeGreaterThan(CASES / 10);
  });

  it("for chains of three joins, reordered", () => {
    const p = { tableIndex: 3, table: u.table, alias: "p" };
    const plans = expectSameRowsFor((random) => {
      const first = join(
        random.pick(["inner", "left"] as const),
        get(o),
        usersRead(),
        keyCondition(random),
      );
      const other = random.pick(["id", "age", "tag", "name"]);
      const joined = join(
        "inner",
        first,
        get(p),
        compare(
          column(p, other),
          random.pick(KEY_OPERATORS),
          column(o, "user_id"),
        ),
        ...conditions(random, bothSubjects(), 1),
      );
      const where = conditions(random, selectiveSubjects(), 2);
      return where.length === 0 ? joined : filter(joined, ...where);
    });
    expect(countWith(plans, "lookup-join")).toBeGreaterThan(CASES / 4);
  });

  it("for filters over projects, aggregates, set operations and DISTINCT", () => {
    const shapes: Shape[] = [
      overProject,
      overAggregate,
      overSetOperation,
      overDistinct,
    ];
    for (const shape of shapes) expectSameRowsFor(shape);
  });

  it("for LIMIT and OFFSET over a select list with a correlated subquery", () => {
    expectSameRowsFor((random) => {
      const counted = project(5, usersRead(), column(u, "name"), orderCount());
      const offset = random.chance(0.5) ? constant(random.below(40)) : null;
      return limit(counted, constant(random.below(5)), offset);
    });
  });

  it("for correlated subqueries that seek on their outer row", () => {
    const inner = { tableIndex: 9, table: o.table, alias: "i" };
    expectSameRowsFor((random) => {
      const outer = column(u, random.pick(["id", "age", "tag"]), 1);
      const matching = compare(column(inner, "user_id"), "=", outer);
      const subjects = ["id", "user_id", "total", "status"].map((name) =>
        column(inner, name),
      );
      const subquery = filter(
        get(inner),
        matching,
        ...conditions(random, subjects, 1),
      );
      return filter(
        usersRead(),
        exists(subquery, true),
        ...conditions(random, userSubjects(), 1),
      );
    });
  });
});

/** Columns of both users and orders. */
function bothSubjects(): logical.Expression[] {
  return [...userSubjects(), ...orderSubjects()];
}

/** Columns a condition of its own may pick a few rows by: rowids and indexed ones. */
function selectiveSubjects(): logical.Expression[] {
  return [
    column(u, "id"),
    column(u, "email"),
    column(u, "age"),
    column(o, "id"),
    column(o, "user_id"),
  ];
}

const KEY_OPERATORS = ["=", "=", "=", "<", "<=", ">", ">="] as const;

/**
 * A condition between an order and a user that a seek can use on either
 * side: orders' rowid or orders_user, or users' rowid, email, age, name,
 * tag or lower(name), against the other side's column, mostly `=`.
 */
function keyCondition(random: Random): logical.Expression {
  const orderSide = random.pick([column(o, "user_id"), column(o, "id")]);
  const userSide = random.pick([
    column(u, "id"),
    column(u, "age"),
    column(u, "tag"),
    column(u, "name"),
    column(u, "email"),
    call("lower", column(u, "name")),
  ]);
  const operator = random.pick(KEY_OPERATORS);
  if (random.chance(0.15)) {
    return between(userSide, orderSide, column(o, "total"));
  }
  if (random.chance(0.5)) return compare(orderSide, operator, userSide);
  return compare(userSide, operator, orderSide);
}

/** `(SELECT count(*) FROM orders i WHERE i.user_id = u.id)`, correlated. */
function orderCount(): logical.Expression {
  const inner = { tableIndex: 9, table: o.table, alias: "i" };
  const matching = compare(column(inner, "user_id"), "=", column(u, "id", 1));
  const counted = aggregate(
    { groupIndex: 10, aggregateIndex: 11 },
    filter(get(inner), matching),
    [],
    aggregateCall("count"),
  );
  return scalarSubquery(counted, true);
}

function overProject(random: Random): logical.Plan {
  const expressions = [
    column(u, "name"),
    column(u, "age"),
    plus(column(u, "age"), constant(1)),
    column(u, "tag"),
    call("lower", column(u, "name")),
  ];
  const projected = project(5, usersRead(), ...expressions);
  const columns = expressions.map((expression, position) =>
    output(5, position, expression.type),
  );
  return filter(
    projected,
    condition(random, columns),
    ...conditions(random, columns),
  );
}

function overAggregate(random: Random): logical.Plan {
  const groups = [column(u, "tag"), column(u, "age")];
  const calls = [
    aggregateCall("count"),
    aggregateCall("max", column(u, "name")),
  ];
  const grouped = aggregate(
    { groupIndex: 3, aggregateIndex: 4 },
    usersRead(),
    groups,
    ...calls,
  );
  const columns = [
    output(3, 0, "ANY"),
    output(3, 1, "INTEGER"),
    output(4, 0, "INTEGER"),
    output(4, 1, "TEXT"),
  ];
  return filter(
    grouped,
    condition(random, columns),
    ...conditions(random, columns),
  );
}

function overSetOperation(random: Random): logical.Plan {
  const left = project(6, usersRead(), column(u, "tag"), column(u, "age"));
  const right = project(7, get(o), column(o, "status"), column(o, "user_id"));
  const operator = random.pick([
    "UNION",
    "UNION ALL",
    "EXCEPT",
    "INTERSECT",
  ] as const);
  const combined = setOperation(operator, 8, left, right);
  const columns = [output(8, 0, "ANY"), output(8, 1, "INTEGER")];
  return filter(
    combined,
    condition(random, columns),
    ...conditions(random, columns),
  );
}

function overDistinct(random: Random): logical.Plan {
  const projected = project(6, usersRead(), column(u, "tag"), column(u, "age"));
  const columns = [output(6, 0, "ANY"), output(6, 1, "INTEGER")];
  return filter(distinct(projected), condition(random, columns));
}

/** Where each ORDER BY column sits in a row of usersRead(). */
const POSITIONS = { id: 0, name: 1, age: 2, email: 3, tag: 4 } as const;
type Key = keyof typeof POSITIONS;
const KEYS: readonly Key[] = ["id", "name", "age", "email", "tag"];

/** Each row's ORDER BY values: rows tied on them may come in any order. */
function keysOf(rows: readonly Tuple[], keys: readonly Key[]): string[] {
  return rows.map((row) =>
    rowKey(keys.map((key): Value => row[POSITIONS[key]] ?? null)),
  );
}

/** An ORDER BY of `key`, in a random direction and NULL placement. */
function orderKeyOf(random: Random, key: Key): logical.OrderKey {
  const nulls = random.pick(["first", "last"] as const);
  const subject = column(u, key);
  return random.chance(0.5) ? asc(subject, nulls) : desc(subject, nulls);
}

describe("optimized orders give the naive orders' keys", () => {
  it("for ORDER BY a unique key over a select list, under LIMIT and OFFSET", () => {
    const random = new Random(SEED);
    for (let count = 0; count < CASES; count += 1) {
      const read = filter(usersRead(), condition(random, userSubjects()));
      const shown = project(
        5,
        read,
        column(u, "name"),
        orderCount(),
        column(u, "email"),
      );
      const email = output(5, 2, "TEXT", "email");
      const byEmail = random.chance(0.5) ? asc(email) : desc(email);
      const offset = constant(random.below(10));
      const plan = limit(
        order(shown, byEmail),
        constant(random.below(6)),
        offset,
      );
      const optimized = optimizedPlan(plan);
      const runs = runThreeWays(plan, optimized, random);
      const printed = formatPhysicalPlan(optimized);
      const expected = sequence(runs.naive);
      expect(sequence(runs.interpreted), printed).toEqual(expected);
      expect(sequence(runs.executed), printed).toEqual(expected);
    }
  });

  it("for ORDER BY, with and without LIMIT", () => {
    const random = new Random(SEED);
    for (let count = 0; count < CASES * 2; count += 1) {
      const keys = Array.from({ length: 1 + random.below(2) }, () =>
        random.pick(KEYS),
      );
      const orderKeys = keys.map((key) => orderKeyOf(random, key));
      const read = usersRead();
      const filtered = random.chance(0.3)
        ? read
        : filter(read, condition(random, userSubjects()));
      const ordered = order(filtered, ...orderKeys);
      const rows = constant(random.pick([0, 1, 3, 10]));
      const plan = random.chance(0.6) ? limit(ordered, rows) : ordered;
      const optimized = optimizedPlan(plan);
      const runs = runThreeWays(plan, optimized, random);
      const printed = formatPhysicalPlan(optimized);
      const expected = keysOf(runs.naive, keys);
      expect(keysOf(runs.interpreted, keys), printed).toEqual(expected);
      expect(keysOf(runs.executed, keys), printed).toEqual(expected);
    }
  });
});
