import { describe, expect, it } from "vitest";
import type { Tuple } from "../../../expressions/index.js";
import { rowKey, type Value } from "../../../values/index.js";
import { formatPhysicalPlan, type logical } from "../../plan/index.js";
import {
  aggregate,
  aggregateCall,
  asc,
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
import { bothRuns, bothSequences, runPlan } from "./reference.js";

// Random plans, each optimized and lowered naively (naive.ts), run by the
// reference interpreter over random tables with an ANY column: the same
// rows, whatever the optimizer chose. Seeded, so a failure repeats; the
// message carries the plan the optimizer made.

const SEED = 20_261_026;
const CASES = 60;

type Shape = (random: Random) => logical.Plan;

/** For CASES plans of `shape`, the optimized and the naive one give the same rows. */
function expectSameRowsFor(shape: Shape): void {
  const random = new Random(SEED);
  for (let count = 0; count < CASES; count += 1) {
    const plan = shape(random);
    const optimized = optimizedPlan(plan);
    const data = {
      tables: sampleTables(random),
      parameters: parametersFor(random),
    };
    const { expected, actual } = bothRuns(plan, optimized, data, bindKey);
    expect(actual, formatPhysicalPlan(optimized)).toEqual(expected);
  }
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
      const byEmail = random.chance(0.5)
        ? asc(output(5, 2, "TEXT", "email"))
        : desc(output(5, 2, "TEXT", "email"));
      const offset = constant(random.below(10));
      const plan = limit(
        order(shown, byEmail),
        constant(random.below(6)),
        offset,
      );
      const optimized = optimizedPlan(plan);
      const data = {
        tables: sampleTables(random),
        parameters: parametersFor(random),
      };
      const { expected, actual } = bothSequences(
        plan,
        optimized,
        data,
        bindKey,
      );
      expect(actual, formatPhysicalPlan(optimized)).toEqual(expected);
    }
  });

  it("for ORDER BY, with and without LIMIT", () => {
    const random = new Random(SEED);
    for (let count = 0; count < CASES * 2; count += 1) {
      const keys = Array.from({ length: 1 + random.below(2) }, () =>
        random.pick(KEYS),
      );
      const orderKeys = keys.map((key) => {
        const nulls = random.pick(["first", "last"] as const);
        return random.chance(0.5)
          ? asc(column(u, key), nulls)
          : desc(column(u, key), nulls);
      });
      const read = random.chance(0.3)
        ? usersRead()
        : filter(usersRead(), condition(random, userSubjects()));
      const ordered = order(read, ...orderKeys);
      const plan = random.chance(0.6)
        ? limit(ordered, constant(random.pick([0, 1, 3, 10])))
        : ordered;
      const optimized = optimizedPlan(plan);
      const tables = sampleTables(random);
      const parameters = parametersFor(random);
      const expected = runPlan(naivePlan(plan), tables, parameters, bindKey);
      const actual = runPlan(optimized, tables, parameters, bindKey);
      expect(keysOf(actual, keys), formatPhysicalPlan(optimized)).toEqual(
        keysOf(expected, keys),
      );
    }
  });
});
