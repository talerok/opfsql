import { describe, expect, it } from "vitest";
import {
  aggregate,
  aggregateCall,
  column,
  compare,
  constant,
  events,
  filter,
  get,
  lines,
  orders,
  parameter,
  planOf,
  users,
} from "./builders.js";
import { expectSameRows } from "./same-rows.js";
import type { logical } from "../../plan/index.js";

const u = { tableIndex: 1, table: users };
const indexes = { groupIndex: 3, aggregateIndex: 4 };
const age = column(u, "age");
const name = column(u, "name");

/** `SELECT call(argument) FROM child`. */
function single(
  call: string,
  child: logical.Plan,
  argument: logical.Expression = age,
): logical.Aggregate {
  return aggregate(indexes, child, [], aggregateCall(call, argument));
}

describe("MIN and MAX from an index's end", () => {
  it("reads MAX of an index's first part", () => {
    expect(planOf(single("max", get(u)))).toBe(
      "index-extreme#4 MAX(age) of users using users_age",
    );
  });

  it("reads MIN of the part after an exact equality prefix", () => {
    const plan = single(
      "min",
      filter(get(u), compare(name, "=", constant("bob"))),
    );
    expect(planOf(plan)).toBe(
      "index-extreme#4 MIN(age) of users using users_name_age (name = 'bob')",
    );
  });

  it("reads MIN after an equality with a parameter, which a seek enforces as = does", () => {
    const plan = single(
      "min",
      filter(get(u), compare(name, "=", parameter(0))),
    );
    expect(planOf(plan)).toBe(
      "index-extreme#4 MIN(age) of users using users_name_age (name = $1)",
    );
    for (const value of ["bob", "5", 5, null]) expectSameRows(plan, [value]);
  });

  it("is not chosen with any other condition", () => {
    const plan = single(
      "max",
      filter(
        get(u),
        compare(name, "=", constant("bob")),
        compare(column(u, "tag"), "=", parameter(0)),
      ),
    );
    expect(planOf(plan)).toMatch(/^hash-aggregate#3#4 compute max\(age\)\n/u);
  });

  it("is not chosen with GROUP BY, another aggregate, or no index", () => {
    const grouped = aggregate(
      indexes,
      get(u),
      [name],
      aggregateCall("max", age),
    );
    const two = aggregate(
      indexes,
      get(u),
      [],
      aggregateCall("max", age),
      aggregateCall("count"),
    );
    const o = { tableIndex: 2, table: orders };
    const unindexed = aggregate(
      indexes,
      get(o),
      [],
      aggregateCall("max", column(o, "total")),
    );
    for (const plan of [grouped, two, unindexed]) {
      expect(planOf(plan)).toMatch(/^hash-aggregate/u);
    }
  });

  it("is not chosen for a JSON or ANY part, whose decoded key may lose an object's key order", () => {
    const log = { tableIndex: 5, table: events };
    const json = aggregate(
      indexes,
      get(log),
      [],
      aggregateCall("max", column(log, "data")),
    );
    expect(planOf(json)).toBe(
      lines("hash-aggregate#3#4 compute max(data)", "  table-scan events#5"),
    );
    const any = single("max", get(u), column(u, "tag"));
    expect(planOf(any)).toBe(
      lines("hash-aggregate#3#4 compute max(tag)", "  table-scan users#1"),
    );
  });

  it("is not chosen for COUNT", () => {
    expect(planOf(single("count", get(u)))).toMatch(/^hash-aggregate/u);
  });
});
