import { describe, expect, it } from "vitest";
import { type ColumnBinding, logical, physical } from "../../plan/index.js";
import { simplifyExpression } from "../simplify.js";
import {
  aggregate,
  aggregateCall,
  and,
  cast,
  column,
  compare,
  constant,
  FALSE,
  filter,
  get,
  join,
  lines,
  not,
  NULL,
  nullCheck,
  optimizedPlan,
  or,
  orders,
  output,
  parameter,
  planOf,
  plus,
  project,
  random,
  times,
  TRUE,
  users,
} from "./builders.js";
import { expectSameRows } from "./same-rows.js";

const u = { tableIndex: 1, table: users };
const o = { tableIndex: 2, table: orders, alias: "o" };
const age = column(u, "age");
const isAdult = compare(age, ">=", constant(18));

describe("folding constants", () => {
  it("evaluates constant subexpressions with the expression compiler", () => {
    const condition = compare(age, ">", plus(constant(1), constant(2)));
    expect(simplifyExpression(condition)).toEqual(
      compare(age, ">", constant(3, "INTEGER")),
    );
  });

  it("keeps the type the expression had, so conversions around it stay", () => {
    expect(simplifyExpression(cast(constant("5"), "INTEGER"))).toEqual(
      constant(5, "INTEGER"),
    );
  });

  it("leaves an expression that fails for run time", () => {
    const failing = cast(constant("x"), "INTEGER");
    expect(simplifyExpression(failing)).toEqual(failing);
  });

  it("never folds volatile calls, parameters or columns", () => {
    for (const expression of [
      plus(random(), constant(1)),
      plus(parameter(0), constant(1)),
      plus(age, constant(1)),
    ]) {
      expect(simplifyExpression(expression)).toEqual(expression);
    }
  });
});

describe("three-valued identities", () => {
  it("drops TRUE from AND and FALSE from OR", () => {
    expect(simplifyExpression(and(TRUE, isAdult))).toEqual(isAdult);
    expect(simplifyExpression(or(FALSE, isAdult))).toEqual(isAdult);
  });

  it("decides AND by FALSE and OR by TRUE, wherever they stand", () => {
    expect(simplifyExpression(and(isAdult, FALSE))).toEqual(FALSE);
    expect(simplifyExpression(or(isAdult, TRUE))).toEqual(TRUE);
  });

  it("removes a double negation of a condition", () => {
    expect(simplifyExpression(not(not(isAdult)))).toEqual(isAdult);
  });

  it("keeps the operator when what is left is not a condition", () => {
    // AND and NOT check their operand is a truth; a bare parameter is not.
    const flag = parameter(0, "BOOLEAN");
    expect(simplifyExpression(and(TRUE, flag))).toEqual(and(TRUE, flag));
    expect(simplifyExpression(not(not(flag)))).toEqual(not(not(flag)));
  });

  it("leaves NULL AND x, which is FALSE or NULL depending on x", () => {
    const condition = and(NULL, isAdult);
    expect(simplifyExpression(condition)).toEqual(condition);
  });

  it("never rewrites x = x or x * 0, which are NULL for a NULL x", () => {
    const same = compare(age, "=", age);
    const zero = compare(times(age, constant(0)), "=", constant(0));
    expect(simplifyExpression(same)).toEqual(same);
    expect(simplifyExpression(zero)).toEqual(zero);
  });
});

describe("conditions no row passes", () => {
  it("reads nothing for a WHERE that folds to FALSE", () => {
    const plan = filter(
      get(u),
      isAdult,
      compare(constant(1), "=", constant(2)),
    );
    expect(planOf(plan)).toBe("rowid-lookup users#1 (rowid IN ())");
  });

  it("reads nothing for a WHERE that folds to NULL", () => {
    const plan = filter(
      get(u),
      compare(age, ">", constant(1)),
      and(NULL, TRUE),
    );
    expect(planOf(plan)).toBe("rowid-lookup users#1 (rowid IN ())");
  });

  it("drops a WHERE that folds to TRUE", () => {
    const plan = filter(get(u), or(TRUE, isAdult));
    expect(planOf(plan)).toBe("table-scan users#1");
  });

  it("reads neither side of an inner join whose ON folds to FALSE", () => {
    const plan = join(
      "inner",
      get(u),
      get(o),
      compare(constant(1), "=", constant(0)),
    );
    expect(planOf(plan)).toBe(
      lines(
        "nested-loop-join inner",
        "  rowid-lookup users#1 (rowid IN ())",
        "  rowid-lookup orders#2 (rowid IN ())",
      ),
    );
  });

  it("keeps every left row of a LEFT join whose ON folds to FALSE", () => {
    const plan = join("left", get(u), get(o), FALSE);
    expect(planOf(plan)).toBe(
      lines(
        "nested-loop-join left on FALSE",
        "  table-scan users#1",
        "  rowid-lookup orders#2 (rowid IN ())",
      ),
    );
  });

  it("keeps the layout of what it stands for: HAVING FALSE without groups", () => {
    const counted = aggregate(
      { groupIndex: 3, aggregateIndex: 4 },
      get(u),
      [],
      aggregateCall("count"),
    );
    expect(planOf(filter(counted, FALSE))).toBe("values#4 no rows");
  });

  it("is not fooled by a column compared with FALSE", () => {
    const paid = column(o, "paid");
    const plan = filter(
      get(o),
      compare(paid, "=", FALSE),
      nullCheck(column(o, "total")),
    );
    expect(planOf(plan)).toBe(
      "table-scan orders#2 residual: o.paid = FALSE AND o.total IS NULL",
    );
    expectSameRows(plan);
  });
});

/** The physical layout is the logical one, so the nodes above find their columns. */
/** Bindings in one order, whatever order a plan lays them out in. */
function byBinding(a: ColumnBinding, b: ColumnBinding): number {
  const tableOrder = a.tableIndex - b.tableIndex;
  return tableOrder === 0 ? a.columnIndex - b.columnIndex : tableOrder;
}

/** The same columns, found by binding: a join's sides may swap (docs/planning.md). */
function expectLayoutKept(plan: logical.Plan): void {
  const found = physical.layoutOf(optimizedPlan(plan));
  expect(found.toSorted(byBinding)).toEqual(
    logical.layoutOf(plan).toSorted(byBinding),
  );
  expectSameRows(plan);
}

describe("plans that read nothing keep their layout", () => {
  const p = { tableIndex: 3, table: users, alias: "p" };
  const userOf = compare(column(o, "user_id"), "=", column(u, "id"));

  it("under a join, a join that reads nothing", () => {
    const nothing = filter(join("inner", get(u), get(o), userOf), FALSE);
    const plan = join(
      "left",
      nothing,
      get(p),
      compare(column(p, "id"), "=", column(o, "user_id")),
    );
    expect(planOf(plan)).toBe(
      lines(
        "lookup-join left",
        "  nested-loop-join inner",
        "    rowid-lookup users#1 (rowid IN ())",
        "    rowid-lookup orders#2 (rowid IN ())",
        "  rowid-lookup users#3 (rowid = o.user_id)",
      ),
    );
    expectLayoutKept(plan);
  });

  it("under a join, an aggregate of groups and aggregates that reads nothing", () => {
    const counted = aggregate(
      { groupIndex: 4, aggregateIndex: 5 },
      get(o),
      [column(o, "user_id")],
      aggregateCall("count"),
    );
    const group = output(4, 0, "INTEGER", "user_id");
    const plan = join(
      "inner",
      get(u),
      filter(counted, NULL),
      compare(group, "=", column(u, "id")),
    );
    // Reading nothing, the aggregate leads; users is looked up by rowid.
    expect(planOf(plan)).toBe(
      lines(
        "lookup-join inner",
        "  nested-loop-join inner",
        "    values#4 no rows",
        "    values#5 no rows",
        "  rowid-lookup users#1 (rowid = user_id)",
      ),
    );
    expectLayoutKept(plan);
  });

  it("under a join, a project that reads nothing", () => {
    const projected = filter(project(6, get(o), column(o, "user_id")), FALSE);
    const plan = join(
      "left",
      get(u),
      projected,
      compare(output(6, 0, "INTEGER", "x"), "=", column(u, "id")),
    );
    expect(planOf(plan)).toBe(
      lines(
        "hash-join left on id = x",
        "  table-scan users#1",
        "  values#6 no rows",
      ),
    );
    expectLayoutKept(plan);
  });
});
