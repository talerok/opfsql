import { describe, expect, it } from "vitest";
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
  or,
  orders,
  parameter,
  planOf,
  plus,
  random,
  times,
  TRUE,
  users,
} from "./builders.js";

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
    const flag = { ...column(u, "name"), type: "BOOLEAN" as const };
    const plan = filter(get(u), compare(flag, "=", FALSE), nullCheck(age));
    expect(planOf(plan)).toBe(
      "table-scan users#1 residual: name = FALSE AND age IS NULL",
    );
  });
});
