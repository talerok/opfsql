import { describe, expect, it } from "vitest";
import type { logical } from "../../sql/plan/index.js";
import { evaluateConstant } from "../index.js";
import {
  and,
  arithmetic,
  blob,
  call,
  caseWhen,
  cast,
  column,
  compare,
  constant,
  jsonAccess,
  NULL,
  parameter,
  scalarSubquery,
  TRUE,
  values,
} from "./builders.js";
import { c0 } from "./harness.js";

describe("evaluateConstant", () => {
  it("folds with the semantics of the compiled query", () => {
    expect(evaluateConstant(and(NULL, TRUE))).toBeNull();
    expect(evaluateConstant(compare(blob(1), "=", blob(1)))).toBe(true);
    expect(evaluateConstant(arithmetic(constant(7), "/", constant(2)))).toBe(3);
    expect(evaluateConstant(call("upper", constant("a")))).toBe("A");
  });

  it("leaves an expression that fails to run time", () => {
    expect(evaluateConstant(cast(constant("x"), "INTEGER"))).toBeUndefined();
  });

  it("never folds a volatile function, however deep", () => {
    expect(evaluateConstant(call("random"))).toBeUndefined();
    expect(evaluateConstant(call("abs", call("random")))).toBeUndefined();
  });

  it("refuses columns, parameters and subqueries, however deep", () => {
    expect(evaluateConstant(c0)).toBeUndefined();
    expect(
      evaluateConstant(arithmetic(parameter(0), "+", constant(1))),
    ).toBeUndefined();
    expect(
      evaluateConstant(jsonAccess(column(0, 0, "JSON"), "a")),
    ).toBeUndefined();
    expect(
      evaluateConstant(scalarSubquery(values(1, [constant(1)]))),
    ).toBeUndefined();
    expect(
      evaluateConstant(caseWhen([[TRUE, constant(1)]], c0)),
    ).toBeUndefined();
  });

  it("folds expressions of logical plans too", () => {
    const sum: logical.Expression = {
      kind: "arithmetic",
      operator: "ADD",
      left: constant(1),
      right: constant(2),
      type: "INTEGER",
    };
    expect(evaluateConstant(sum)).toBe(3);
  });
});
