import { describe, expect, it } from "vitest";
import type { BoundExpression } from "../../binder/types.js";
import { BoundExpressionClass } from "../../binder/types.js";
import { flipComparison } from "../utils/flip_comparison.js";
import { negateComparison } from "../utils/negate_comparison.js";
import { expressionReferencesTable } from "../utils/expression_references_table.js";
import { collectColumnRefs } from "../utils/collect_column_refs.js";
import { makeColRef, makeIntConstant } from "./test_helpers.js";

describe("flipComparison", () => {
  it("flips LESS to GREATER", () => expect(flipComparison("LESS")).toBe("GREATER"));
  it("flips GREATER to LESS", () => expect(flipComparison("GREATER")).toBe("LESS"));
  it("flips LESS_EQUAL to GREATER_EQUAL", () => expect(flipComparison("LESS_EQUAL")).toBe("GREATER_EQUAL"));
  it("flips GREATER_EQUAL to LESS_EQUAL", () => expect(flipComparison("GREATER_EQUAL")).toBe("LESS_EQUAL"));
  it("EQUAL stays EQUAL", () => expect(flipComparison("EQUAL")).toBe("EQUAL"));
  it("NOT_EQUAL stays NOT_EQUAL", () => expect(flipComparison("NOT_EQUAL")).toBe("NOT_EQUAL"));
});

describe("negateComparison", () => {
  it("negates LESS to GREATER_EQUAL", () => expect(negateComparison("LESS")).toBe("GREATER_EQUAL"));
  it("negates GREATER to LESS_EQUAL", () => expect(negateComparison("GREATER")).toBe("LESS_EQUAL"));
  it("negates LESS_EQUAL to GREATER", () => expect(negateComparison("LESS_EQUAL")).toBe("GREATER"));
  it("negates GREATER_EQUAL to LESS", () => expect(negateComparison("GREATER_EQUAL")).toBe("LESS"));
  it("negates EQUAL to NOT_EQUAL", () => expect(negateComparison("EQUAL")).toBe("NOT_EQUAL"));
  it("negates NOT_EQUAL to EQUAL", () => expect(negateComparison("NOT_EQUAL")).toBe("EQUAL"));
});

describe("expressionReferencesTable", () => {
  it("returns true when expression references the table", () => {
    const ref = makeColRef(2, 0);
    expect(expressionReferencesTable(ref, 2)).toBe(true);
  });

  it("returns false when expression does not reference the table", () => {
    const ref = makeColRef(1, 0);
    expect(expressionReferencesTable(ref, 2)).toBe(false);
  });

  it("checks nested expressions (comparison)", () => {
    const expr: BoundExpression = {
      expressionClass: BoundExpressionClass.BOUND_COMPARISON,
      comparisonType: "EQUAL",
      left: makeColRef(3, 0),
      right: makeIntConstant(5),
      returnType: "BOOLEAN",
    };
    expect(expressionReferencesTable(expr, 3)).toBe(true);
    expect(expressionReferencesTable(expr, 0)).toBe(false);
  });
});

describe("collectColumnRefs", () => {
  it("collects refs from simple column ref", () => {
    const refs = collectColumnRefs(makeColRef(1, 2));
    expect(refs).toEqual([{ tableIndex: 1, columnIndex: 2 }]);
  });

  it("collects refs from comparison", () => {
    const expr: BoundExpression = {
      expressionClass: BoundExpressionClass.BOUND_COMPARISON,
      comparisonType: "EQUAL",
      left: makeColRef(0, 1),
      right: makeColRef(1, 2),
      returnType: "BOOLEAN",
    };
    const refs = collectColumnRefs(expr);
    expect(refs).toHaveLength(2);
    expect(refs).toContainEqual({ tableIndex: 0, columnIndex: 1 });
    expect(refs).toContainEqual({ tableIndex: 1, columnIndex: 2 });
  });

  it("collects refs from constant (none)", () => {
    const refs = collectColumnRefs(makeIntConstant(42));
    expect(refs).toHaveLength(0);
  });
});
