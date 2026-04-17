import { beforeEach, describe, expect, it } from "vitest";
import type {
  BoundColumnRefExpression,
  BoundComparisonExpression,
  BoundConstantExpression,
  BoundExpression,
  BoundParameterExpression,
} from "../../binder/types.js";
import { BoundExpressionClass } from "../../binder/types.js";
import { FilterCombiner } from "../filter_combiner.js";
import { makeColRef, makeIntConstant, makeStrConstant } from "./test_helpers.js";

describe("FilterCombiner", () => {
  it("detects redundant range filters: x > 5 AND x > 7 → x > 7", () => {
    const combiner = new FilterCombiner();
    combiner.addFilter({
      expressionClass: BoundExpressionClass.BOUND_COMPARISON,
      comparisonType: "GREATER",
      left: makeColRef(0, 0),
      right: makeIntConstant(5),
      returnType: "BOOLEAN",
    });
    combiner.addFilter({
      expressionClass: BoundExpressionClass.BOUND_COMPARISON,
      comparisonType: "GREATER",
      left: makeColRef(0, 0),
      right: makeIntConstant(7),
      returnType: "BOOLEAN",
    });

    const filters = combiner.generateFilters();
    const comparisons = filters.filter(
      (f) => f.expressionClass === BoundExpressionClass.BOUND_COMPARISON,
    ) as BoundComparisonExpression[];
    expect(comparisons).toHaveLength(1);
    expect(comparisons[0].comparisonType).toBe("GREATER");
    expect((comparisons[0].right as BoundConstantExpression).value).toBe(7);
  });

  it("detects unsatisfiable equality: x = 5 AND x = 6 → false", () => {
    const combiner = new FilterCombiner();
    combiner.addFilter({
      expressionClass: BoundExpressionClass.BOUND_COMPARISON,
      comparisonType: "EQUAL",
      left: makeColRef(0, 0),
      right: makeIntConstant(5),
      returnType: "BOOLEAN",
    });
    combiner.addFilter({
      expressionClass: BoundExpressionClass.BOUND_COMPARISON,
      comparisonType: "EQUAL",
      left: makeColRef(0, 0),
      right: makeIntConstant(6),
      returnType: "BOOLEAN",
    });

    const filters = combiner.generateFilters();
    expect(filters).toHaveLength(1);
    expect(filters[0].expressionClass).toBe(
      BoundExpressionClass.BOUND_CONSTANT,
    );
    expect((filters[0] as BoundConstantExpression).value).toBe(false);
  });

  it("generates transitive filters: x = y AND x = 5 → y = 5", () => {
    const combiner = new FilterCombiner();
    combiner.addFilter({
      expressionClass: BoundExpressionClass.BOUND_COMPARISON,
      comparisonType: "EQUAL",
      left: makeColRef(0, 0),
      right: makeColRef(1, 0),
      returnType: "BOOLEAN",
    });
    combiner.addFilter({
      expressionClass: BoundExpressionClass.BOUND_COMPARISON,
      comparisonType: "EQUAL",
      left: makeColRef(0, 0),
      right: makeIntConstant(5),
      returnType: "BOOLEAN",
    });

    const filters = combiner.generateFilters();
    const eqFilters = filters.filter(
      (f) =>
        f.expressionClass === BoundExpressionClass.BOUND_COMPARISON &&
        (f as BoundComparisonExpression).comparisonType === "EQUAL",
    ) as BoundComparisonExpression[];

    const table1Filters = eqFilters.filter((f) => {
      if (f.left.expressionClass !== BoundExpressionClass.BOUND_COLUMN_REF)
        return false;
      return (f.left as BoundColumnRefExpression).binding.tableIndex === 1;
    });
    expect(table1Filters.length).toBeGreaterThanOrEqual(1);
  });

  it("generates table filters for scan pushdown", () => {
    const combiner = new FilterCombiner();
    combiner.addFilter({
      expressionClass: BoundExpressionClass.BOUND_COMPARISON,
      comparisonType: "GREATER",
      left: makeColRef(0, 2),
      right: makeIntConstant(18),
      returnType: "BOOLEAN",
    });

    const tableFilters = combiner.generateTableFilters(0);
    expect(tableFilters).toHaveLength(1);
    expect(tableFilters[0].columnIndex).toBe(2);
    expect(tableFilters[0].comparisonType).toBe("GREATER");
    expect((tableFilters[0].constant as BoundConstantExpression).value).toBe(
      18,
    );
  });

  it("detects unsatisfiable range: x > 10 AND x < 5 → false", () => {
    const combiner = new FilterCombiner();
    combiner.addFilter({
      expressionClass: BoundExpressionClass.BOUND_COMPARISON,
      comparisonType: "GREATER",
      left: makeColRef(0, 0),
      right: makeIntConstant(10),
      returnType: "BOOLEAN",
    });
    combiner.addFilter({
      expressionClass: BoundExpressionClass.BOUND_COMPARISON,
      comparisonType: "LESS",
      left: makeColRef(0, 0),
      right: makeIntConstant(5),
      returnType: "BOOLEAN",
    });

    const filters = combiner.generateFilters();
    expect(filters).toHaveLength(1);
    expect(filters[0].expressionClass).toBe(
      BoundExpressionClass.BOUND_CONSTANT,
    );
    expect((filters[0] as BoundConstantExpression).value).toBe(false);
  });

  it("keeps both NOT_EQUAL filters: x != 5 AND x != 6", () => {
    const combiner = new FilterCombiner();
    combiner.addFilter({
      expressionClass: BoundExpressionClass.BOUND_COMPARISON,
      comparisonType: "NOT_EQUAL",
      left: makeColRef(0, 0),
      right: makeIntConstant(5),
      returnType: "BOOLEAN",
    });
    combiner.addFilter({
      expressionClass: BoundExpressionClass.BOUND_COMPARISON,
      comparisonType: "NOT_EQUAL",
      left: makeColRef(0, 0),
      right: makeIntConstant(6),
      returnType: "BOOLEAN",
    });

    const filters = combiner.generateFilters();
    const neFilters = filters.filter(
      (f) =>
        f.expressionClass === BoundExpressionClass.BOUND_COMPARISON &&
        (f as BoundComparisonExpression).comparisonType === "NOT_EQUAL",
    );
    expect(neFilters).toHaveLength(2);
  });

  it("tightens GREATER_EQUAL + GREATER: x >= 5 AND x > 5 → x > 5", () => {
    const combiner = new FilterCombiner();
    combiner.addFilter({
      expressionClass: BoundExpressionClass.BOUND_COMPARISON,
      comparisonType: "GREATER_EQUAL",
      left: makeColRef(0, 0),
      right: makeIntConstant(5),
      returnType: "BOOLEAN",
    });
    combiner.addFilter({
      expressionClass: BoundExpressionClass.BOUND_COMPARISON,
      comparisonType: "GREATER",
      left: makeColRef(0, 0),
      right: makeIntConstant(5),
      returnType: "BOOLEAN",
    });

    const filters = combiner.generateFilters();
    const comparisons = filters.filter(
      (f) => f.expressionClass === BoundExpressionClass.BOUND_COMPARISON,
    ) as BoundComparisonExpression[];
    expect(comparisons).toHaveLength(1);
    expect(comparisons[0].comparisonType).toBe("GREATER");
  });

  it("tightens LESS_EQUAL + LESS: x <= 5 AND x < 5 → x < 5", () => {
    const combiner = new FilterCombiner();
    combiner.addFilter({
      expressionClass: BoundExpressionClass.BOUND_COMPARISON,
      comparisonType: "LESS_EQUAL",
      left: makeColRef(0, 0),
      right: makeIntConstant(5),
      returnType: "BOOLEAN",
    });
    combiner.addFilter({
      expressionClass: BoundExpressionClass.BOUND_COMPARISON,
      comparisonType: "LESS",
      left: makeColRef(0, 0),
      right: makeIntConstant(5),
      returnType: "BOOLEAN",
    });

    const filters = combiner.generateFilters();
    const comparisons = filters.filter(
      (f) => f.expressionClass === BoundExpressionClass.BOUND_COMPARISON,
    ) as BoundComparisonExpression[];
    expect(comparisons).toHaveLength(1);
    expect(comparisons[0].comparisonType).toBe("LESS");
  });

  it("keeps triple range filters, retains tightest: x > 3 AND x > 5 AND x > 7 → x > 7", () => {
    const combiner = new FilterCombiner();
    for (const v of [3, 5, 7]) {
      combiner.addFilter({
        expressionClass: BoundExpressionClass.BOUND_COMPARISON,
        comparisonType: "GREATER",
        left: makeColRef(0, 0),
        right: makeIntConstant(v),
        returnType: "BOOLEAN",
      });
    }

    const filters = combiner.generateFilters();
    const comparisons = filters.filter(
      (f) => f.expressionClass === BoundExpressionClass.BOUND_COMPARISON,
    ) as BoundComparisonExpression[];
    expect(comparisons).toHaveLength(1);
    expect((comparisons[0].right as BoundConstantExpression).value).toBe(7);
  });

  it("generates table filters for multiple columns", () => {
    const combiner = new FilterCombiner();
    combiner.addFilter({
      expressionClass: BoundExpressionClass.BOUND_COMPARISON,
      comparisonType: "GREATER",
      left: makeColRef(0, 2),
      right: makeIntConstant(18),
      returnType: "BOOLEAN",
    });
    combiner.addFilter({
      expressionClass: BoundExpressionClass.BOUND_COMPARISON,
      comparisonType: "EQUAL",
      left: makeColRef(0, 0),
      right: makeIntConstant(5),
      returnType: "BOOLEAN",
    });

    const tableFilters = combiner.generateTableFilters(0);
    expect(tableFilters).toHaveLength(2);
    const colIndices = tableFilters.map((f) => f.columnIndex).sort();
    expect(colIndices).toEqual([0, 2]);
  });
});

describe("FilterCombiner — boundary conditions", () => {
  it("keeps parameter comparisons (can't optimize at compile time)", () => {
    const combiner = new FilterCombiner();
    const param: BoundParameterExpression = {
      expressionClass: BoundExpressionClass.BOUND_PARAMETER,
      index: 0,
      returnType: "INTEGER",
    };
    combiner.addFilter({
      expressionClass: BoundExpressionClass.BOUND_COMPARISON,
      comparisonType: "GREATER",
      left: makeColRef(0, 0),
      right: param,
      returnType: "BOOLEAN",
    });
    combiner.addFilter({
      expressionClass: BoundExpressionClass.BOUND_COMPARISON,
      comparisonType: "GREATER",
      left: makeColRef(0, 0),
      right: makeIntConstant(5),
      returnType: "BOOLEAN",
    });
    const filters = combiner.generateFilters();
    const comparisons = filters.filter(
      (f) => f.expressionClass === BoundExpressionClass.BOUND_COMPARISON,
    );
    expect(comparisons).toHaveLength(2);
  });

  it("detects unsatisfiable non-numeric equality: x = 'a' AND x = 'b'", () => {
    const combiner = new FilterCombiner();
    combiner.addFilter({
      expressionClass: BoundExpressionClass.BOUND_COMPARISON,
      comparisonType: "EQUAL",
      left: makeColRef(0, 0),
      right: makeStrConstant("a"),
      returnType: "BOOLEAN",
    });
    combiner.addFilter({
      expressionClass: BoundExpressionClass.BOUND_COMPARISON,
      comparisonType: "EQUAL",
      left: makeColRef(0, 0),
      right: makeStrConstant("b"),
      returnType: "BOOLEAN",
    });
    const filters = combiner.generateFilters();
    expect(filters).toHaveLength(1);
    expect((filters[0] as BoundConstantExpression).value).toBe(false);
  });

  it("prunes redundant non-numeric equality: x = 'a' AND x = 'a'", () => {
    const combiner = new FilterCombiner();
    combiner.addFilter({
      expressionClass: BoundExpressionClass.BOUND_COMPARISON,
      comparisonType: "EQUAL",
      left: makeColRef(0, 0),
      right: makeStrConstant("hello"),
      returnType: "BOOLEAN",
    });
    combiner.addFilter({
      expressionClass: BoundExpressionClass.BOUND_COMPARISON,
      comparisonType: "EQUAL",
      left: makeColRef(0, 0),
      right: makeStrConstant("hello"),
      returnType: "BOOLEAN",
    });
    const filters = combiner.generateFilters();
    const comparisons = filters.filter(
      (f) => f.expressionClass === BoundExpressionClass.BOUND_COMPARISON,
    );
    expect(comparisons).toHaveLength(1);
  });

  it("keeps non-numeric with range comparison", () => {
    const combiner = new FilterCombiner();
    combiner.addFilter({
      expressionClass: BoundExpressionClass.BOUND_COMPARISON,
      comparisonType: "GREATER",
      left: makeColRef(0, 0),
      right: makeStrConstant("a"),
      returnType: "BOOLEAN",
    });
    combiner.addFilter({
      expressionClass: BoundExpressionClass.BOUND_COMPARISON,
      comparisonType: "LESS",
      left: makeColRef(0, 0),
      right: makeStrConstant("z"),
      returnType: "BOOLEAN",
    });
    const filters = combiner.generateFilters();
    const comparisons = filters.filter(
      (f) => f.expressionClass === BoundExpressionClass.BOUND_COMPARISON,
    );
    expect(comparisons).toHaveLength(2);
  });

  it("detects unsatisfiable opposite directions: x < 5 AND x > 10", () => {
    const combiner = new FilterCombiner();
    combiner.addFilter({
      expressionClass: BoundExpressionClass.BOUND_COMPARISON,
      comparisonType: "LESS",
      left: makeColRef(0, 0),
      right: makeIntConstant(5),
      returnType: "BOOLEAN",
    });
    combiner.addFilter({
      expressionClass: BoundExpressionClass.BOUND_COMPARISON,
      comparisonType: "GREATER",
      left: makeColRef(0, 0),
      right: makeIntConstant(10),
      returnType: "BOOLEAN",
    });
    const filters = combiner.generateFilters();
    expect(filters).toHaveLength(1);
    expect((filters[0] as BoundConstantExpression).value).toBe(false);
  });

  it("detects unsatisfiable strict bounds: x <= 5 AND x > 5", () => {
    const combiner = new FilterCombiner();
    combiner.addFilter({
      expressionClass: BoundExpressionClass.BOUND_COMPARISON,
      comparisonType: "LESS_EQUAL",
      left: makeColRef(0, 0),
      right: makeIntConstant(5),
      returnType: "BOOLEAN",
    });
    combiner.addFilter({
      expressionClass: BoundExpressionClass.BOUND_COMPARISON,
      comparisonType: "GREATER",
      left: makeColRef(0, 0),
      right: makeIntConstant(5),
      returnType: "BOOLEAN",
    });
    const filters = combiner.generateFilters();
    expect(filters).toHaveLength(1);
    expect((filters[0] as BoundConstantExpression).value).toBe(false);
  });

  it("detects unsatisfiable: x >= 5 AND x < 5", () => {
    const combiner = new FilterCombiner();
    combiner.addFilter({
      expressionClass: BoundExpressionClass.BOUND_COMPARISON,
      comparisonType: "GREATER_EQUAL",
      left: makeColRef(0, 0),
      right: makeIntConstant(5),
      returnType: "BOOLEAN",
    });
    combiner.addFilter({
      expressionClass: BoundExpressionClass.BOUND_COMPARISON,
      comparisonType: "LESS",
      left: makeColRef(0, 0),
      right: makeIntConstant(5),
      returnType: "BOOLEAN",
    });
    const filters = combiner.generateFilters();
    expect(filters).toHaveLength(1);
    expect((filters[0] as BoundConstantExpression).value).toBe(false);
  });

  it("compatible opposite bounds: x >= 5 AND x <= 10", () => {
    const combiner = new FilterCombiner();
    combiner.addFilter({
      expressionClass: BoundExpressionClass.BOUND_COMPARISON,
      comparisonType: "GREATER_EQUAL",
      left: makeColRef(0, 0),
      right: makeIntConstant(5),
      returnType: "BOOLEAN",
    });
    combiner.addFilter({
      expressionClass: BoundExpressionClass.BOUND_COMPARISON,
      comparisonType: "LESS_EQUAL",
      left: makeColRef(0, 0),
      right: makeIntConstant(10),
      returnType: "BOOLEAN",
    });
    const filters = combiner.generateFilters();
    const comparisons = filters.filter(
      (f) => f.expressionClass === BoundExpressionClass.BOUND_COMPARISON,
    );
    expect(comparisons).toHaveLength(2);
  });

  it("EQUAL satisfies range: x = 7 AND x > 5 → keeps both", () => {
    const combiner = new FilterCombiner();
    combiner.addFilter({
      expressionClass: BoundExpressionClass.BOUND_COMPARISON,
      comparisonType: "GREATER",
      left: makeColRef(0, 0),
      right: makeIntConstant(5),
      returnType: "BOOLEAN",
    });
    combiner.addFilter({
      expressionClass: BoundExpressionClass.BOUND_COMPARISON,
      comparisonType: "EQUAL",
      left: makeColRef(0, 0),
      right: makeIntConstant(7),
      returnType: "BOOLEAN",
    });
    const filters = combiner.generateFilters();
    const comparisons = filters.filter(
      (f) => f.expressionClass === BoundExpressionClass.BOUND_COMPARISON,
    ) as BoundComparisonExpression[];
    expect(comparisons.some((c) => c.comparisonType === "EQUAL")).toBe(true);
  });

  it("EQUAL violates range: x = 3 AND x > 5 → unsatisfiable", () => {
    const combiner = new FilterCombiner();
    combiner.addFilter({
      expressionClass: BoundExpressionClass.BOUND_COMPARISON,
      comparisonType: "GREATER",
      left: makeColRef(0, 0),
      right: makeIntConstant(5),
      returnType: "BOOLEAN",
    });
    combiner.addFilter({
      expressionClass: BoundExpressionClass.BOUND_COMPARISON,
      comparisonType: "EQUAL",
      left: makeColRef(0, 0),
      right: makeIntConstant(3),
      returnType: "BOOLEAN",
    });
    const filters = combiner.generateFilters();
    expect(filters).toHaveLength(1);
    expect((filters[0] as BoundConstantExpression).value).toBe(false);
  });

  it("existing EQUAL checked against incoming range: x = 7 then x < 5 → unsatisfiable", () => {
    const combiner = new FilterCombiner();
    combiner.addFilter({
      expressionClass: BoundExpressionClass.BOUND_COMPARISON,
      comparisonType: "EQUAL",
      left: makeColRef(0, 0),
      right: makeIntConstant(7),
      returnType: "BOOLEAN",
    });
    combiner.addFilter({
      expressionClass: BoundExpressionClass.BOUND_COMPARISON,
      comparisonType: "LESS",
      left: makeColRef(0, 0),
      right: makeIntConstant(5),
      returnType: "BOOLEAN",
    });
    const filters = combiner.generateFilters();
    expect(filters).toHaveLength(1);
    expect((filters[0] as BoundConstantExpression).value).toBe(false);
  });

  it("flips constant on left: 5 < x → x > 5", () => {
    const combiner = new FilterCombiner();
    combiner.addFilter({
      expressionClass: BoundExpressionClass.BOUND_COMPARISON,
      comparisonType: "LESS",
      left: makeIntConstant(5),
      right: makeColRef(0, 0),
      returnType: "BOOLEAN",
    });
    const filters = combiner.generateFilters();
    const cmp = filters.find(
      (f) => f.expressionClass === BoundExpressionClass.BOUND_COMPARISON,
    ) as BoundComparisonExpression;
    expect(cmp).toBeDefined();
    expect(cmp.comparisonType).toBe("GREATER");
  });

  it("does not generate transitive filters from parameter values", () => {
    const combiner = new FilterCombiner();
    combiner.addFilter({
      expressionClass: BoundExpressionClass.BOUND_COMPARISON,
      comparisonType: "EQUAL",
      left: makeColRef(0, 0),
      right: makeColRef(1, 0),
      returnType: "BOOLEAN",
    });
    const param: BoundParameterExpression = {
      expressionClass: BoundExpressionClass.BOUND_PARAMETER,
      index: 0,
      returnType: "INTEGER",
    };
    combiner.addFilter({
      expressionClass: BoundExpressionClass.BOUND_COMPARISON,
      comparisonType: "EQUAL",
      left: makeColRef(0, 0),
      right: param,
      returnType: "BOOLEAN",
    });
    const filters = combiner.generateFilters();
    const table1Filters = filters.filter((f) => {
      if (f.expressionClass !== BoundExpressionClass.BOUND_COMPARISON) return false;
      const cmp = f as BoundComparisonExpression;
      if (cmp.left.expressionClass !== BoundExpressionClass.BOUND_COLUMN_REF) return false;
      return (cmp.left as BoundColumnRefExpression).binding.tableIndex === 1;
    });
    expect(table1Filters).toHaveLength(0);
  });

  it("generates table filters and skips unsatisfiable markers", () => {
    const combiner = new FilterCombiner();
    combiner.addFilter({
      expressionClass: BoundExpressionClass.BOUND_COMPARISON,
      comparisonType: "EQUAL",
      left: makeColRef(0, 0),
      right: makeIntConstant(5),
      returnType: "BOOLEAN",
    });
    combiner.addFilter({
      expressionClass: BoundExpressionClass.BOUND_COMPARISON,
      comparisonType: "EQUAL",
      left: makeColRef(0, 0),
      right: makeIntConstant(6),
      returnType: "BOOLEAN",
    });
    const tableFilters = combiner.generateTableFilters(0);
    expect(tableFilters).toHaveLength(0);
  });

  it("handles parameter filter in tableFilter generation", () => {
    const combiner = new FilterCombiner();
    const param: BoundParameterExpression = {
      expressionClass: BoundExpressionClass.BOUND_PARAMETER,
      index: 0,
      returnType: "INTEGER",
    };
    combiner.addFilter({
      expressionClass: BoundExpressionClass.BOUND_COMPARISON,
      comparisonType: "EQUAL",
      left: makeColRef(0, 2),
      right: param,
      returnType: "BOOLEAN",
    });
    const tableFilters = combiner.generateTableFilters(0);
    expect(tableFilters).toHaveLength(1);
    expect(tableFilters[0].columnIndex).toBe(2);
  });

  it("tightens upper bounds: x < 5 AND x <= 5 → x < 5", () => {
    const combiner = new FilterCombiner();
    combiner.addFilter({
      expressionClass: BoundExpressionClass.BOUND_COMPARISON,
      comparisonType: "LESS_EQUAL",
      left: makeColRef(0, 0),
      right: makeIntConstant(5),
      returnType: "BOOLEAN",
    });
    combiner.addFilter({
      expressionClass: BoundExpressionClass.BOUND_COMPARISON,
      comparisonType: "LESS",
      left: makeColRef(0, 0),
      right: makeIntConstant(5),
      returnType: "BOOLEAN",
    });
    const filters = combiner.generateFilters();
    const comparisons = filters.filter(
      (f) => f.expressionClass === BoundExpressionClass.BOUND_COMPARISON,
    ) as BoundComparisonExpression[];
    expect(comparisons).toHaveLength(1);
    expect(comparisons[0].comparisonType).toBe("LESS");
  });

  it("tightens upper bounds by value: x < 5 AND x < 3 → x < 3", () => {
    const combiner = new FilterCombiner();
    combiner.addFilter({
      expressionClass: BoundExpressionClass.BOUND_COMPARISON,
      comparisonType: "LESS",
      left: makeColRef(0, 0),
      right: makeIntConstant(5),
      returnType: "BOOLEAN",
    });
    combiner.addFilter({
      expressionClass: BoundExpressionClass.BOUND_COMPARISON,
      comparisonType: "LESS",
      left: makeColRef(0, 0),
      right: makeIntConstant(3),
      returnType: "BOOLEAN",
    });
    const filters = combiner.generateFilters();
    const comparisons = filters.filter(
      (f) => f.expressionClass === BoundExpressionClass.BOUND_COMPARISON,
    ) as BoundComparisonExpression[];
    expect(comparisons).toHaveLength(1);
    expect((comparisons[0].right as BoundConstantExpression).value).toBe(3);
  });

  it("pushes non-comparison expressions to remainingFilters", () => {
    const combiner = new FilterCombiner();
    combiner.addFilter({
      expressionClass: BoundExpressionClass.BOUND_OPERATOR,
      operatorType: "IS_NOT_NULL",
      children: [makeColRef(0, 0)],
      returnType: "BOOLEAN",
    } as unknown as BoundExpression);
    const filters = combiner.generateFilters();
    expect(filters).toHaveLength(1);
    expect(filters[0].expressionClass).toBe(BoundExpressionClass.BOUND_OPERATOR);
  });
});
