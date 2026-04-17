import { beforeEach, describe, expect, it } from "vitest";
import type {
  BoundCastExpression,
  BoundColumnRefExpression,
  BoundComparisonExpression,
  BoundConjunctionExpression,
  BoundConstantExpression,
  BoundExpression,
  BoundFunctionExpression,
  BoundOperatorExpression,
  LogicalFilter,
  LogicalOperator,
} from "../../binder/types.js";
import {
  BoundExpressionClass,
  LogicalOperatorType,
} from "../../binder/types.js";
import { reorderFilters } from "../index.js";
import { estimateCost, canThrow } from "../reorder_filter.js";
import {
  containsFunction,
  createTestContext,
  findNode,
  makeColRef,
  makeIntConstant,
  makeStrConstant,
} from "./test_helpers.js";

let bind: (sql: string) => LogicalOperator;

beforeEach(() => {
  const ctx = createTestContext();
  bind = ctx.bind;
});

describe("ReorderFilter", () => {
  it("puts cheap conditions before expensive ones", () => {
    const plan = bind(
      "SELECT * FROM users WHERE upper(name) = 'JOHN' AND age > 18",
    );
    const optimized = reorderFilters(plan);
    const filter = findNode(
      optimized,
      LogicalOperatorType.LOGICAL_FILTER,
    ) as LogicalFilter;
    if (filter && filter.expressions.length > 1) {
      const first = filter.expressions[0];
      const second = filter.expressions[1];
      const firstHasFunction = containsFunction(first);
      const secondHasFunction = containsFunction(second);
      if (firstHasFunction !== secondHasFunction) {
        expect(firstHasFunction).toBe(false);
      }
    }
  });

  it("does not reorder when expression can throw (division)", () => {
    const plan = bind("SELECT * FROM users WHERE age > 0 AND id / age > 5");
    const optimized = reorderFilters(plan);
    const filter = findNode(
      optimized,
      LogicalOperatorType.LOGICAL_FILTER,
    ) as LogicalFilter;
    if (filter && filter.expressions.length > 1) {
      const first = filter.expressions[0] as BoundComparisonExpression;
      expect(first.comparisonType).toBe("GREATER");
      expect((first.right as BoundConstantExpression).value).toBe(0);
    }
  });

  it("preserves single filter unchanged", () => {
    const plan = bind("SELECT * FROM users WHERE age > 18");
    const optimized = reorderFilters(plan);
    const filter = findNode(
      optimized,
      LogicalOperatorType.LOGICAL_FILTER,
    ) as LogicalFilter;
    expect(filter).not.toBeNull();
    expect(filter.expressions).toHaveLength(1);
    expect(
      (filter.expressions[0] as BoundComparisonExpression).comparisonType,
    ).toBe("GREATER");
  });

  it("reorders subquery after cheap column comparison", () => {
    const plan = bind(
      "SELECT * FROM users WHERE id IN (SELECT user_id FROM orders) AND age > 18",
    );
    const optimized = reorderFilters(plan);
    const filter = findNode(
      optimized,
      LogicalOperatorType.LOGICAL_FILTER,
    ) as LogicalFilter;
    if (filter && filter.expressions.length > 1) {
      const first = filter.expressions[0];
      expect(first.expressionClass).not.toBe(
        BoundExpressionClass.BOUND_SUBQUERY,
      );
    }
  });
});

describe("estimateCost", () => {
  it("BOUND_CONSTANT costs 1", () => {
    expect(estimateCost(makeIntConstant(42))).toBe(1);
    expect(estimateCost(makeStrConstant("hello"))).toBe(1);
  });

  it("BOUND_COLUMN_REF — INTEGER costs 8", () => {
    expect(estimateCost(makeColRef(0, 0))).toBe(8);
  });

  it("BOUND_COLUMN_REF — TEXT costs 40 (8*5)", () => {
    const ref: BoundColumnRefExpression = {
      expressionClass: BoundExpressionClass.BOUND_COLUMN_REF,
      binding: { tableIndex: 0, columnIndex: 1 },
      tableName: "", columnName: "", returnType: "TEXT",
    };
    expect(estimateCost(ref)).toBe(40);
  });

  it("BOUND_COLUMN_REF — REAL costs 16 (8*2)", () => {
    const ref: BoundColumnRefExpression = {
      expressionClass: BoundExpressionClass.BOUND_COLUMN_REF,
      binding: { tableIndex: 0, columnIndex: 0 },
      tableName: "", columnName: "", returnType: "REAL",
    };
    expect(estimateCost(ref)).toBe(16);
  });

  it("BOUND_COLUMN_REF — BLOB costs 40 (8*5)", () => {
    const ref: BoundColumnRefExpression = {
      expressionClass: BoundExpressionClass.BOUND_COLUMN_REF,
      binding: { tableIndex: 0, columnIndex: 0 },
      tableName: "", columnName: "", returnType: "BLOB",
    };
    expect(estimateCost(ref)).toBe(40);
  });

  it("BOUND_COMPARISON costs 5 + left + right", () => {
    const cmp: BoundComparisonExpression = {
      expressionClass: BoundExpressionClass.BOUND_COMPARISON,
      comparisonType: "EQUAL",
      left: makeColRef(0, 0),
      right: makeIntConstant(5),
      returnType: "BOOLEAN",
    };
    expect(estimateCost(cmp)).toBe(5 + 8 + 1);
  });

  it("BOUND_CONJUNCTION costs 5 + sum(children)", () => {
    const conj: BoundConjunctionExpression = {
      expressionClass: BoundExpressionClass.BOUND_CONJUNCTION,
      conjunctionType: "AND",
      children: [makeIntConstant(1), makeColRef(0, 0), makeIntConstant(2)],
      returnType: "BOOLEAN",
    };
    expect(estimateCost(conj)).toBe(5 + 1 + 8 + 1);
  });

  describe("BOUND_OPERATOR — operatorType variants", () => {
    function makeOp(operatorType: string, children: BoundExpression[]): BoundOperatorExpression {
      return {
        expressionClass: BoundExpressionClass.BOUND_OPERATOR,
        operatorType: operatorType as BoundOperatorExpression["operatorType"],
        children,
        returnType: "INTEGER",
      };
    }

    it("ADD base cost 5", () => {
      expect(estimateCost(makeOp("ADD", [makeIntConstant(1), makeIntConstant(2)]))).toBe(5 + 1 + 1);
    });

    it("SUBTRACT base cost 5", () => {
      expect(estimateCost(makeOp("SUBTRACT", [makeIntConstant(1), makeIntConstant(2)]))).toBe(5 + 1 + 1);
    });

    it("MULTIPLY base cost 10", () => {
      expect(estimateCost(makeOp("MULTIPLY", [makeIntConstant(1), makeIntConstant(2)]))).toBe(10 + 1 + 1);
    });

    it("DIVIDE base cost 15", () => {
      expect(estimateCost(makeOp("DIVIDE", [makeIntConstant(1), makeIntConstant(2)]))).toBe(15 + 1 + 1);
    });

    it("MOD base cost 15", () => {
      expect(estimateCost(makeOp("MOD", [makeIntConstant(1), makeIntConstant(2)]))).toBe(15 + 1 + 1);
    });

    it("NOT base cost 3", () => {
      expect(estimateCost(makeOp("NOT", [makeIntConstant(1)]))).toBe(3 + 1);
    });

    it("IS_NULL base cost 3", () => {
      expect(estimateCost(makeOp("IS_NULL", [makeColRef(0, 0)]))).toBe(3 + 8);
    });

    it("IS_NOT_NULL base cost 3", () => {
      expect(estimateCost(makeOp("IS_NOT_NULL", [makeColRef(0, 0)]))).toBe(3 + 8);
    });

    it("NEGATE base cost 3", () => {
      expect(estimateCost(makeOp("NEGATE", [makeIntConstant(1)]))).toBe(3 + 1);
    });

    it("IN base cost (N-1)*100", () => {
      const children = [makeColRef(0, 0), makeIntConstant(1), makeIntConstant(2), makeIntConstant(3)];
      expect(estimateCost(makeOp("IN", children))).toBe(300 + 8 + 1 + 1 + 1);
    });

    it("NOT_IN base cost (N-1)*100", () => {
      const children = [makeColRef(0, 0), makeIntConstant(1), makeIntConstant(2)];
      expect(estimateCost(makeOp("NOT_IN", children))).toBe(200 + 8 + 1 + 1);
    });

    it("CONCAT (default) base cost 10", () => {
      expect(estimateCost(makeOp("CONCAT", [makeIntConstant(1), makeIntConstant(2)]))).toBe(10 + 1 + 1);
    });
  });

  describe("BOUND_FUNCTION — functionCost variants", () => {
    function makeFunc(name: string, children: BoundExpression[] = [makeIntConstant(1)]): BoundFunctionExpression {
      return {
        expressionClass: BoundExpressionClass.BOUND_FUNCTION,
        functionName: name,
        children,
        returnType: "INTEGER",
      };
    }

    it("ABS costs 5", () => expect(estimateCost(makeFunc("ABS"))).toBe(5 + 1));
    it("SIGN costs 5", () => expect(estimateCost(makeFunc("SIGN"))).toBe(5 + 1));
    it("UPPER costs 200", () => expect(estimateCost(makeFunc("UPPER"))).toBe(200 + 1));
    it("LOWER costs 200", () => expect(estimateCost(makeFunc("LOWER"))).toBe(200 + 1));
    it("TRIM costs 200", () => expect(estimateCost(makeFunc("TRIM"))).toBe(200 + 1));
    it("LTRIM costs 200", () => expect(estimateCost(makeFunc("LTRIM"))).toBe(200 + 1));
    it("RTRIM costs 200", () => expect(estimateCost(makeFunc("RTRIM"))).toBe(200 + 1));
    it("LENGTH costs 10", () => expect(estimateCost(makeFunc("LENGTH"))).toBe(10 + 1));
    it("TYPEOF costs 10", () => expect(estimateCost(makeFunc("TYPEOF"))).toBe(10 + 1));
    it("SUBSTR costs 200", () => expect(estimateCost(makeFunc("SUBSTR"))).toBe(200 + 1));
    it("SUBSTRING costs 200", () => expect(estimateCost(makeFunc("SUBSTRING"))).toBe(200 + 1));
    it("REPLACE costs 200", () => expect(estimateCost(makeFunc("REPLACE"))).toBe(200 + 1));
    it("INSTR costs 200", () => expect(estimateCost(makeFunc("INSTR"))).toBe(200 + 1));
    it("COALESCE costs 5", () => expect(estimateCost(makeFunc("COALESCE"))).toBe(5 + 1));
    it("IFNULL costs 5", () => expect(estimateCost(makeFunc("IFNULL"))).toBe(5 + 1));
    it("NULLIF costs 5", () => expect(estimateCost(makeFunc("NULLIF"))).toBe(5 + 1));
    it("HEX costs 200", () => expect(estimateCost(makeFunc("HEX"))).toBe(200 + 1));
    it("UNHEX costs 200", () => expect(estimateCost(makeFunc("UNHEX"))).toBe(200 + 1));
    it("QUOTE costs 200", () => expect(estimateCost(makeFunc("QUOTE"))).toBe(200 + 1));
    it("unknown function costs 100", () => expect(estimateCost(makeFunc("MY_FUNC"))).toBe(100 + 1));

    it("is case-insensitive", () => {
      expect(estimateCost(makeFunc("abs"))).toBe(5 + 1);
      expect(estimateCost(makeFunc("Upper"))).toBe(200 + 1);
    });

    it("accumulates child costs", () => {
      const f = makeFunc("ABS", [makeColRef(0, 0), makeIntConstant(2)]);
      expect(estimateCost(f)).toBe(5 + 8 + 1);
    });
  });

  describe("BOUND_CAST", () => {
    function makeCast(castType: "INTEGER" | "REAL" | "TEXT" | "BLOB", child: BoundExpression): BoundCastExpression {
      return {
        expressionClass: BoundExpressionClass.BOUND_CAST,
        child,
        castType,
        returnType: castType,
      };
    }

    it("CAST to TEXT costs 200 + child", () => {
      expect(estimateCost(makeCast("TEXT", makeIntConstant(1)))).toBe(200 + 1);
    });

    it("CAST to BLOB costs 200 + child", () => {
      expect(estimateCost(makeCast("BLOB", makeIntConstant(1)))).toBe(200 + 1);
    });

    it("CAST to INTEGER costs 5 + child", () => {
      expect(estimateCost(makeCast("INTEGER", makeStrConstant("42")))).toBe(5 + 1);
    });

    it("CAST to REAL costs 5 + child", () => {
      expect(estimateCost(makeCast("REAL", makeIntConstant(1)))).toBe(5 + 1);
    });
  });

  it("BOUND_SUBQUERY costs 10000", () => {
    const expr: BoundExpression = {
      expressionClass: BoundExpressionClass.BOUND_SUBQUERY,
      subqueryType: "SCALAR",
      subplan: { type: LogicalOperatorType.LOGICAL_GET, children: [], expressions: [], types: [], estimatedCardinality: 0, getColumnBindings: () => [] } as unknown as LogicalOperator,
      returnType: "INTEGER",
    } as BoundExpression;
    expect(estimateCost(expr)).toBe(10000);
  });

  it("BOUND_CASE costs 50", () => {
    const expr: BoundExpression = {
      expressionClass: BoundExpressionClass.BOUND_CASE,
      caseChecks: [],
      elseExpr: null,
      returnType: "INTEGER",
    } as unknown as BoundExpression;
    expect(estimateCost(expr)).toBe(50);
  });

  it("BOUND_BETWEEN costs 15", () => {
    const expr: BoundExpression = {
      expressionClass: BoundExpressionClass.BOUND_BETWEEN,
      input: makeColRef(0, 0),
      lower: makeIntConstant(1),
      upper: makeIntConstant(10),
      returnType: "BOOLEAN",
    } as unknown as BoundExpression;
    expect(estimateCost(expr)).toBe(15);
  });

  it("BOUND_AGGREGATE costs 100", () => {
    const expr: BoundExpression = {
      expressionClass: BoundExpressionClass.BOUND_AGGREGATE,
      functionName: "COUNT",
      children: [],
      distinct: false,
      isStar: true,
      aggregateIndex: 0,
      returnType: "INTEGER",
    } as unknown as BoundExpression;
    expect(estimateCost(expr)).toBe(100);
  });

  it("unknown expression class defaults to 10", () => {
    const expr = {
      expressionClass: "BOUND_UNKNOWN" as BoundExpressionClass,
      returnType: "INTEGER",
    } as unknown as BoundExpression;
    expect(estimateCost(expr)).toBe(10);
  });

  it("nested expressions accumulate cost correctly", () => {
    const innerAdd: BoundOperatorExpression = {
      expressionClass: BoundExpressionClass.BOUND_OPERATOR,
      operatorType: "ADD",
      children: [makeColRef(0, 0), makeIntConstant(1)],
      returnType: "INTEGER",
    };
    const absFunc: BoundFunctionExpression = {
      expressionClass: BoundExpressionClass.BOUND_FUNCTION,
      functionName: "ABS",
      children: [innerAdd],
      returnType: "INTEGER",
    };
    const textCol: BoundColumnRefExpression = {
      expressionClass: BoundExpressionClass.BOUND_COLUMN_REF,
      binding: { tableIndex: 0, columnIndex: 1 },
      tableName: "", columnName: "", returnType: "TEXT",
    };
    const upperFunc: BoundFunctionExpression = {
      expressionClass: BoundExpressionClass.BOUND_FUNCTION,
      functionName: "UPPER",
      children: [textCol],
      returnType: "TEXT",
    };
    const cmp: BoundComparisonExpression = {
      expressionClass: BoundExpressionClass.BOUND_COMPARISON,
      comparisonType: "GREATER",
      left: absFunc,
      right: upperFunc,
      returnType: "BOOLEAN",
    };
    expect(estimateCost(cmp)).toBe(264);
  });
});

describe("canThrow", () => {
  it("DIVIDE returns true", () => {
    const op: BoundOperatorExpression = {
      expressionClass: BoundExpressionClass.BOUND_OPERATOR,
      operatorType: "DIVIDE",
      children: [makeColRef(0, 0), makeColRef(0, 1)],
      returnType: "INTEGER",
    };
    expect(canThrow(op)).toBe(true);
  });

  it("MOD returns true", () => {
    const op: BoundOperatorExpression = {
      expressionClass: BoundExpressionClass.BOUND_OPERATOR,
      operatorType: "MOD",
      children: [makeColRef(0, 0), makeColRef(0, 1)],
      returnType: "INTEGER",
    };
    expect(canThrow(op)).toBe(true);
  });

  it("ADD returns false (no throwing children)", () => {
    const op: BoundOperatorExpression = {
      expressionClass: BoundExpressionClass.BOUND_OPERATOR,
      operatorType: "ADD",
      children: [makeColRef(0, 0), makeIntConstant(1)],
      returnType: "INTEGER",
    };
    expect(canThrow(op)).toBe(false);
  });

  it("ADD with DIVIDE child returns true", () => {
    const div: BoundOperatorExpression = {
      expressionClass: BoundExpressionClass.BOUND_OPERATOR,
      operatorType: "DIVIDE",
      children: [makeColRef(0, 0), makeColRef(0, 1)],
      returnType: "INTEGER",
    };
    const op: BoundOperatorExpression = {
      expressionClass: BoundExpressionClass.BOUND_OPERATOR,
      operatorType: "ADD",
      children: [div, makeIntConstant(1)],
      returnType: "INTEGER",
    };
    expect(canThrow(op)).toBe(true);
  });

  it("COMPARISON with non-throwing children returns false", () => {
    const cmp: BoundComparisonExpression = {
      expressionClass: BoundExpressionClass.BOUND_COMPARISON,
      comparisonType: "EQUAL",
      left: makeColRef(0, 0),
      right: makeIntConstant(5),
      returnType: "BOOLEAN",
    };
    expect(canThrow(cmp)).toBe(false);
  });

  it("COMPARISON with throwing left returns true", () => {
    const div: BoundOperatorExpression = {
      expressionClass: BoundExpressionClass.BOUND_OPERATOR,
      operatorType: "DIVIDE",
      children: [makeColRef(0, 0), makeColRef(0, 1)],
      returnType: "INTEGER",
    };
    const cmp: BoundComparisonExpression = {
      expressionClass: BoundExpressionClass.BOUND_COMPARISON,
      comparisonType: "GREATER",
      left: div,
      right: makeIntConstant(5),
      returnType: "BOOLEAN",
    };
    expect(canThrow(cmp)).toBe(true);
  });

  it("COMPARISON with throwing right returns true", () => {
    const mod: BoundOperatorExpression = {
      expressionClass: BoundExpressionClass.BOUND_OPERATOR,
      operatorType: "MOD",
      children: [makeColRef(0, 0), makeColRef(0, 1)],
      returnType: "INTEGER",
    };
    const cmp: BoundComparisonExpression = {
      expressionClass: BoundExpressionClass.BOUND_COMPARISON,
      comparisonType: "EQUAL",
      left: makeIntConstant(1),
      right: mod,
      returnType: "BOOLEAN",
    };
    expect(canThrow(cmp)).toBe(true);
  });

  it("CONJUNCTION with non-throwing children returns false", () => {
    const conj: BoundConjunctionExpression = {
      expressionClass: BoundExpressionClass.BOUND_CONJUNCTION,
      conjunctionType: "AND",
      children: [makeIntConstant(1), makeColRef(0, 0)],
      returnType: "BOOLEAN",
    };
    expect(canThrow(conj)).toBe(false);
  });

  it("CONJUNCTION with throwing child returns true", () => {
    const div: BoundOperatorExpression = {
      expressionClass: BoundExpressionClass.BOUND_OPERATOR,
      operatorType: "DIVIDE",
      children: [makeColRef(0, 0), makeColRef(0, 1)],
      returnType: "INTEGER",
    };
    const conj: BoundConjunctionExpression = {
      expressionClass: BoundExpressionClass.BOUND_CONJUNCTION,
      conjunctionType: "AND",
      children: [makeIntConstant(1), div],
      returnType: "BOOLEAN",
    };
    expect(canThrow(conj)).toBe(true);
  });

  it("FUNCTION with non-throwing children returns false", () => {
    const func: BoundFunctionExpression = {
      expressionClass: BoundExpressionClass.BOUND_FUNCTION,
      functionName: "ABS",
      children: [makeColRef(0, 0)],
      returnType: "INTEGER",
    };
    expect(canThrow(func)).toBe(false);
  });

  it("FUNCTION with throwing child returns true", () => {
    const div: BoundOperatorExpression = {
      expressionClass: BoundExpressionClass.BOUND_OPERATOR,
      operatorType: "DIVIDE",
      children: [makeColRef(0, 0), makeColRef(0, 1)],
      returnType: "INTEGER",
    };
    const func: BoundFunctionExpression = {
      expressionClass: BoundExpressionClass.BOUND_FUNCTION,
      functionName: "ABS",
      children: [div],
      returnType: "INTEGER",
    };
    expect(canThrow(func)).toBe(true);
  });

  it("CAST with non-throwing child returns false", () => {
    const cast: BoundCastExpression = {
      expressionClass: BoundExpressionClass.BOUND_CAST,
      child: makeIntConstant(1),
      castType: "TEXT",
      returnType: "TEXT",
    };
    expect(canThrow(cast)).toBe(false);
  });

  it("CAST with throwing child returns true", () => {
    const div: BoundOperatorExpression = {
      expressionClass: BoundExpressionClass.BOUND_OPERATOR,
      operatorType: "DIVIDE",
      children: [makeColRef(0, 0), makeColRef(0, 1)],
      returnType: "INTEGER",
    };
    const cast: BoundCastExpression = {
      expressionClass: BoundExpressionClass.BOUND_CAST,
      child: div,
      castType: "TEXT",
      returnType: "TEXT",
    };
    expect(canThrow(cast)).toBe(true);
  });

  it("CONSTANT returns false", () => {
    expect(canThrow(makeIntConstant(42))).toBe(false);
  });

  it("COLUMN_REF returns false", () => {
    expect(canThrow(makeColRef(0, 0))).toBe(false);
  });

  it("deeply nested throwing expression bubbles up", () => {
    const div: BoundOperatorExpression = {
      expressionClass: BoundExpressionClass.BOUND_OPERATOR,
      operatorType: "DIVIDE",
      children: [makeColRef(0, 0), makeColRef(0, 1)],
      returnType: "INTEGER",
    };
    const func: BoundFunctionExpression = {
      expressionClass: BoundExpressionClass.BOUND_FUNCTION,
      functionName: "ABS",
      children: [div],
      returnType: "INTEGER",
    };
    const cast: BoundCastExpression = {
      expressionClass: BoundExpressionClass.BOUND_CAST,
      child: func,
      castType: "INTEGER",
      returnType: "INTEGER",
    };
    const cmp: BoundComparisonExpression = {
      expressionClass: BoundExpressionClass.BOUND_COMPARISON,
      comparisonType: "GREATER",
      left: cast,
      right: makeIntConstant(0),
      returnType: "BOOLEAN",
    };
    expect(canThrow(cmp)).toBe(true);
  });
});
