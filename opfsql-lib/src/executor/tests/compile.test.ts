import { describe, expect, it, vi } from "vitest";
import {
  DatatypeMismatchError,
  IntegerOverflowError,
  MisuseError,
} from "../../errors.js";
import type {
  BoundExpression,
  BoundFunctionExpression,
  BoundSubqueryExpression,
  LogicalOperator,
  TableFilter,
} from "../../sql/binder/types.js";
import { BoundExpressionClass } from "../../sql/binder/types.js";
import type { Value } from "../../values/index.js";
import {
  compileExpression,
  compilePredicate,
  evaluateConstant,
  type ExecutionContext,
} from "../evaluate/index.js";
import { compileTableFilter } from "../operators/shared.js";
import { buildResolver } from "../resolve.js";
import type { Tuple } from "../types.js";
import {
  between,
  caseExpr,
  cast,
  colRef,
  comparison,
  conjunction,
  constant,
  fnCall,
  jsonAccess,
  layout,
  noopCtx,
  operator,
  withParameters,
} from "./helpers.js";

// Rows of [c0 INTEGER, c1 TEXT, c2 (NULL), c3 BOOLEAN] in table 0.
const resolver = buildResolver(layout([0, 0], [0, 1], [0, 2], [0, 3]));
const row: Tuple = [10, "hello", null, true];

function evaluate(
  expression: BoundExpression,
  input: Tuple = row,
  context: ExecutionContext = noopCtx,
): Value {
  return compileExpression(expression, resolver, context)(input);
}

const NULL = constant(null);
const TRUE = constant(true);
const FALSE = constant(false);

function blob(...bytes: number[]): BoundExpression {
  return {
    expressionClass: BoundExpressionClass.BOUND_CONSTANT,
    value: new Uint8Array(bytes),
    returnType: "BLOB",
  };
}

/** An expression that returns `value` and counts its evaluations. */
function counting(value: Value): {
  expression: BoundExpression;
  calls: () => number;
} {
  let calls = 0;
  const base = fnCall("upper", [constant("x")]) as BoundFunctionExpression;
  const expression: BoundFunctionExpression = {
    ...base,
    function: {
      ...base.function,
      implement: () => ({
        kind: "eager",
        call: () => {
          calls++;
          return value;
        },
      }),
    },
  };
  return { expression, calls: () => calls };
}

function parameter(index: number) {
  return {
    expressionClass: BoundExpressionClass.BOUND_PARAMETER as const,
    index,
    returnType: "ANY" as const,
  };
}

function json(value: Value): BoundExpression {
  return {
    expressionClass: BoundExpressionClass.BOUND_CONSTANT,
    value,
    returnType: "JSON",
  };
}

function integer(value: number): BoundExpression {
  return constant(value, "INTEGER");
}

function real(value: number): BoundExpression {
  return constant(value, "REAL");
}

function concat(left: BoundExpression, right: BoundExpression): Value {
  return evaluate(operator("CONCAT", [left, right], "TEXT"));
}

describe("columns, constants and parameters", () => {
  it("reads a column at its position", () => {
    expect(evaluate(colRef(0, 1))).toBe("hello");
    expect(evaluate(colRef(0, 2))).toBeNull();
  });

  it("reads a column of the outer row in a correlated subquery", () => {
    const context: ExecutionContext = {
      ...noopCtx,
      outer: { tuple: ["outer"], resolver: buildResolver(layout([7, 0])) },
    };
    expect(evaluate(colRef(7, 0), row, context)).toBe("outer");
  });

  it("fails at compile time for a column that is nowhere", () => {
    expect(() => compileExpression(colRef(5, 0), resolver, noopCtx)).toThrow(
      /not in the row/,
    );
  });

  it("reads parameters, and fails for a missing one", () => {
    expect(evaluate(parameter(1), row, withParameters(["a", "b"]))).toBe("b");
    expect(() => evaluate(parameter(0))).toThrow(MisuseError);
  });
});

describe("comparisons", () => {
  it("are NULL when either side is NULL", () => {
    expect(evaluate(comparison(colRef(0, 2), constant(1)))).toBeNull();
    expect(evaluate(comparison(NULL, NULL))).toBeNull();
  });

  it("use the total order: no numeric text, booleans apart from numbers", () => {
    expect(evaluate(comparison(constant(9), constant("10"), "LESS"))).toBe(
      true,
    );
    expect(evaluate(comparison(constant("10"), constant("9"), "LESS"))).toBe(
      true,
    );
    expect(evaluate(comparison(constant("9"), constant(9)))).toBe(false);
    expect(evaluate(comparison(constant(true), constant(1)))).toBe(false);
  });

  it("compare BLOBs by their bytes", () => {
    expect(evaluate(comparison(blob(1), blob(1)))).toBe(true);
    expect(evaluate(comparison(blob(1), blob(1, 0), "LESS"))).toBe(true);
  });

  it("compare JSON by canonical text", () => {
    expect(
      evaluate(comparison(json({ b: 1, a: 2 }), json({ a: 2, b: 1 }))),
    ).toBe(true);
  });
});

describe("comparison affinity", () => {
  it("converts a value of unknown type to the other side's type", () => {
    const text = withParameters(["10"]);
    expect(evaluate(comparison(colRef(0, 0), parameter(0)), row, text)).toBe(
      true,
    );
    const name = colRef(0, 1, "name", "TEXT");
    expect(
      evaluate(comparison(name, parameter(0)), row, withParameters([5])),
    ).toBe(false);
    expect(
      evaluate(
        comparison(name, parameter(0)),
        [1, "5", null, true],
        withParameters([5]),
      ),
    ).toBe(true);
  });

  it("leaves text that is not exactly a number alone", () => {
    expect(
      evaluate(
        comparison(colRef(0, 0), parameter(0), "LESS"),
        row,
        withParameters(["ten"]),
      ),
    ).toBe(true);
  });

  it("does not convert typed operands", () => {
    // A JSON value "10" is text, not the number 10.
    expect(evaluate(comparison(json("10"), constant(10)))).toBe(false);
  });

  it("applies to IN items, BETWEEN bounds and CASE operands", () => {
    const parameters = withParameters(["10", "5", "20"]);
    expect(
      evaluate(
        operator("IN", [colRef(0, 0), parameter(1), parameter(0)], "BOOLEAN"),
        row,
        parameters,
      ),
    ).toBe(true);
    expect(
      evaluate(
        between(colRef(0, 0), parameter(1), parameter(2)),
        row,
        parameters,
      ),
    ).toBe(true);
    const simple = {
      ...caseExpr(
        [{ when: parameter(0), result: constant("ten") }],
        NULL,
        "TEXT",
      ),
      operand: colRef(0, 0),
    } as BoundExpression;
    expect(evaluate(simple, row, parameters)).toBe("ten");
  });
});

describe("three-valued logic", () => {
  it("AND and OR follow the truth tables", () => {
    expect(evaluate(conjunction("AND", NULL, TRUE))).toBeNull();
    expect(evaluate(conjunction("AND", NULL, FALSE))).toBe(false);
    expect(evaluate(conjunction("OR", NULL, TRUE))).toBe(true);
    expect(evaluate(conjunction("OR", NULL, FALSE))).toBeNull();
  });

  it("AND stops at FALSE and OR at TRUE", () => {
    const failing = cast(constant("x"), "INTEGER");
    expect(evaluate(conjunction("AND", FALSE, failing))).toBe(false);
    expect(evaluate(conjunction("OR", TRUE, failing))).toBe(true);
  });

  it("NOT NULL is NULL, and NOT of (unknown AND x) is not 'everything'", () => {
    expect(evaluate(operator("NOT", [NULL], "BOOLEAN"))).toBeNull();
    const greater = comparison(colRef(0, 0), constant(2), "GREATER");
    const unknown = operator(
      "NOT",
      [conjunction("AND", greater, NULL)],
      "BOOLEAN",
    );
    expect(evaluate(unknown)).toBeNull();
    expect(evaluate(unknown, [1, "", null, true])).toBe(true);
  });

  it("refuses conditions that are not BOOLEAN", () => {
    expect(() => evaluate(operator("NOT", [constant(1)], "BOOLEAN"))).toThrow(
      DatatypeMismatchError,
    );
    expect(() => evaluate(conjunction("AND", TRUE, constant("x")))).toThrow(
      DatatypeMismatchError,
    );
  });

  it("BETWEEN is x >= a AND x <= b", () => {
    expect(evaluate(between(colRef(0, 0), constant(1), constant(10)))).toBe(
      true,
    );
    // 10 <= 5 is false, so the AND is false despite the NULL bound.
    expect(evaluate(between(colRef(0, 0), NULL, constant(5)))).toBe(false);
    expect(evaluate(between(colRef(0, 0), NULL, constant(50)))).toBeNull();
    expect(evaluate(between(NULL, constant(1), constant(2)))).toBeNull();
  });

  it("evaluates the BETWEEN operand once", () => {
    const operand = counting("M");
    evaluate(between(operand.expression, constant("A"), constant("Z")));
    expect(operand.calls()).toBe(1);
  });

  it("IN is true, NULL or false as values/logic says", () => {
    const list = (...items: BoundExpression[]) =>
      operator("IN", [colRef(0, 0), ...items], "BOOLEAN");
    expect(evaluate(list(constant(1), constant(10)))).toBe(true);
    expect(evaluate(list(constant(1), NULL))).toBeNull();
    expect(evaluate(list(constant(1), constant(2)))).toBe(false);
    expect(
      evaluate(
        operator("NOT_IN", [colRef(0, 0), constant(1), NULL], "BOOLEAN"),
      ),
    ).toBeNull();
    expect(
      evaluate(operator("NOT_IN", [colRef(0, 0), constant(1)], "BOOLEAN")),
    ).toBe(true);
  });

  it("IS NULL and IS NOT NULL never return NULL", () => {
    expect(evaluate(operator("IS_NULL", [colRef(0, 2)], "BOOLEAN"))).toBe(true);
    expect(evaluate(operator("IS_NOT_NULL", [colRef(0, 2)], "BOOLEAN"))).toBe(
      false,
    );
  });
});

describe("arithmetic", () => {
  it("divides INTEGERs toward zero and REALs exactly", () => {
    expect(evaluate(operator("DIVIDE", [integer(7), integer(2)]))).toBe(3);
    expect(evaluate(operator("DIVIDE", [integer(-7), integer(2)]))).toBe(-3);
    expect(evaluate(operator("DIVIDE", [real(7), integer(2)], "REAL"))).toBe(
      3.5,
    );
  });

  it("decides by the values when an operand's type is not known", () => {
    const divide = operator("DIVIDE", [parameter(0), integer(2)], "ANY");
    expect(evaluate(divide, row, withParameters([7]))).toBe(3);
    expect(evaluate(divide, row, withParameters([7.5]))).toBe(3.75);
  });

  it("takes the sign of the dividend for %, and is NULL dividing by 0", () => {
    expect(evaluate(operator("MOD", [integer(-7), integer(3)]))).toBe(-1);
    expect(evaluate(operator("DIVIDE", [integer(1), integer(0)]))).toBeNull();
    expect(evaluate(operator("MOD", [integer(1), integer(0)]))).toBeNull();
  });

  it("fails when an INTEGER result is not a safe integer", () => {
    const max = integer(Number.MAX_SAFE_INTEGER);
    expect(() => evaluate(operator("ADD", [max, integer(1)]))).toThrow(
      IntegerOverflowError,
    );
    expect(evaluate(operator("ADD", [max, real(1)], "REAL"))).toBe(
      Number.MAX_SAFE_INTEGER + 1,
    );
  });

  it("is NULL with a NULL operand, and refuses values that are not numbers", () => {
    expect(evaluate(operator("ADD", [colRef(0, 2), integer(1)]))).toBeNull();
    const text = operator("ADD", [parameter(0), integer(1)], "ANY");
    expect(() => evaluate(text, row, withParameters(["x"]))).toThrow(
      DatatypeMismatchError,
    );
  });

  it("concatenates text, printing numbers and booleans", () => {
    expect(concat(constant("a"), constant(1.5))).toBe("a1.5");
    expect(concat(constant("a"), TRUE)).toBe("atrue");
    expect(concat(constant("a"), NULL)).toBeNull();
  });
});

describe("CASE", () => {
  it("picks the first WHEN that holds, else ELSE, else NULL", () => {
    const checks = [
      { when: FALSE, result: constant(1) },
      { when: TRUE, result: constant(2) },
    ];
    expect(evaluate(caseExpr(checks, constant(3)))).toBe(2);
    expect(
      evaluate(caseExpr([{ when: NULL, result: constant(1) }])),
    ).toBeNull();
  });

  it("evaluates the operand once and compares it with each WHEN value", () => {
    const operand = counting(2);
    const simple = {
      ...caseExpr(
        [
          { when: constant(1), result: constant("one") },
          { when: constant(2), result: constant("two") },
          { when: constant(3), result: constant("three") },
        ],
        constant("other"),
        "TEXT",
      ),
      operand: operand.expression,
    } as BoundExpression;
    expect(evaluate(simple)).toBe("two");
    expect(operand.calls()).toBe(1);
  });

  it("matches nothing with a NULL operand", () => {
    const simple = {
      ...caseExpr([{ when: NULL, result: constant(1) }], constant(2)),
      operand: NULL,
    } as BoundExpression;
    expect(evaluate(simple)).toBe(2);
  });
});

describe("functions", () => {
  it("call the registry's implementation", () => {
    expect(evaluate(fnCall("upper", [colRef(0, 1)]))).toBe("HELLO");
    expect(
      evaluate(fnCall("substr", [colRef(0, 1), constant(0), constant(2)])),
    ).toBe("h");
    expect(evaluate(fnCall("round", [constant(-2.5, "REAL")], "REAL"))).toBe(
      -3,
    );
  });

  it("evaluate lazy arguments only as needed", () => {
    const failing = cast(constant("x"), "INTEGER");
    expect(
      evaluate(fnCall("coalesce", [constant(1), failing], "INTEGER")),
    ).toBe(1);
    expect(
      evaluate(fnCall("iif", [TRUE, constant(1), failing], "INTEGER")),
    ).toBe(1);
  });

  it("tell the implementation which arguments are constant", () => {
    const like = fnCall(
      "like",
      [constant("h%"), colRef(0, 1)],
      "BOOLEAN",
    ) as BoundFunctionExpression;
    const implement = vi.spyOn(like.function, "implement");
    expect(evaluate(like)).toBe(true);
    expect(implement).toHaveBeenCalledWith({
      argumentTypes: ["TEXT", "INTEGER"],
      constants: ["h%", undefined],
    });
    implement.mockRestore();
  });
});

describe("casts and JSON access", () => {
  it("cast as values/convert defines", () => {
    expect(evaluate(cast(constant("42"), "INTEGER"))).toBe(42);
    expect(evaluate(cast(constant(3.9, "REAL"), "INTEGER"))).toBe(3);
    expect(evaluate(cast(TRUE, "TEXT"))).toBe("true");
    expect(evaluate(cast(blob(0xca, 0xfe), "TEXT"))).toBe("cafe");
    expect(() => evaluate(cast(constant("12abc"), "INTEGER"))).toThrow(
      DatatypeMismatchError,
    );
  });

  it("follow a JSON path, NULL where there is nothing", () => {
    const data = [{ name: "Alice", items: [10, 20], nested: { x: null } }];
    const dataResolver = buildResolver(layout([0, 0]));
    const access = (path: Parameters<typeof jsonAccess>[1]) =>
      compileExpression(
        jsonAccess(colRef(0, 0, "data", "JSON"), path),
        dataResolver,
        noopCtx,
      )(data);
    expect(access([{ type: "field", name: "name" }])).toBe("Alice");
    expect(
      access([
        { type: "field", name: "items" },
        { type: "index", value: 1 },
      ]),
    ).toBe(20);
    expect(access([{ type: "field", name: "missing" }])).toBeNull();
    expect(
      access([
        { type: "field", name: "nested" },
        { type: "field", name: "x" },
      ]),
    ).toBeNull();
    expect(
      access([
        { type: "field", name: "name" },
        { type: "field", name: "length" },
      ]),
    ).toBeNull();
  });
});

const plan = { types: ["INTEGER"] } as unknown as LogicalOperator;

function subquery(
  subqueryType: BoundSubqueryExpression["subqueryType"],
  rows: Tuple[],
  extra: Partial<BoundSubqueryExpression> = {},
): { value: Value; limits: (number | undefined)[] } {
  const limits: (number | undefined)[] = [];
  const context: ExecutionContext = {
    ...noopCtx,
    runSubquery: (_plan, outer, limit) => {
      expect(outer.tuple).toBe(row);
      limits.push(limit);
      return limit === undefined ? rows : rows.slice(0, limit);
    },
  };
  const expression: BoundSubqueryExpression = {
    expressionClass: BoundExpressionClass.BOUND_SUBQUERY,
    subqueryType,
    subplan: plan,
    returnType: "BOOLEAN",
    ...extra,
  };
  return { value: evaluate(expression, row, context), limits };
}

function anyOf(child: BoundExpression, rows: Tuple[]): Value {
  return subquery("ANY", rows, { child, comparisonType: "EQUAL" }).value;
}

function allOf(rows: Tuple[]): Value {
  return subquery("ALL", rows, {
    child: colRef(0, 0),
    comparisonType: "GREATER",
  }).value;
}

describe("subqueries", () => {
  it("EXISTS and NOT EXISTS stop at the first row", () => {
    expect(subquery("EXISTS", [[1], [2]])).toEqual({
      value: true,
      limits: [1],
    });
    expect(subquery("NOT_EXISTS", [])).toEqual({ value: true, limits: [1] });
  });

  it("a scalar subquery is its one value, or NULL without rows", () => {
    expect(subquery("SCALAR", [[42]]).value).toBe(42);
    expect(subquery("SCALAR", []).value).toBeNull();
    expect(() => subquery("SCALAR", [[1], [2]])).toThrow(/more than one row/);
  });

  it("x IN (subquery) is false over no rows, even for a NULL x", () => {
    expect(anyOf(NULL, [])).toBe(false);
    expect(anyOf(NULL, [[1]])).toBeNull();
    expect(anyOf(colRef(0, 0), [[1], [10]])).toBe(true);
    expect(anyOf(colRef(0, 0), [[1], [null]])).toBeNull();
    expect(anyOf(colRef(0, 0), [[1], [2]])).toBe(false);
  });

  it("x op ALL (subquery) is true over no rows", () => {
    expect(allOf([])).toBe(true);
    expect(allOf([[1], [2]])).toBe(true);
    expect(allOf([[1], [null]])).toBeNull();
    expect(allOf([[null], [20]])).toBe(false);
  });
});

describe("predicates and table filters", () => {
  it("pass a row only when the condition is TRUE", () => {
    const predicate = compilePredicate(
      comparison(colRef(0, 0), constant(5), "GREATER"),
      resolver,
      noopCtx,
    );
    expect(predicate(row)).toBe(true);
    expect(predicate([1, "", null, true])).toBe(false);
    expect(predicate([null, "", null, true])).toBe(false);
  });

  it("compare a column with a constant or a parameter", () => {
    const filter: TableFilter = {
      expression: colRef(0, 3, "flag", "BOOLEAN"),
      comparisonType: "EQUAL",
      constant: {
        ...constant(false),
        returnType: "BOOLEAN",
      } as TableFilter["constant"],
    };
    const isFalse = compileTableFilter(filter, resolver, noopCtx);
    expect(isFalse([1, "", null, false])).toBe(true);
    expect(isFalse([1, "", null, true])).toBe(false);
    const byParameter = compileTableFilter(
      { ...filter, constant: parameter(0) },
      resolver,
      withParameters([true]),
    );
    expect(byParameter(row)).toBe(true);
  });
});

describe("evaluateConstant", () => {
  it("folds with the same semantics as the compiled query", () => {
    expect(evaluateConstant(conjunction("AND", NULL, TRUE))).toBeNull();
    expect(evaluateConstant(comparison(blob(1), blob(1)))).toBe(true);
    expect(
      evaluateConstant(operator("DIVIDE", [constant(7), constant(2)])),
    ).toBe(3);
  });

  it("leaves expressions that fail to run time", () => {
    expect(evaluateConstant(cast(constant("x"), "INTEGER"))).toBeUndefined();
  });
});
