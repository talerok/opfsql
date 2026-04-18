import { beforeEach, describe, expect, it } from "vitest";
import { BindError } from "../core/errors.js";
import { sameExpression } from "../expression/same-expression.js";
import type {
  BoundBetweenExpression,
  BoundCaseExpression,
  BoundCastExpression,
  BoundColumnRefExpression,
  BoundComparisonExpression,
  BoundConjunctionExpression,
  BoundConstantExpression,
  BoundFunctionExpression,
  BoundJsonAccessExpression,
  BoundOperatorExpression,
  LogicalFilter,
  LogicalProjection,
} from "../types.js";
import { BoundExpressionClass } from "../types.js";
import { createTestContext } from "./test_helpers.js";

let catalog: ReturnType<typeof createTestContext>["catalog"];
let bind: ReturnType<typeof createTestContext>["bind"];

beforeEach(() => {
  const ctx = createTestContext();
  catalog = ctx.catalog;
  bind = ctx.bind;
});

describe("Expressions", () => {
  it("LIKE produces BoundFunctionExpression", () => {
    const plan = bind("SELECT * FROM users WHERE name LIKE '%alice%'");
    const proj = plan as LogicalProjection;
    const filter = proj.children[0] as LogicalFilter;
    const expr = filter.expressions[0] as BoundFunctionExpression;
    expect(expr.expressionClass).toBe(BoundExpressionClass.BOUND_FUNCTION);
    expect(expr.functionName).toBe("LIKE");
    expect(expr.returnType).toBe("BOOLEAN");
  });

  it("NOT LIKE produces BoundFunctionExpression", () => {
    const plan = bind("SELECT * FROM users WHERE name NOT LIKE '%bob%'");
    const proj = plan as LogicalProjection;
    const filter = proj.children[0] as LogicalFilter;
    const expr = filter.expressions[0] as BoundFunctionExpression;
    expect(expr.functionName).toBe("NOT_LIKE");
  });

  it("IS NULL produces BoundOperatorExpression", () => {
    const plan = bind("SELECT * FROM users WHERE age IS NULL");
    const proj = plan as LogicalProjection;
    const filter = proj.children[0] as LogicalFilter;
    const expr = filter.expressions[0] as BoundOperatorExpression;
    expect(expr.expressionClass).toBe(BoundExpressionClass.BOUND_OPERATOR);
    expect(expr.operatorType).toBe("IS_NULL");
    expect(expr.returnType).toBe("BOOLEAN");
  });

  it("IS NOT NULL produces BoundOperatorExpression", () => {
    const plan = bind("SELECT * FROM users WHERE age IS NOT NULL");
    const proj = plan as LogicalProjection;
    const filter = proj.children[0] as LogicalFilter;
    const expr = filter.expressions[0] as BoundOperatorExpression;
    expect(expr.operatorType).toBe("IS_NOT_NULL");
  });

  it("BETWEEN produces BoundBetweenExpression", () => {
    const plan = bind("SELECT * FROM users WHERE age BETWEEN 18 AND 65");
    const proj = plan as LogicalProjection;
    const filter = proj.children[0] as LogicalFilter;
    const expr = filter.expressions[0] as BoundBetweenExpression;
    expect(expr.expressionClass).toBe(BoundExpressionClass.BOUND_BETWEEN);
    expect(expr.returnType).toBe("BOOLEAN");
  });

  it("arithmetic operators return correct types", () => {
    const plan = bind("SELECT age + 1, age * 2 FROM users");
    const proj = plan as LogicalProjection;
    expect(proj.expressions).toHaveLength(2);
    const add = proj.expressions[0] as BoundOperatorExpression;
    expect(add.operatorType).toBe("ADD");
    expect(add.returnType).toBe("INTEGER");
    const mul = proj.expressions[1] as BoundOperatorExpression;
    expect(mul.operatorType).toBe("MULTIPLY");
    expect(mul.returnType).toBe("INTEGER");
  });

  it("|| operator returns TEXT", () => {
    const plan = bind("SELECT name || ' suffix' FROM users");
    const proj = plan as LogicalProjection;
    const concat = proj.expressions[0] as BoundOperatorExpression;
    expect(concat.operatorType).toBe("CONCAT");
    expect(concat.returnType).toBe("TEXT");
  });

  it("arithmetic with REAL promotes to REAL", () => {
    const plan = bind("SELECT amount + 1 FROM orders");
    const proj = plan as LogicalProjection;
    const add = proj.expressions[0] as BoundOperatorExpression;
    expect(add.returnType).toBe("REAL");
  });

  it("CASE expression binds correctly", () => {
    const plan = bind(
      "SELECT CASE WHEN age > 18 THEN 'adult' ELSE 'minor' END FROM users",
    );
    const proj = plan as LogicalProjection;
    const caseExpr = proj.expressions[0] as BoundCaseExpression;
    expect(caseExpr.expressionClass).toBe(BoundExpressionClass.BOUND_CASE);
    expect(caseExpr.caseChecks).toHaveLength(1);
    expect(caseExpr.elseExpr).not.toBeNull();
    expect(caseExpr.returnType).toBe("TEXT");
  });

  it("CAST expression binds correctly", () => {
    const plan = bind("SELECT CAST(age AS TEXT) FROM users");
    const proj = plan as LogicalProjection;
    const cast = proj.expressions[0] as BoundCastExpression;
    expect(cast.expressionClass).toBe(BoundExpressionClass.BOUND_CAST);
    expect(cast.castType).toBe("TEXT");
    expect(cast.returnType).toBe("TEXT");
  });

  it("AND/OR conjunction binds children", () => {
    const plan = bind("SELECT * FROM users WHERE age > 18 AND active = true");
    const proj = plan as LogicalProjection;
    const filter = proj.children[0] as LogicalFilter;
    const conj = filter.expressions[0] as BoundConjunctionExpression;
    expect(conj.expressionClass).toBe(BoundExpressionClass.BOUND_CONJUNCTION);
    expect(conj.conjunctionType).toBe("AND");
    expect(conj.children).toHaveLength(2);
  });

  it("IN operator produces BoundOperatorExpression", () => {
    const plan = bind("SELECT * FROM users WHERE id IN (1, 2, 3)");
    const proj = plan as LogicalProjection;
    const filter = proj.children[0] as LogicalFilter;
    const op = filter.expressions[0] as BoundOperatorExpression;
    expect(op.expressionClass).toBe(BoundExpressionClass.BOUND_OPERATOR);
    expect(op.operatorType).toBe("IN");
    expect(op.returnType).toBe("BOOLEAN");
  });

  it("scalar functions bind correctly", () => {
    const plan = bind("SELECT UPPER(name), LENGTH(name) FROM users");
    const proj = plan as LogicalProjection;
    const upper = proj.expressions[0] as BoundFunctionExpression;
    expect(upper.functionName).toBe("UPPER");
    expect(upper.returnType).toBe("TEXT");
    const len = proj.expressions[1] as BoundFunctionExpression;
    expect(len.functionName).toBe("LENGTH");
    expect(len.returnType).toBe("INTEGER");
  });

  it("COALESCE returns non-null type", () => {
    const plan = bind("SELECT COALESCE(name, age) FROM users");
    const proj = plan as LogicalProjection;
    const fn = proj.expressions[0] as BoundFunctionExpression;
    expect(fn.functionName).toBe("COALESCE");
  });
});

describe("Expressions — additional", () => {
  it("NEGATE operator returns correct type", () => {
    const plan = bind("SELECT -age FROM users");
    const proj = plan as LogicalProjection;
    const neg = proj.expressions[0] as BoundOperatorExpression;
    expect(neg.operatorType).toBe("NEGATE");
    expect(neg.returnType).toBe("INTEGER");
  });

  it("NOT operator returns BOOLEAN", () => {
    const plan = bind("SELECT * FROM users WHERE NOT active");
    const proj = plan as LogicalProjection;
    const filter = proj.children[0] as LogicalFilter;
    const not = filter.expressions[0] as BoundOperatorExpression;
    expect(not.operatorType).toBe("NOT");
    expect(not.returnType).toBe("BOOLEAN");
  });

  it("MOD operator", () => {
    const plan = bind("SELECT age % 2 FROM users");
    const proj = plan as LogicalProjection;
    const mod = proj.expressions[0] as BoundOperatorExpression;
    expect(mod.operatorType).toBe("MOD");
    expect(mod.returnType).toBe("INTEGER");
  });

  it("DIVIDE operator", () => {
    const plan = bind("SELECT age / 2 FROM users");
    const proj = plan as LogicalProjection;
    const div = proj.expressions[0] as BoundOperatorExpression;
    expect(div.operatorType).toBe("DIVIDE");
    expect(div.returnType).toBe("INTEGER");
  });

  it("SUBTRACT operator", () => {
    const plan = bind("SELECT age - 1 FROM users");
    const proj = plan as LogicalProjection;
    const sub = proj.expressions[0] as BoundOperatorExpression;
    expect(sub.operatorType).toBe("SUBTRACT");
    expect(sub.returnType).toBe("INTEGER");
  });

  it("NULL constant has returnType NULL", () => {
    const plan = bind("SELECT NULL");
    const proj = plan as LogicalProjection;
    const c = proj.expressions[0] as BoundConstantExpression;
    expect(c.value).toBeNull();
    expect(c.returnType).toBe("NULL");
  });

  it("TRUE/FALSE constants have returnType BOOLEAN", () => {
    const plan = bind("SELECT TRUE, FALSE");
    const proj = plan as LogicalProjection;
    expect(proj.types).toEqual(["BOOLEAN", "BOOLEAN"]);
    expect((proj.expressions[0] as BoundConstantExpression).value).toBe(true);
    expect((proj.expressions[1] as BoundConstantExpression).value).toBe(false);
  });

  it("ABS returns child type", () => {
    const plan = bind("SELECT ABS(age) FROM users");
    const proj = plan as LogicalProjection;
    const fn = proj.expressions[0] as BoundFunctionExpression;
    expect(fn.functionName).toBe("ABS");
    expect(fn.returnType).toBe("INTEGER");
  });

  it("TYPEOF returns TEXT", () => {
    const plan = bind("SELECT TYPEOF(name) FROM users");
    const proj = plan as LogicalProjection;
    const fn = proj.expressions[0] as BoundFunctionExpression;
    expect(fn.functionName).toBe("TYPEOF");
    expect(fn.returnType).toBe("TEXT");
  });

  it("NOT IN operator", () => {
    const plan = bind("SELECT * FROM users WHERE id NOT IN (1, 2, 3)");
    const proj = plan as LogicalProjection;
    const filter = proj.children[0] as LogicalFilter;
    const op = filter.expressions[0] as BoundOperatorExpression;
    expect(op.operatorType).toBe("NOT_IN");
    expect(op.returnType).toBe("BOOLEAN");
  });

  it("OR conjunction", () => {
    const plan = bind("SELECT * FROM users WHERE age > 18 OR active = true");
    const proj = plan as LogicalProjection;
    const filter = proj.children[0] as LogicalFilter;
    const conj = filter.expressions[0] as BoundConjunctionExpression;
    expect(conj.conjunctionType).toBe("OR");
  });
});

describe("Expression edge cases", () => {
  it("nested CASE expressions bind correctly", () => {
    const plan = bind(
      "SELECT CASE WHEN age > 18 THEN CASE WHEN age > 30 THEN 'senior' ELSE 'adult' END ELSE 'minor' END FROM users",
    );
    const proj = plan as LogicalProjection;
    expect(proj.expressions[0].expressionClass).toBe(
      BoundExpressionClass.BOUND_CASE,
    );
    const outerCase = proj.expressions[0] as BoundCaseExpression;
    expect(outerCase.caseChecks[0].then.expressionClass).toBe(
      BoundExpressionClass.BOUND_CASE,
    );
  });

  it("CAST inside CASE binds correctly", () => {
    const plan = bind(
      "SELECT CASE WHEN age > 18 THEN CAST(age AS TEXT) ELSE 'N/A' END FROM users",
    );
    const proj = plan as LogicalProjection;
    expect(proj.expressions[0].expressionClass).toBe(
      BoundExpressionClass.BOUND_CASE,
    );
  });

  it("function inside ORDER BY binds correctly", () => {
    const plan = bind("SELECT name FROM users ORDER BY UPPER(name)");
    // Should not throw
    expect(plan).toBeDefined();
  });

  it("aggregate with expression argument", () => {
    const plan = bind(
      "SELECT SUM(age + 1) FROM users",
    );
    expect(plan).toBeDefined();
    // Should not throw — aggregate wrapping an expression
  });

  it("CONCAT of multiple columns", () => {
    const plan = bind("SELECT CONCAT(name, ' ', name) FROM users");
    const proj = plan as LogicalProjection;
    expect(proj.expressions[0].expressionClass).toBe(
      BoundExpressionClass.BOUND_FUNCTION,
    );
    expect(
      (proj.expressions[0] as BoundFunctionExpression).functionName,
    ).toBe("CONCAT");
  });
});

describe("BETWEEN type checking", () => {
  it("BETWEEN with compatible types succeeds", () => {
    const plan = bind("SELECT * FROM users WHERE age BETWEEN 18 AND 65");
    const proj = plan as LogicalProjection;
    const filter = proj.children[0] as LogicalFilter;
    expect(filter.expressions[0].returnType).toBe("BOOLEAN");
  });

  it("BETWEEN with incompatible lower bound throws BindError", () => {
    expect(() =>
      bind("SELECT * FROM users WHERE age BETWEEN 'young' AND 65"),
    ).toThrow(BindError);
    expect(() =>
      bind("SELECT * FROM users WHERE age BETWEEN 'young' AND 65"),
    ).toThrow("Type mismatch");
  });

  it("BETWEEN with incompatible upper bound throws BindError", () => {
    expect(() =>
      bind("SELECT * FROM users WHERE age BETWEEN 18 AND 'old'"),
    ).toThrow(BindError);
    expect(() =>
      bind("SELECT * FROM users WHERE age BETWEEN 18 AND 'old'"),
    ).toThrow("Type mismatch");
  });
});

describe("CASE branch type checking", () => {
  it("CASE with compatible THEN branches succeeds", () => {
    const plan = bind("SELECT CASE WHEN age > 18 THEN 1 ELSE 0 END FROM users");
    const proj = plan as LogicalProjection;
    const caseExpr = proj.expressions[0] as BoundCaseExpression;
    expect(caseExpr.returnType).toBe("INTEGER");
  });

  it("CASE with incompatible THEN branches throws BindError", () => {
    expect(() =>
      bind("SELECT CASE WHEN age > 18 THEN 'adult' ELSE 42 END FROM users"),
    ).toThrow(BindError);
    expect(() =>
      bind("SELECT CASE WHEN age > 18 THEN 'adult' ELSE 42 END FROM users"),
    ).toThrow("Type mismatch");
  });

  it("CASE with multiple incompatible WHEN branches throws BindError", () => {
    expect(() =>
      bind(
        "SELECT CASE WHEN age < 13 THEN 'child' WHEN age < 18 THEN 42 ELSE 'adult' END FROM users",
      ),
    ).toThrow(BindError);
  });

  it("CASE with numeric type promotion succeeds", () => {
    const plan = bind(
      "SELECT CASE WHEN age > 18 THEN age ELSE amount END FROM users JOIN orders ON users.id = orders.user_id",
    );
    const proj = plan as LogicalProjection;
    const caseExpr = proj.expressions[0] as BoundCaseExpression;
    // INTEGER and REAL should promote to REAL
    expect(caseExpr.returnType).toBe("REAL");
  });
});

describe("CASE extra tests", () => {
  it("CASE with all TEXT branches succeeds", () => {
    const plan = bind(
      "SELECT CASE WHEN age > 18 THEN 'adult' WHEN age > 10 THEN 'teen' ELSE 'child' END FROM users",
    );
    const proj = plan as LogicalProjection;
    const caseExpr = proj.expressions[0] as BoundCaseExpression;
    expect(caseExpr.returnType).toBe("TEXT");
  });

  it("CASE with NULL ELSE is compatible with any THEN type", () => {
    const plan = bind(
      "SELECT CASE WHEN age > 18 THEN 'adult' ELSE NULL END FROM users",
    );
    const proj = plan as LogicalProjection;
    const caseExpr = proj.expressions[0] as BoundCaseExpression;
    expect(caseExpr.returnType).toBe("TEXT");
  });
});

describe("CASE — additional", () => {
  it("CASE with multiple WHEN branches", () => {
    const plan = bind(
      "SELECT CASE WHEN age < 13 THEN 'child' WHEN age < 18 THEN 'teen' ELSE 'adult' END FROM users",
    );
    const proj = plan as LogicalProjection;
    const caseExpr = proj.expressions[0] as BoundCaseExpression;
    expect(caseExpr.caseChecks).toHaveLength(2);
    expect(caseExpr.elseExpr).not.toBeNull();
  });

  it("CASE without ELSE", () => {
    const plan = bind("SELECT CASE WHEN age > 18 THEN 'adult' END FROM users");
    const proj = plan as LogicalProjection;
    const caseExpr = proj.expressions[0] as BoundCaseExpression;
    expect(caseExpr.elseExpr).toBeNull();
  });
});

describe("sameExpression — BOUND_JSON_ACCESS", () => {
  it("identical JSON access expressions are equal", () => {
    const a: BoundJsonAccessExpression = {
      expressionClass: BoundExpressionClass.BOUND_JSON_ACCESS,
      child: {
        expressionClass: BoundExpressionClass.BOUND_COLUMN_REF,
        binding: { tableIndex: 0, columnIndex: 1 },
        tableName: "t",
        columnName: "data",
        returnType: "JSON",
      },
      path: [{ type: "field", name: "name" }],
      returnType: "JSON",
    };
    const b: BoundJsonAccessExpression = {
      expressionClass: BoundExpressionClass.BOUND_JSON_ACCESS,
      child: {
        expressionClass: BoundExpressionClass.BOUND_COLUMN_REF,
        binding: { tableIndex: 0, columnIndex: 1 },
        tableName: "t",
        columnName: "data",
        returnType: "JSON",
      },
      path: [{ type: "field", name: "name" }],
      returnType: "JSON",
    };
    expect(sameExpression(a, b)).toBe(true);
  });

  it("different paths are not equal", () => {
    const a: BoundJsonAccessExpression = {
      expressionClass: BoundExpressionClass.BOUND_JSON_ACCESS,
      child: {
        expressionClass: BoundExpressionClass.BOUND_COLUMN_REF,
        binding: { tableIndex: 0, columnIndex: 1 },
        tableName: "t",
        columnName: "data",
        returnType: "JSON",
      },
      path: [{ type: "field", name: "name" }],
      returnType: "JSON",
    };
    const b: BoundJsonAccessExpression = {
      expressionClass: BoundExpressionClass.BOUND_JSON_ACCESS,
      child: {
        expressionClass: BoundExpressionClass.BOUND_COLUMN_REF,
        binding: { tableIndex: 0, columnIndex: 1 },
        tableName: "t",
        columnName: "data",
        returnType: "JSON",
      },
      path: [{ type: "field", name: "age" }],
      returnType: "JSON",
    };
    expect(sameExpression(a, b)).toBe(false);
  });

  it("different path lengths are not equal", () => {
    const a: BoundJsonAccessExpression = {
      expressionClass: BoundExpressionClass.BOUND_JSON_ACCESS,
      child: {
        expressionClass: BoundExpressionClass.BOUND_COLUMN_REF,
        binding: { tableIndex: 0, columnIndex: 1 },
        tableName: "t",
        columnName: "data",
        returnType: "JSON",
      },
      path: [{ type: "field", name: "a" }, { type: "field", name: "b" }],
      returnType: "JSON",
    };
    const b: BoundJsonAccessExpression = {
      expressionClass: BoundExpressionClass.BOUND_JSON_ACCESS,
      child: {
        expressionClass: BoundExpressionClass.BOUND_COLUMN_REF,
        binding: { tableIndex: 0, columnIndex: 1 },
        tableName: "t",
        columnName: "data",
        returnType: "JSON",
      },
      path: [{ type: "field", name: "a" }],
      returnType: "JSON",
    };
    expect(sameExpression(a, b)).toBe(false);
  });

  it("field vs index path segment are not equal", () => {
    const a: BoundJsonAccessExpression = {
      expressionClass: BoundExpressionClass.BOUND_JSON_ACCESS,
      child: {
        expressionClass: BoundExpressionClass.BOUND_COLUMN_REF,
        binding: { tableIndex: 0, columnIndex: 1 },
        tableName: "t",
        columnName: "data",
        returnType: "JSON",
      },
      path: [{ type: "field", name: "items" }],
      returnType: "JSON",
    };
    const b: BoundJsonAccessExpression = {
      expressionClass: BoundExpressionClass.BOUND_JSON_ACCESS,
      child: {
        expressionClass: BoundExpressionClass.BOUND_COLUMN_REF,
        binding: { tableIndex: 0, columnIndex: 1 },
        tableName: "t",
        columnName: "data",
        returnType: "JSON",
      },
      path: [{ type: "index", value: 0 }],
      returnType: "JSON",
    };
    expect(sameExpression(a, b)).toBe(false);
  });

  it("different column bindings are not equal", () => {
    const a: BoundJsonAccessExpression = {
      expressionClass: BoundExpressionClass.BOUND_JSON_ACCESS,
      child: {
        expressionClass: BoundExpressionClass.BOUND_COLUMN_REF,
        binding: { tableIndex: 0, columnIndex: 1 },
        tableName: "t",
        columnName: "data",
        returnType: "JSON",
      },
      path: [{ type: "field", name: "x" }],
      returnType: "JSON",
    };
    const b: BoundJsonAccessExpression = {
      expressionClass: BoundExpressionClass.BOUND_JSON_ACCESS,
      child: {
        expressionClass: BoundExpressionClass.BOUND_COLUMN_REF,
        binding: { tableIndex: 1, columnIndex: 1 },
        tableName: "t2",
        columnName: "data",
        returnType: "JSON",
      },
      path: [{ type: "field", name: "x" }],
      returnType: "JSON",
    };
    expect(sameExpression(a, b)).toBe(false);
  });

  it("index path segments with same value are equal", () => {
    const a: BoundJsonAccessExpression = {
      expressionClass: BoundExpressionClass.BOUND_JSON_ACCESS,
      child: {
        expressionClass: BoundExpressionClass.BOUND_COLUMN_REF,
        binding: { tableIndex: 0, columnIndex: 1 },
        tableName: "t",
        columnName: "data",
        returnType: "JSON",
      },
      path: [{ type: "field", name: "items" }, { type: "index", value: 2 }],
      returnType: "JSON",
    };
    const b: BoundJsonAccessExpression = {
      expressionClass: BoundExpressionClass.BOUND_JSON_ACCESS,
      child: {
        expressionClass: BoundExpressionClass.BOUND_COLUMN_REF,
        binding: { tableIndex: 0, columnIndex: 1 },
        tableName: "t",
        columnName: "data",
        returnType: "JSON",
      },
      path: [{ type: "field", name: "items" }, { type: "index", value: 2 }],
      returnType: "JSON",
    };
    expect(sameExpression(a, b)).toBe(true);
  });
});

describe("sameExpression — additional expression types", () => {
  const colA: BoundColumnRefExpression = {
    expressionClass: BoundExpressionClass.BOUND_COLUMN_REF,
    binding: { tableIndex: 0, columnIndex: 0 },
    tableName: "t",
    columnName: "id",
    returnType: "INTEGER",
  };
  const colB: BoundColumnRefExpression = {
    expressionClass: BoundExpressionClass.BOUND_COLUMN_REF,
    binding: { tableIndex: 0, columnIndex: 1 },
    tableName: "t",
    columnName: "name",
    returnType: "TEXT",
  };
  const constOne: BoundConstantExpression = {
    expressionClass: BoundExpressionClass.BOUND_CONSTANT,
    value: 1,
    returnType: "INTEGER",
  };

  it("BOUND_COMPARISON: same comparisons are equal", () => {
    const a: BoundComparisonExpression = {
      expressionClass: BoundExpressionClass.BOUND_COMPARISON,
      comparisonType: "EQUAL",
      left: colA,
      right: constOne,
      returnType: "BOOLEAN",
    };
    const b: BoundComparisonExpression = { ...a };
    expect(sameExpression(a, b)).toBe(true);
  });

  it("BOUND_COMPARISON: different comparison types are not equal", () => {
    const a: BoundComparisonExpression = {
      expressionClass: BoundExpressionClass.BOUND_COMPARISON,
      comparisonType: "EQUAL",
      left: colA,
      right: constOne,
      returnType: "BOOLEAN",
    };
    const b: BoundComparisonExpression = { ...a, comparisonType: "LESS" };
    expect(sameExpression(a, b)).toBe(false);
  });

  it("BOUND_CONJUNCTION: same conjunctions are equal", () => {
    const a: BoundConjunctionExpression = {
      expressionClass: BoundExpressionClass.BOUND_CONJUNCTION,
      conjunctionType: "AND",
      children: [colA, colB],
      returnType: "BOOLEAN",
    };
    const b: BoundConjunctionExpression = { ...a, children: [colA, colB] };
    expect(sameExpression(a, b)).toBe(true);
  });

  it("BOUND_CONJUNCTION: different types are not equal", () => {
    const a: BoundConjunctionExpression = {
      expressionClass: BoundExpressionClass.BOUND_CONJUNCTION,
      conjunctionType: "AND",
      children: [colA],
      returnType: "BOOLEAN",
    };
    const b: BoundConjunctionExpression = { ...a, conjunctionType: "OR" };
    expect(sameExpression(a, b)).toBe(false);
  });

  it("BOUND_OPERATOR: same operators are equal", () => {
    const a: BoundOperatorExpression = {
      expressionClass: BoundExpressionClass.BOUND_OPERATOR,
      operatorType: "ADD",
      children: [colA, constOne],
      returnType: "INTEGER",
    };
    const b: BoundOperatorExpression = { ...a, children: [colA, constOne] };
    expect(sameExpression(a, b)).toBe(true);
  });

  it("BOUND_OPERATOR: different operator types are not equal", () => {
    const a: BoundOperatorExpression = {
      expressionClass: BoundExpressionClass.BOUND_OPERATOR,
      operatorType: "ADD",
      children: [colA, constOne],
      returnType: "INTEGER",
    };
    const b: BoundOperatorExpression = { ...a, operatorType: "SUBTRACT" };
    expect(sameExpression(a, b)).toBe(false);
  });

  it("BOUND_FUNCTION: same functions are equal", () => {
    const a: BoundFunctionExpression = {
      expressionClass: BoundExpressionClass.BOUND_FUNCTION,
      functionName: "UPPER",
      children: [colB],
      returnType: "TEXT",
    };
    const b: BoundFunctionExpression = { ...a, children: [colB] };
    expect(sameExpression(a, b)).toBe(true);
  });

  it("BOUND_FUNCTION: different names are not equal", () => {
    const a: BoundFunctionExpression = {
      expressionClass: BoundExpressionClass.BOUND_FUNCTION,
      functionName: "UPPER",
      children: [colB],
      returnType: "TEXT",
    };
    const b: BoundFunctionExpression = { ...a, functionName: "LOWER" };
    expect(sameExpression(a, b)).toBe(false);
  });

  it("BOUND_BETWEEN: same between expressions are equal", () => {
    const a: BoundBetweenExpression = {
      expressionClass: BoundExpressionClass.BOUND_BETWEEN,
      input: colA,
      lower: constOne,
      upper: { ...constOne, value: 10 },
      returnType: "BOOLEAN",
    };
    const b: BoundBetweenExpression = {
      ...a,
      input: colA,
      lower: constOne,
      upper: { ...constOne, value: 10 },
    };
    expect(sameExpression(a, b)).toBe(true);
  });

  it("BOUND_BETWEEN: different bounds are not equal", () => {
    const a: BoundBetweenExpression = {
      expressionClass: BoundExpressionClass.BOUND_BETWEEN,
      input: colA,
      lower: constOne,
      upper: { ...constOne, value: 10 },
      returnType: "BOOLEAN",
    };
    const b: BoundBetweenExpression = {
      ...a,
      upper: { ...constOne, value: 99 },
    };
    expect(sameExpression(a, b)).toBe(false);
  });

  it("BOUND_CAST: same casts are equal", () => {
    const a: BoundCastExpression = {
      expressionClass: BoundExpressionClass.BOUND_CAST,
      child: colA,
      castType: "TEXT",
      returnType: "TEXT",
    };
    const b: BoundCastExpression = { ...a };
    expect(sameExpression(a, b)).toBe(true);
  });

  it("BOUND_CAST: different cast types are not equal", () => {
    const a: BoundCastExpression = {
      expressionClass: BoundExpressionClass.BOUND_CAST,
      child: colA,
      castType: "TEXT",
      returnType: "TEXT",
    };
    const b: BoundCastExpression = {
      ...a,
      castType: "REAL",
      returnType: "REAL",
    };
    expect(sameExpression(a, b)).toBe(false);
  });

  it("default: unhandled expression class returns false", () => {
    const a = {
      expressionClass: BoundExpressionClass.BOUND_PARAMETER as const,
      paramIndex: 0,
      returnType: "ANY" as const,
    };
    const b = { ...a };
    expect(sameExpression(a, b)).toBe(false);
  });

  it("different expression classes return false", () => {
    expect(sameExpression(colA, constOne)).toBe(false);
  });
});
