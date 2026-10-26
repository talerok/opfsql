import { describe, expect, it, vi } from "vitest";
import {
  DatatypeMismatchError,
  IntegerOverflowError,
  InternalError,
  MisuseError,
} from "../../errors.js";
import type { ScalarFunction } from "../../functions/index.js";
import { scalarFunction } from "../../functions/index.js";
import type { Value } from "../../values/index.js";
import { compileExpression } from "../index.js";
import {
  and,
  arithmetic,
  between,
  blob,
  call,
  callOf,
  caseWhen,
  cast,
  column,
  compare,
  concat,
  constant,
  FALSE,
  inList,
  json,
  jsonAccess,
  negate,
  not,
  notInList,
  notNullCheck,
  NULL,
  nullCheck,
  or,
  parameter,
  simpleCase,
  TRUE,
} from "./builders.js";
import {
  c0,
  c1,
  c2,
  c3,
  evaluate,
  evaluateEach,
  holds,
  ROW,
  rowWith,
  scopeOf,
} from "./harness.js";

/** A call that returns `value` and counts how often it runs. */
function counting(value: Value): {
  expression: ReturnType<typeof callOf>;
  calls: () => number;
} {
  let calls = 0;
  const definition: ScalarFunction = {
    kind: "scalar",
    name: "counting",
    signature: { required: [] },
    returnType: () => "ANY",
    implement: () => ({
      kind: "eager",
      call: () => {
        calls++;
        return value;
      },
    }),
  };
  return { expression: callOf(definition, []), calls: () => calls };
}

/** `CAST('x' AS INTEGER)`: fails whenever it is evaluated. */
const failing = cast(constant("x"), "INTEGER");

describe("columns, constants and parameters", () => {
  it("reads a column at its position in the layout, not its column index", () => {
    const layout = [c1.binding, c0.binding];
    expect(evaluate(c0, { layout, row: ["a", 1] })).toBe(1);
  });

  it("resolves columns once and reads each row", () => {
    expect(evaluateEach(c0, [[1], [2], [3]])).toEqual([1, 2, 3]);
  });

  it("fails at compile time for a column that is not in the layout", () => {
    expect(() => compileExpression(column(5, 0), scopeOf())).toThrow(
      InternalError,
    );
  });

  it("fails at compile time for a column deeper than the enclosing queries", () => {
    expect(() =>
      compileExpression(column(0, 0, "INTEGER", 1), scopeOf()),
    ).toThrow(InternalError);
  });

  it("fails for a row narrower than its layout, rather than read NULL", () => {
    expect(() => evaluate(c3, { row: [10, "hello"] })).toThrow(
      new InternalError(
        "column t0.c3 (0.3) is past the end of a row of 2 values",
      ),
    );
    expect(evaluate(c2, { row: [10, "hello", null] })).toBeNull();
  });

  it("gives a fresh copy of a JSON constant each time, so no caller changes it", () => {
    const compiled = compileExpression(json({ a: [1] }), scopeOf());
    const first = compiled([]);
    expect(first).toEqual({ a: [1] });
    (first as { a: number[] }).a.push(2);
    expect(compiled([])).toEqual({ a: [1] });
  });

  it("reads parameters, normalized: NaN is NULL and -0 is 0", () => {
    const parameters = ["a", NaN, -0];
    expect(evaluate(parameter(0), { parameters })).toBe("a");
    expect(evaluate(parameter(1), { parameters })).toBeNull();
    expect(Object.is(evaluate(parameter(2), { parameters }), 0)).toBe(true);
  });

  it("fails with MISUSE for a parameter the run was not given", () => {
    expect(() => evaluate(parameter(0))).toThrow(MisuseError);
  });
});

describe("comparisons", () => {
  it("are NULL when either side is NULL", () => {
    expect(evaluate(compare(c2, "=", constant(1)))).toBeNull();
    expect(evaluate(compare(NULL, "=", NULL))).toBeNull();
  });

  it("use the total order: no numeric text, booleans apart from numbers", () => {
    expect(evaluate(compare(constant(9), "<", constant("10")))).toBe(true);
    expect(evaluate(compare(constant("10"), "<", constant("9")))).toBe(true);
    expect(evaluate(compare(constant("9"), "=", constant(9)))).toBe(false);
    expect(evaluate(compare(TRUE, "=", constant(1)))).toBe(false);
  });

  it("compare BLOBs by their bytes and JSON by canonical text", () => {
    expect(evaluate(compare(blob(1), "<", blob(1, 0)))).toBe(true);
    const reordered = compare(json({ b: 1, a: 2 }), "=", json({ a: 2, b: 1 }));
    expect(evaluate(reordered)).toBe(true);
  });

  it("support every operator", () => {
    const results = (["=", "<>", "<", "<=", ">", ">="] as const).map((symbol) =>
      evaluate(compare(c0, symbol, constant(10))),
    );
    expect(results).toEqual([true, false, false, true, false, true]);
  });
});

describe("parameter affinity", () => {
  const numeric = parameter(0, "numeric");

  it("turns numeric text into a number for a numeric side", () => {
    expect(evaluate(compare(c0, "=", numeric), { parameters: ["10"] })).toBe(
      true,
    );
  });

  it("turns a number into text for a text side", () => {
    const row = rowWith(c1, "5");
    const text = parameter(0, "text");
    expect(evaluate(compare(c1, "=", text), { parameters: [5], row })).toBe(
      true,
    );
  });

  it("leaves text that is not exactly a number alone", () => {
    expect(evaluate(compare(c0, "<", numeric), { parameters: ["ten"] })).toBe(
      true,
    );
  });

  it("is the only conversion: everything else compares as it is", () => {
    const untyped = parameter(0);
    expect(evaluate(compare(c0, "=", untyped), { parameters: ["10"] })).toBe(
      false,
    );
    const anyText = rowWith(c2, "10");
    expect(evaluate(compare(c2, "=", constant(10)), { row: anyText })).toBe(
      false,
    );
    expect(evaluate(compare(json("10"), "=", constant(10)))).toBe(false);
  });

  it("converts IN needles and items, hashed or not", () => {
    const parameters = ["10"];
    expect(evaluate(inList(c0, numeric), { parameters })).toBe(true);
    const tens = inList(numeric, constant(10), constant(20));
    expect(evaluate(tens, { parameters })).toBe(true);
    expect(evaluate(inList(c0, c2, numeric), { parameters })).toBe(true);
  });

  it("converts BETWEEN bounds and CASE values", () => {
    const parameters = ["5", "20", "10"];
    const bounds = between(c0, numeric, parameter(1, "numeric"));
    expect(evaluate(bounds, { parameters })).toBe(true);
    const ten = simpleCase(c0, [[parameter(2, "numeric"), constant("ten")]]);
    expect(evaluate(ten, { parameters })).toBe("ten");
  });
});

describe("three-valued logic", () => {
  it("AND and OR follow the truth tables", () => {
    expect(evaluate(and(NULL, TRUE))).toBeNull();
    expect(evaluate(and(NULL, FALSE))).toBe(false);
    expect(evaluate(or(NULL, TRUE))).toBe(true);
    expect(evaluate(or(NULL, FALSE))).toBeNull();
  });

  it("AND stops at FALSE and OR at TRUE", () => {
    expect(evaluate(and(FALSE, failing))).toBe(false);
    expect(evaluate(or(TRUE, failing))).toBe(true);
  });

  it("NOT NULL is NULL, and NOT (unknown AND x) is not 'everything'", () => {
    expect(evaluate(not(NULL))).toBeNull();
    const unknown = not(and(compare(c0, ">", constant(2)), NULL));
    expect(evaluate(unknown)).toBeNull();
    expect(evaluate(unknown, { row: rowWith(c0, 1) })).toBe(true);
  });

  it("refuses conditions that are not BOOLEAN", () => {
    expect(() => evaluate(not(constant(1)))).toThrow(DatatypeMismatchError);
    expect(() => evaluate(and(TRUE, constant("x")))).toThrow(
      DatatypeMismatchError,
    );
  });

  it("IS NULL and IS NOT NULL never return NULL", () => {
    expect(evaluate(nullCheck(c2))).toBe(true);
    expect(evaluate(notNullCheck(c2))).toBe(false);
    expect(evaluate(notNullCheck(c3))).toBe(true);
  });

  it("BETWEEN is x >= a AND x <= b", () => {
    expect(evaluate(between(c0, constant(1), constant(10)))).toBe(true);
    // 10 <= 5 is false, so the AND is false despite the NULL bound.
    expect(evaluate(between(c0, NULL, constant(5)))).toBe(false);
    expect(evaluate(between(c0, NULL, constant(50)))).toBeNull();
    expect(evaluate(between(NULL, constant(1), constant(2)))).toBeNull();
  });

  it("evaluates the BETWEEN operand once", () => {
    const operand = counting("M");
    evaluate(between(operand.expression, constant("A"), constant("Z")));
    expect(operand.calls()).toBe(1);
  });

  it("IN is true, NULL or false as values says", () => {
    expect(evaluate(inList(c0, constant(1), constant(10)))).toBe(true);
    expect(evaluate(inList(c0, constant(1), NULL))).toBeNull();
    expect(evaluate(inList(c0, constant(1), constant(2)))).toBe(false);
    expect(evaluate(inList(NULL, constant(1)))).toBeNull();
    expect(evaluate(inList(c0))).toBe(false);
  });

  it("NOT IN is NOT of IN", () => {
    expect(evaluate(notInList(c0, constant(1), NULL))).toBeNull();
    expect(evaluate(notInList(c0, constant(1)))).toBe(true);
    expect(evaluate(notInList(c0, c0))).toBe(false);
  });
});

describe("arithmetic", () => {
  it("divides INTEGERs toward zero and REALs exactly", () => {
    expect(evaluate(arithmetic(constant(7), "/", constant(2)))).toBe(3);
    expect(evaluate(arithmetic(constant(-7), "/", constant(2)))).toBe(-3);
    expect(evaluate(arithmetic(constant(7.5), "/", constant(2)))).toBe(3.75);
  });

  it("decides by the values when an operand's type is not known", () => {
    const half = arithmetic(parameter(0), "/", constant(2));
    expect(evaluate(half, { parameters: [7] })).toBe(3);
    expect(evaluate(half, { parameters: [7.5] })).toBe(3.75);
  });

  it("takes the sign of the dividend for %, and is NULL dividing by 0", () => {
    expect(evaluate(arithmetic(constant(-7), "%", constant(3)))).toBe(-1);
    expect(evaluate(arithmetic(constant(1), "/", constant(0)))).toBeNull();
    expect(evaluate(arithmetic(constant(1), "%", constant(0)))).toBeNull();
  });

  it("fails when an INTEGER result is not a safe integer", () => {
    const max = constant(Number.MAX_SAFE_INTEGER);
    expect(() => evaluate(arithmetic(max, "+", constant(1)))).toThrow(
      IntegerOverflowError,
    );
    expect(evaluate(arithmetic(max, "+", constant(1, "REAL")))).toBe(
      Number.MAX_SAFE_INTEGER + 1,
    );
  });

  it("is NULL with a NULL operand, and refuses values that are not numbers", () => {
    expect(evaluate(arithmetic(c2, "+", constant(1)))).toBeNull();
    const sum = arithmetic(parameter(0), "+", constant(1));
    expect(() => evaluate(sum, { parameters: ["x"] })).toThrow(
      DatatypeMismatchError,
    );
  });

  it("negates numbers, and NULL to NULL", () => {
    expect(evaluate(negate(c0))).toBe(-10);
    expect(evaluate(negate(c2))).toBeNull();
  });

  it("concatenates text, printing numbers and booleans", () => {
    expect(evaluate(concat(constant("a"), constant(1.5)))).toBe("a1.5");
    expect(evaluate(concat(constant("a"), TRUE))).toBe("atrue");
    expect(evaluate(concat(constant("a"), NULL))).toBeNull();
  });
});

describe("CASE", () => {
  it("picks the first WHEN that holds, else ELSE, else NULL", () => {
    const second = caseWhen(
      [
        [FALSE, constant(1)],
        [TRUE, constant(2)],
      ],
      constant(3),
    );
    expect(evaluate(second)).toBe(2);
    expect(evaluate(caseWhen([[NULL, constant(1)]]))).toBeNull();
  });

  it("evaluates only the branch it picks", () => {
    expect(evaluate(caseWhen([[TRUE, constant(1)]], failing))).toBe(1);
  });

  it("evaluates the operand once and compares it with each WHEN value", () => {
    const operand = counting(2);
    const numbers = simpleCase(operand.expression, [
      [constant(1), constant("one")],
      [constant(2), constant("two")],
      [constant(3), constant("three")],
    ]);
    expect(evaluate(numbers)).toBe("two");
    expect(operand.calls()).toBe(1);
  });

  it("matches nothing with a NULL operand", () => {
    expect(evaluate(simpleCase(NULL, [[NULL, constant(1)]], constant(2)))).toBe(
      2,
    );
  });
});

describe("functions", () => {
  it("call the registry's implementation", () => {
    expect(evaluate(call("upper", c1))).toBe("HELLO");
    expect(evaluate(call("substr", c1, constant(0), constant(2)))).toBe("h");
    expect(evaluate(call("round", constant(-2.5)))).toBe(-3);
  });

  it("evaluate lazy arguments only as needed", () => {
    expect(evaluate(call("coalesce", constant(1), failing))).toBe(1);
    expect(evaluate(call("iif", TRUE, constant(1), failing))).toBe(1);
  });

  it("are implemented once per compile, told the types and fixed arguments", () => {
    // Definitions are frozen: spy on a copy.
    const like = scalarFunction("like");
    const implement = vi.fn(like.implement);
    const spied = callOf({ ...like, implement }, [parameter(0), c1]);
    const rows = [ROW, rowWith(c1, "x")];
    const options = { parameters: ["h%"] };
    expect(evaluateEach(spied, rows, options)).toEqual([true, false]);
    expect(implement).toHaveBeenCalledOnce();
    expect(implement).toHaveBeenCalledWith({
      argumentTypes: ["ANY", "TEXT"],
      constants: ["h%", undefined],
    });
  });
});

describe("casts and JSON access", () => {
  it("cast as values/convert defines", () => {
    expect(evaluate(cast(constant("42"), "INTEGER"))).toBe(42);
    expect(evaluate(cast(constant(3.9), "INTEGER"))).toBe(3);
    expect(evaluate(cast(TRUE, "TEXT"))).toBe("true");
    expect(evaluate(cast(blob(0xca, 0xfe), "TEXT"))).toBe("cafe");
    expect(() => evaluate(failing)).toThrow(DatatypeMismatchError);
  });

  it("follow a JSON path, NULL where there is nothing", () => {
    const data = column(0, 0, "JSON");
    const row = [{ name: "Alice", items: [10, 20], nested: { x: null } }];
    const at = (...path: (string | number)[]) =>
      evaluate(jsonAccess(data, ...path), { layout: [data.binding], row });
    expect(at("name")).toBe("Alice");
    expect(at("items", 1)).toBe(20);
    expect(at("missing")).toBeNull();
    expect(at("nested", "x")).toBeNull();
    expect(at("name", "length")).toBeNull();
  });

  it("give NULL for a BLOB, which holds no JSON", () => {
    const data = column(0, 0, "ANY");
    const row = [new Uint8Array([1])];
    expect(
      evaluate(jsonAccess(data, "a"), { layout: [data.binding], row }),
    ).toBeNull();
  });
});

describe("predicates", () => {
  it("hold only when the condition is TRUE", () => {
    const isLarge = compare(c0, ">", constant(5));
    expect(holds(isLarge)).toBe(true);
    expect(holds(isLarge, { row: rowWith(c0, 1) })).toBe(false);
    expect(holds(isLarge, { row: rowWith(c0, null) })).toBe(false);
  });

  it("refuse a value that is not BOOLEAN", () => {
    expect(() => holds(c0)).toThrow(DatatypeMismatchError);
  });
});
