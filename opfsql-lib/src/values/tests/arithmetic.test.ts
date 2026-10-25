import { describe, expect, it } from "vitest";
import { DatatypeMismatchError, IntegerOverflowError } from "../../errors.js";
import {
  arithmetic,
  concatenate,
  negate,
  numberOperand,
  textOperand,
} from "../arithmetic.js";
import { truthOf } from "../logic.js";

const MAX = Number.MAX_SAFE_INTEGER;

describe("arithmetic", () => {
  it("is NULL when either operand is NULL", () => {
    for (const mode of ["integer", "real", "dynamic"] as const) {
      expect(arithmetic("add", mode)(null, 1)).toBeNull();
      expect(arithmetic("divide", mode)(1, null)).toBeNull();
    }
  });

  it("divides INTEGERs toward zero and REALs exactly", () => {
    expect(arithmetic("divide", "integer")(7, 2)).toBe(3);
    expect(arithmetic("divide", "integer")(-7, 2)).toBe(-3);
    expect(arithmetic("divide", "real")(7, 2)).toBe(3.5);
  });

  it("decides by the values when the types are not known", () => {
    const divide = arithmetic("divide", "dynamic");
    expect(divide(7, 2)).toBe(3);
    expect(divide(7.5, 2)).toBe(3.75);
    // Not a safe integer, so not an INTEGER: divides exactly.
    expect(divide(1e20, 3)).toBeCloseTo(3.333e19, -17);
  });

  it("takes the sign of the dividend for %", () => {
    expect(arithmetic("remainder", "integer")(-7, 3)).toBe(-1);
    expect(arithmetic("remainder", "integer")(7, -3)).toBe(1);
  });

  it("truncates REAL operands of % to integers, as SQLite does", () => {
    expect(arithmetic("remainder", "real")(5.5, 2)).toBe(1);
    expect(arithmetic("remainder", "real")(5, 0.5)).toBeNull();
  });

  it("is NULL for division or remainder by zero", () => {
    for (const mode of ["integer", "real", "dynamic"] as const) {
      expect(arithmetic("divide", mode)(1, 0)).toBeNull();
      expect(arithmetic("remainder", mode)(1, 0)).toBeNull();
    }
  });

  it("fails when an INTEGER result leaves the safe range", () => {
    expect(() => arithmetic("add", "integer")(MAX, 1)).toThrow(
      IntegerOverflowError,
    );
    expect(() => arithmetic("multiply", "dynamic")(MAX, 2)).toThrow(
      IntegerOverflowError,
    );
    expect(arithmetic("add", "real")(MAX, 1)).toBe(MAX + 1);
  });

  it("never returns NaN or -0", () => {
    expect(arithmetic("subtract", "real")(Infinity, Infinity)).toBeNull();
    expect(Object.is(arithmetic("multiply", "integer")(-3, 0), 0)).toBe(true);
    expect(Object.is(negate(0), 0)).toBe(true);
  });

  it("takes text that is exactly a number, and nothing else", () => {
    expect(arithmetic("add", "dynamic")("2", 3)).toBe(5);
    expect(() => arithmetic("add", "dynamic")("two", 3)).toThrow(
      DatatypeMismatchError,
    );
    expect(() => numberOperand(true)).toThrow(DatatypeMismatchError);
    expect(() => numberOperand(new Uint8Array([1]))).toThrow(
      DatatypeMismatchError,
    );
  });
});

describe("concatenation", () => {
  it("prints numbers as JavaScript does and booleans as words", () => {
    expect(concatenate("a", 1.5)).toBe("a1.5");
    expect(concatenate(true, false)).toBe("truefalse");
    expect(concatenate("a", null)).toBeNull();
  });

  it("writes JSON as JSON text and refuses BLOBs", () => {
    expect(textOperand({ a: [1] })).toBe('{"a":[1]}');
    expect(() => textOperand(new Uint8Array([1]))).toThrow(
      DatatypeMismatchError,
    );
  });
});

describe("conditions", () => {
  it("are BOOLEAN or NULL, never judged by truthiness", () => {
    expect(truthOf(true)).toBe(true);
    expect(truthOf(null)).toBeNull();
    expect(() => truthOf(1)).toThrow(DatatypeMismatchError);
    expect(() => truthOf("true")).toThrow(DatatypeMismatchError);
  });
});
