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
      expect(arithmetic("ADD", mode)(null, 1)).toBeNull();
      expect(arithmetic("DIVIDE", mode)(1, null)).toBeNull();
    }
  });

  it("divides INTEGERs toward zero and REALs exactly", () => {
    expect(arithmetic("DIVIDE", "integer")(7, 2)).toBe(3);
    expect(arithmetic("DIVIDE", "integer")(-7, 2)).toBe(-3);
    expect(arithmetic("DIVIDE", "real")(7, 2)).toBe(3.5);
  });

  it("decides by the values when the types are not known", () => {
    const divide = arithmetic("DIVIDE", "dynamic");
    expect(divide(7, 2)).toBe(3);
    expect(divide(7.5, 2)).toBe(3.75);
    // Not a safe integer, so not an INTEGER: divides exactly.
    expect(divide(1e20, 3)).toBeCloseTo(3.333e19, -17);
  });

  it("takes the sign of the dividend for %", () => {
    expect(arithmetic("MOD", "integer")(-7, 3)).toBe(-1);
    expect(arithmetic("MOD", "integer")(7, -3)).toBe(1);
  });

  it("truncates REAL operands of % to integers, as SQLite does", () => {
    expect(arithmetic("MOD", "real")(5.5, 2)).toBe(1);
    expect(arithmetic("MOD", "real")(5, 0.5)).toBeNull();
  });

  it("is NULL for division or remainder by zero", () => {
    for (const mode of ["integer", "real", "dynamic"] as const) {
      expect(arithmetic("DIVIDE", mode)(1, 0)).toBeNull();
      expect(arithmetic("MOD", mode)(1, 0)).toBeNull();
    }
  });

  it("fails when an INTEGER result leaves the safe range", () => {
    expect(() => arithmetic("ADD", "integer")(MAX, 1)).toThrow(
      IntegerOverflowError,
    );
    expect(() => arithmetic("MULTIPLY", "dynamic")(MAX, 2)).toThrow(
      IntegerOverflowError,
    );
    expect(arithmetic("ADD", "real")(MAX, 1)).toBe(MAX + 1);
  });

  it("never returns NaN or -0", () => {
    expect(arithmetic("SUBTRACT", "real")(Infinity, Infinity)).toBeNull();
    expect(Object.is(arithmetic("MULTIPLY", "integer")(-3, 0), 0)).toBe(true);
    expect(Object.is(negate(0), 0)).toBe(true);
  });

  it("takes text that is exactly a number, and nothing else", () => {
    expect(arithmetic("ADD", "dynamic")("2", 3)).toBe(5);
    expect(() => arithmetic("ADD", "dynamic")("two", 3)).toThrow(
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
