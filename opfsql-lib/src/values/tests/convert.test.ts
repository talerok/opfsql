import { describe, expect, it } from "vitest";
import { DatatypeMismatchError } from "../../errors.js";
import {
  applyAffinity,
  assignValue,
  bytesToHex,
  castValue,
  hexToBytes,
  isStoredAs,
  normalizeNumber,
  normalizeValue,
  numberFromText,
  parseJsonText,
  truncate,
} from "../convert.js";
import type { SqlType } from "../types.js";
import { typeFromName } from "../types.js";
import type { Value } from "../value.js";

const blob = new Uint8Array([0xde, 0xad]);

describe("normalizeValue", () => {
  it("turns NaN into NULL and -0 into 0", () => {
    expect(normalizeValue(NaN)).toBeNull();
    expect(Object.is(normalizeValue(-0), 0)).toBe(true);
  });

  it("rejects text with a lone surrogate", () => {
    expect(() => normalizeValue("a\u{D800}b")).toThrow(DatatypeMismatchError);
    expect(normalizeValue("\u{1F600}")).toBe("\u{1F600}");
  });
});

describe("assignValue", () => {
  it.each([
    ["INTEGER", 42, 42],
    ["INTEGER", 4 / 2, 2],
    ["INTEGER", "-7", -7],
    ["INTEGER", "1e2", 100],
    ["REAL", 1.5, 1.5],
    ["REAL", "1.5", 1.5],
    ["REAL", Infinity, Infinity],
    ["TEXT", "x", "x"],
    ["TEXT", 1.5, "1.5"],
    ["BLOB", blob, blob],
    ["BOOLEAN", true, true],
    ["BOOLEAN", 0, false],
    ["BOOLEAN", 1, true],
    ["JSON", '{"a":[1]}', { a: [1] }],
    ["JSON", '"s"', "s"],
    ["JSON", [1, 2], [1, 2]],
    ["JSON", 3, 3],
    ["ANY", blob, blob],
  ] satisfies [SqlType, Value, Value][])(
    "stores %s from %j as %j",
    (type, value, expected) => {
      expect(assignValue(value, type)).toEqual(expected);
    },
  );

  it.each([
    ["INTEGER", 2.5],
    ["INTEGER", 2 ** 53],
    ["INTEGER", "1.5"],
    ["INTEGER", "ten"],
    ["INTEGER", " 1"],
    ["INTEGER", true],
    ["REAL", "1,5"],
    ["TEXT", true],
    ["TEXT", blob],
    ["BLOB", "dead"],
    ["BOOLEAN", 2],
    ["BOOLEAN", "true"],
    ["JSON", "{not json"],
    ["JSON", blob],
  ] as const)("refuses to store %s from %j", (type, value) => {
    expect(() => assignValue(value, type)).toThrow(DatatypeMismatchError);
  });

  it("stores NULL as every type", () => {
    const types = [
      "INTEGER",
      "REAL",
      "TEXT",
      "BLOB",
      "BOOLEAN",
      "JSON",
      "ANY",
    ] as const;
    for (const type of types) {
      expect(assignValue(null, type)).toBeNull();
    }
  });
});

describe("castValue", () => {
  it.each([
    ["INTEGER", 2.9, 2],
    ["INTEGER", -2.9, -2],
    ["INTEGER", -0.5, 0],
    ["INTEGER", true, 1],
    ["INTEGER", "12", 12],
    ["REAL", false, 0],
    ["TEXT", true, "true"],
    ["TEXT", blob, "dead"],
    ["TEXT", { b: 1, a: 2 }, '{"a":2,"b":1}'],
    ["BLOB", "dEaD", blob],
    ["BOOLEAN", "TRUE", true],
    ["BOOLEAN", "false", false],
    ["BOOLEAN", 1, true],
  ] as const)("casts to %s from %j as %j", (type, value, expected) => {
    expect(castValue(value, type)).toEqual(expected);
  });

  it.each([
    ["INTEGER", "12abc"],
    ["INTEGER", 1e300],
    ["REAL", "abc"],
    ["BLOB", "abc"],
    ["BOOLEAN", "yes"],
    ["BOOLEAN", 2],
  ] as const)("refuses to cast to %s from %j", (type, value) => {
    expect(() => castValue(value, type)).toThrow(DatatypeMismatchError);
  });
});

describe("numberFromText", () => {
  it("reads exactly the numbers SQL writes", () => {
    expect(
      ["1", "-1", "+1.", ".5", "1e3", "2.5E-1"].map((text) =>
        numberFromText(text),
      ),
    ).toEqual([1, -1, 1, 0.5, 1000, 0.25]);
    expect(
      ["", " 1", "1 ", "0x10", "1_000", "Infinity", "."].map((text) =>
        numberFromText(text),
      ),
    ).toEqual([null, null, null, null, null, null, null]);
  });

  it("reads -0 as 0, as every value has it", () => {
    expect(Object.is(numberFromText("-0"), 0)).toBe(true);
    expect(Object.is(castValue("-0.0", "REAL"), 0)).toBe(true);
  });
});

describe("parseJsonText", () => {
  it("normalizes a document at any depth", () => {
    const parsed = parseJsonText('{"a": [-0, {"b": -0.0}]}');
    expect(parsed).toEqual({ a: [0, { b: 0 }] });
    expect(Object.is((parsed as { a: number[] }).a[0], 0)).toBe(true);
  });

  it("refuses a lone surrogate written as an escape, in a value or a key", () => {
    for (const text of [
      String.raw`["\ud800"]`,
      String.raw`{"\udc00": 1}`,
      String.raw`"\ud800x"`,
    ]) {
      expect(() => parseJsonText(text)).toThrow(DatatypeMismatchError);
    }
    expect(parseJsonText(String.raw`"\ud83d\ude00"`)).toBe("😀");
  });

  it("refuses text that is not JSON", () => {
    expect(() => parseJsonText("{")).toThrow(DatatypeMismatchError);
  });
});

describe("hex", () => {
  it("round-trips bytes", () => {
    expect(hexToBytes(bytesToHex(blob))).toEqual(blob);
    expect(() => hexToBytes("abc")).toThrow(DatatypeMismatchError);
  });
});

describe("isStoredAs", () => {
  it.each([
    ["INTEGER", 42, true],
    ["INTEGER", "42", false],
    ["TEXT", "42", true],
    ["REAL", 1.5, true],
    ["BOOLEAN", 1, false],
    ["JSON", "abc", true],
    ["JSON", { a: 1 }, true],
    ["JSON", Uint8Array.from([1]), false],
    ["BLOB", null, true],
  ] as const)("a %s column stores %j as it is: %s", (type, value, expected) => {
    expect(isStoredAs(value, type)).toBe(expected);
  });
});

describe("typeFromName", () => {
  it("maps declared names to types", () => {
    expect(
      ["integer", "BIGINT", "varchar(255)", "Double", "bool", "json"].map(
        (name) => typeFromName(name),
      ),
    ).toEqual(["INTEGER", "INTEGER", "TEXT", "REAL", "BOOLEAN", "JSON"]);
    expect(typeFromName("DECIMAL")).toBeNull();
  });

  it("changes the case of ASCII letters only", () => {
    // Unicode upper-casing turns the dotless ı into I.
    expect(typeFromName("\u{131}nteger")).toBeNull();
  });
});

describe("comparison affinity", () => {
  it("numeric affinity takes text that is exactly a number", () => {
    expect(applyAffinity("5", "numeric")).toBe(5);
    expect(applyAffinity("1.5e1", "numeric")).toBe(15);
    expect(applyAffinity("five", "numeric")).toBe("five");
    expect(applyAffinity(" 5", "numeric")).toBe(" 5");
    expect(applyAffinity(true, "numeric")).toBe(true);
  });

  it("text affinity prints numbers", () => {
    expect(applyAffinity(5, "text")).toBe("5");
    expect(applyAffinity(1.5, "text")).toBe("1.5");
    expect(applyAffinity(false, "text")).toBe(false);
    expect(applyAffinity(null, "text")).toBeNull();
  });
});

describe("number helpers", () => {
  it("truncates toward zero, never to -0", () => {
    expect(truncate(2.7)).toBe(2);
    expect(truncate(-2.7)).toBe(-2);
    expect(truncate(-0.5)).toBe(0);
  });

  it("normalizes computed numbers: NaN is NULL, -0 is 0", () => {
    expect(normalizeNumber(NaN)).toBeNull();
    expect(normalizeNumber(-0)).toBe(0);
    expect(normalizeNumber(1.5)).toBe(1.5);
  });
});
