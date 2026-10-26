import { describe, expect, it } from "vitest";
import { MisuseError } from "../../errors.js";
import { NO_PARAMETERS, parametersOf } from "../parameters.js";

// Parameters are checked once, where a caller hands them in: values as
// docs/semantics.md defines them, normalized and copied; anything else is
// MISUSE naming where in which parameter it was.

function refusalOf(values: unknown): string {
  try {
    parametersOf(values);
  } catch (error) {
    expect(error).toBeInstanceOf(MisuseError);
    return (error as MisuseError).message;
  }
  throw new Error("the parameters were accepted");
}

describe("parametersOf", () => {
  it("takes no parameters as none", () => {
    expect(parametersOf(undefined)).toBe(NO_PARAMETERS);
    expect(parametersOf([])).toEqual([]);
  });

  it("keeps every kind of value as it is", () => {
    const values = [null, true, 1.5, -Infinity, "é", { a: [1, "x"] }];
    expect(parametersOf(values)).toEqual(values);
  });

  it("turns NaN into NULL and -0 into 0, inside JSON too", () => {
    const [nan, zero, json] = parametersOf([NaN, -0, { a: [NaN, -0] }]);
    expect(nan).toBeNull();
    expect(Object.is(zero, 0)).toBe(true);
    expect(json).toEqual({ a: [null, 0] });
    expect(Object.is((json as { a: number[] }).a[1], 0)).toBe(true);
  });

  it("copies BLOBs and JSON, so the caller's later changes change nothing", () => {
    const blob = new Uint8Array([1, 2]);
    const json = { list: [1] };
    const [copiedBlob, copiedJson] = parametersOf([blob, json]);
    blob[0] = 9;
    json.list.push(2);
    expect(copiedBlob).toEqual(new Uint8Array([1, 2]));
    expect(copiedJson).toEqual({ list: [1] });
  });

  it("makes a BLOB of a Uint8Array subclass a plain Uint8Array", () => {
    class Bytes extends Uint8Array {}
    const [blob] = parametersOf([new Bytes([7])]);
    expect(Object.getPrototypeOf(blob)).toBe(Uint8Array.prototype);
    expect(blob).toEqual(new Uint8Array([7]));
  });

  it("keeps a key named __proto__ as a key", () => {
    const object = JSON.parse('{"__proto__": 1}') as object;
    const [json] = parametersOf([object]);
    expect(Object.keys(json as object)).toEqual(["__proto__"]);
  });

  it("accepts objects without a prototype", () => {
    const object = Object.assign(Object.create(null) as object, { a: 1 });
    expect(parametersOf([object])).toEqual([{ a: 1 }]);
  });

  it.each([
    ["undefined", [undefined], "parameter $1 is undefined: pass null for NULL"],
    ["a BigInt", [1, 2n], "parameter $2 is a BigInt"],
    [
      "a function",
      [() => 1],
      "parameter $1 is a function, which is not a value",
    ],
    [
      "a symbol",
      [Symbol("s")],
      "parameter $1 is a symbol, which is not a value",
    ],
    ["a Date", [new Date(0)], "parameter $1 is a Date, which is not a value"],
    ["a Map", [new Map()], "parameter $1 is a Map, which is not a value"],
    ["a Set", [new Set()], "parameter $1 is a Set, which is not a value"],
    [
      "a class instance",
      [
        new (class Point {
          readonly x = 1;
        })(),
      ],
      "parameter $1 is a Point",
    ],
    ["an ArrayBuffer", [new ArrayBuffer(2)], "parameter $1 is an ArrayBuffer"],
    ["an Int16Array", [new Int16Array(2)], "parameter $1 is an Int16Array"],
    [
      "a Uint8ClampedArray",
      [new Uint8ClampedArray(2)],
      "parameter $1 is a Uint8ClampedArray",
    ],
    [
      "a DataView",
      [new DataView(new ArrayBuffer(2))],
      "parameter $1 is a DataView",
    ],
  ])("refuses %s", (_, values, message) => {
    expect(refusalOf(values)).toContain(message);
  });

  it("refuses what JSON cannot hold, naming where it is", () => {
    expect(refusalOf([1, { tags: ["a", undefined] }])).toBe(
      "parameter $2.tags[1] is undefined: pass null for NULL",
    );
    expect(refusalOf([{ data: new Uint8Array(1) }])).toContain(
      "parameter $1.data is a Uint8Array: a BLOB is a Uint8Array, and only a whole parameter",
    );
    expect(refusalOf([{ when: new Date(0) }])).toContain(
      "parameter $1.when is a Date",
    );
    expect(refusalOf([{ [Symbol("s")]: 1 }])).toBe(
      "parameter $1 has symbol keys, which JSON cannot hold",
    );
    const sparse: unknown[] = [1];
    sparse[2] = 3;
    expect(refusalOf([sparse])).toBe("parameter $1[1] is a hole in the array");
  });

  it("refuses a lone surrogate anywhere", () => {
    const lone = "a\u{D800}";
    const problem = "has a lone surrogate, which is not valid Unicode";
    expect(refusalOf([lone])).toBe(`parameter $1 ${problem}`);
    expect(refusalOf([{ list: [lone] }])).toBe(
      `parameter $1.list[0] ${problem}`,
    );
    expect(refusalOf([{ [lone]: 1 }])).toBe(`parameter $1.${lone} ${problem}`);
  });

  it("refuses JSON that refers to itself, or nests without end", () => {
    const loop: unknown[] = [];
    loop.push(loop);
    expect(refusalOf([loop])).toBe("parameter $1[0] refers to itself");
    let deep: unknown = 1;
    for (let level = 0; level < 2000; level++) deep = [deep];
    expect(refusalOf([deep])).toContain("nests deeper than 1000 levels");
  });

  it("accepts the same object twice, side by side", () => {
    const shared = { a: 1 };
    expect(parametersOf([[shared, shared]])).toEqual([[{ a: 1 }, { a: 1 }]]);
  });

  it("refuses parameters that are not an array", () => {
    expect(refusalOf("a")).toBe(
      "parameters are an array of values, not a string",
    );
    expect(refusalOf({ 0: 1 })).toBe(
      "parameters are an array of values, not an Object",
    );
  });
});
