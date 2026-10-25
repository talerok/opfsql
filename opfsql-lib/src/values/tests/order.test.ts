import { describe, expect, it } from "vitest";
import { mulberry32 } from "../../../test/support/random.js";
import { randomText, randomValue } from "../../../test/support/values.js";
import { withoutNegativeZero } from "../convert.js";
import { rowKey, valueKey } from "../key.js";
import {
  areEqual,
  canonicalJson,
  compareBytes,
  compareText,
  compareValues,
} from "../order.js";
import type { Value } from "../value.js";

const encoder = new TextEncoder();

/** -1, 0 or 1: Math.sign, which keeps -0, made comparable with toBe(0). */
function sign(n: number): number {
  return withoutNegativeZero(Math.sign(n));
}

describe("compareValues", () => {
  it("orders kinds NULL < BOOLEAN < NUMBER < TEXT < BLOB < JSON", () => {
    const ascending: Value[] = [
      null,
      false,
      true,
      -Infinity,
      -1,
      0,
      1.5,
      Infinity,
      "",
      "a",
      new Uint8Array(),
      new Uint8Array([0]),
      [],
      {},
    ];
    for (const [index, value] of ascending.slice(1).entries()) {
      expect(compareValues(ascending[index]!, value)).toBeLessThan(0);
    }
  });

  it("treats values of different JavaScript identity as equal when SQL does", () => {
    expect(areEqual(1, 0.5 * 2)).toBe(true);
    expect(areEqual(0, -0)).toBe(true);
    expect(areEqual(null, null)).toBe(true);
    expect(areEqual(new Uint8Array([1, 2]), new Uint8Array([1, 2]))).toBe(true);
    expect(areEqual({ a: 1, b: [2] }, { b: [2], a: 1 })).toBe(true);
    expect(areEqual(true, 1)).toBe(false);
    expect(areEqual("1", 1)).toBe(false);
    expect(areEqual("a", "A")).toBe(false);
  });

  it.each([1, 2, 3])("is a total order (seed %i)", (seed) => {
    const random = mulberry32(seed);
    for (let round = 0; round < 3000; round++) {
      const first = randomValue(random);
      const second = randomValue(random);
      const third = randomValue(random);
      expect(compareValues(first, first)).toBe(0);
      expect(
        sign(compareValues(first, second)) + sign(compareValues(second, first)),
      ).toBe(0);
      if (
        compareValues(first, second) <= 0 &&
        compareValues(second, third) <= 0
      ) {
        expect(compareValues(first, third)).toBeLessThanOrEqual(0);
      }
    }
  });

  it.each([1, 2, 3])(
    "gives equal values, and only those, the same key (seed %i)",
    (seed) => {
      const random = mulberry32(seed);
      for (let round = 0; round < 3000; round++) {
        const a = randomValue(random);
        const b = randomValue(random);
        expect(valueKey(a) === valueKey(b)).toBe(compareValues(a, b) === 0);
      }
    },
  );
});

describe("compareText", () => {
  it.each([1, 2, 3])("orders like UTF-8 bytes (seed %i)", (seed) => {
    const random = mulberry32(seed);
    for (let round = 0; round < 3000; round++) {
      const a = randomText(random);
      const b = randomText(random);
      expect(sign(compareText(a, b))).toBe(
        sign(compareBytes(encoder.encode(a), encoder.encode(b))),
      );
    }
  });

  it("puts code points above U+FFFF after U+E000–U+FFFF, unlike <", () => {
    const emoji = "\u{1F600}";
    const privateUse = "\u{E000}";
    expect(compareText(emoji, privateUse)).toBeGreaterThan(0);
  });
});

describe("compareBytes", () => {
  it("compares lexicographically, prefixes first", () => {
    expect(
      compareBytes(new Uint8Array([1]), new Uint8Array([1, 0])),
    ).toBeLessThan(0);
    expect(
      compareBytes(new Uint8Array([2]), new Uint8Array([1, 9])),
    ).toBeGreaterThan(0);
    expect(compareBytes(new Uint8Array([1, 2]), new Uint8Array([1, 2]))).toBe(
      0,
    );
  });
});

describe("canonicalJson", () => {
  it("sorts keys at every level and drops whitespace", () => {
    expect(canonicalJson({ b: { d: 1, c: [true, null] }, a: "x" })).toBe(
      '{"a":"x","b":{"c":[true,null],"d":1}}',
    );
  });
});

describe("rowKey", () => {
  it("keeps the boundaries between values", () => {
    expect(rowKey(["ab", "c"])).not.toBe(rowKey(["a", "bc"]));
    expect(rowKey([1, "x"])).toBe(rowKey([0.5 * 2, "x"]));
  });
});
