import { describe, expect, it } from "vitest";
import { mulberry32 } from "../../../../test/support/random.js";
import { randomValue } from "../../../../test/support/values.js";
import type { Value } from "../../../values/index.js";
import { compareBytes, compareValues } from "../../../values/index.js";
import { decodeKey, encodeKey, keyRowid, prefixEnd } from "../key.js";

/** SQL order of value lists: by value, then a prefix first. */
function compareLists(a: readonly Value[], b: readonly Value[]): number {
  for (let index = 0; index < Math.min(a.length, b.length); index++) {
    const difference = compareValues(a[index] ?? null, b[index] ?? null);
    if (difference !== 0) return difference;
  }
  return a.length - b.length;
}

function randomList(random: () => number): Value[] {
  return Array.from({ length: 1 + Math.floor(random() * 3) }, () =>
    randomValue(random),
  );
}

describe("index keys", () => {
  it.each([1, 2, 3, 4])(
    "compare byte by byte in SQL order (seed %i)",
    (seed) => {
      const random = mulberry32(seed);
      for (let round = 0; round < 4000; round++) {
        const a = randomList(random);
        const b = randomList(random);
        expect(Math.sign(compareBytes(encodeKey(a), encodeKey(b)))).toBe(
          Math.sign(compareLists(a, b)),
        );
      }
    },
  );

  it.each([1, 2, 3])("decode back to equal values (seed %i)", (seed) => {
    const random = mulberry32(seed);
    for (let round = 0; round < 2000; round++) {
      const values = randomList(random);
      const decoded = decodeKey(encodeKey(values));
      expect(compareLists(decoded, values)).toBe(0);
    }
  });

  it("encode -0 as 0 and keep NULs inside text and BLOBs apart", () => {
    expect(encodeKey([-0])).toEqual(encodeKey([0]));
    expect(compareBytes(encodeKey(["a"]), encodeKey(["a\0"]))).toBeLessThan(0);
    expect(
      compareBytes(encodeKey(["a\0", 1]), encodeKey(["a", 2])),
    ).toBeGreaterThan(0);
    expect(decodeKey(encodeKey(["a\0b"]))).toEqual(["a\0b"]);
  });

  it("frame every key with a prefix between the prefix and its end", () => {
    const prefix = encodeKey(["user", 7]);
    const inside = encodeKey(["user", 7, "x", 42]);
    const after = encodeKey(["user", 8]);
    expect(compareBytes(prefix, inside)).toBeLessThan(0);
    expect(compareBytes(inside, prefixEnd(prefix))).toBeLessThan(0);
    expect(compareBytes(prefixEnd(prefix), after)).toBeLessThan(0);
  });

  it("end with the rowid", () => {
    expect(keyRowid(encodeKey(["x", null, 123]))).toBe(123);
    expect(keyRowid(encodeKey([-5]))).toBe(-5);
  });
});
