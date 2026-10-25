import { describe, expect, it } from "vitest";
import { mulberry32 } from "../../../../test/support/random.js";
import { randomValue } from "../../../../test/support/values.js";
import { CorruptError } from "../../../errors.js";
import { areEqual } from "../../../values/index.js";
import type { RecordField } from "../record.js";
import {
  encodeRecord,
  OverflowValue,
  prepareField,
  RecordReader,
  recordSize,
} from "../record.js";

function encode(fields: readonly RecordField[]): Uint8Array {
  return encodeRecord(fields.map((field) => prepareField(field)));
}

describe("records", () => {
  it.each([1, 2, 3])("round-trip every kind of value (seed %i)", (seed) => {
    const random = mulberry32(seed);
    for (let round = 0; round < 2000; round++) {
      const values = Array.from({ length: Math.floor(random() * 6) }, () =>
        randomValue(random),
      );
      const reader = new RecordReader(encode(values));
      expect(reader).toHaveLength(values.length);
      for (const [index, value] of values.entries()) {
        expect(areEqual(reader.value(index), value)).toBe(true);
      }
    }
  });

  it("know their size before encoding", () => {
    const random = mulberry32(9);
    for (let round = 0; round < 500; round++) {
      const prepared = Array.from({ length: Math.floor(random() * 40) }, () =>
        prepareField(randomValue(random)),
      );
      expect(encodeRecord(prepared)).toHaveLength(recordSize(prepared));
    }
  });

  it("store integers in the fewest bytes", () => {
    const sizes = [0, 1, 2, 127, 128, 2 ** 31, 2 ** 47, 2 ** 53 - 1].map(
      (value) => encode([value]).length,
    );
    expect(sizes).toEqual([2, 2, 3, 3, 4, 8, 10, 10]);
  });

  it("decode one field without the others", () => {
    const reader = new RecordReader(encode(["skip me", 42, null, true]));
    expect(reader.value(1)).toBe(42);
    expect(reader.value(3)).toBe(true);
  });

  it("hold overflow references apart from inline values", () => {
    const reference = new OverflowValue("text", 70_000, 123);
    const reader = new RecordReader(encode([1, reference, "x"]));
    expect(reader.isOverflow(1)).toBe(true);
    expect(reader.isOverflow(2)).toBe(false);
    expect(reader.overflow(1)).toEqual(reference);
    expect(reader.value(2)).toBe("x");
  });

  it("refuse a body that does not match the header", () => {
    const bytes = encode(["abc"]);
    expect(() => new RecordReader(bytes.subarray(0, -1))).toThrow(CorruptError);
  });
});
