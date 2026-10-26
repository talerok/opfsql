import { describe, expect, it } from "vitest";
import { mulberry32 } from "../../../../test/support/random.js";
import { randomValue } from "../../../../test/support/values.js";
import { CorruptError } from "../../../errors.js";
import { areEqual } from "../../../values/index.js";
import type { RecordField } from "../record.js";
import {
  decodeRecord,
  encodeRecord,
  OverflowValue,
  prepareField,
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
      const fields = decodeRecord(encode(values));
      expect(fields).toHaveLength(values.length);
      for (const [index, value] of values.entries()) {
        const field = fields[index] ?? null;
        expect(field).not.toBeInstanceOf(OverflowValue);
        if (field instanceof OverflowValue) continue;
        expect(areEqual(field, value)).toBe(true);
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
    // A one-field record has two header bytes: its size, then the serial type.
    const header = 2;
    const sizes = [0, 1, 2, 127, 128, 2 ** 31, 2 ** 47, 2 ** 53 - 1].map(
      (value) => encode([value]).length - header,
    );
    expect(sizes).toEqual([0, 0, 1, 1, 2, 6, 8, 8]);
  });

  it("decode the fields asked for, skipping the others' bodies", () => {
    const record = encode(["skip me", 42, null, true]);
    const wanted = [false, true, false, true];
    expect(decodeRecord(record, wanted)).toEqual([
      undefined,
      42,
      undefined,
      true,
    ]);
  });

  it("give overflow fields as references to their chains", () => {
    const reference = new OverflowValue("text", 70_000, 123);
    expect(decodeRecord(encode([1, reference, "x"]))).toEqual([
      1,
      reference,
      "x",
    ]);
  });

  it("refuse a body that does not match the header", () => {
    const bytes = encode(["abc"]);
    expect(() => decodeRecord(bytes.subarray(0, -1))).toThrow(CorruptError);
  });
});
