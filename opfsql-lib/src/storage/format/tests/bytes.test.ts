import { describe, expect, it } from "vitest";
import { CorruptError } from "../../../errors.js";
import type { IntegerWidth } from "../bytes.js";
import {
  ByteWriter,
  integerWidth,
  readInteger,
  readVarint,
  skipVarint,
  unzigzag,
  varintSize,
  zigzag,
} from "../bytes.js";

describe("varints", () => {
  it.each([0, 1, 127, 128, 16_383, 16_384, 2 ** 31, 2 ** 53 - 1])(
    "round-trip %i in the bytes they say they take",
    (value) => {
      const writer = new ByteWriter(1);
      writer.varint(value);
      const bytes = writer.toBytes();
      expect(bytes).toHaveLength(varintSize(value));
      expect(readVarint(bytes, 0)).toBe(value);
      expect(skipVarint(bytes, 0)).toBe(bytes.length);
    },
  );

  it("refuse a varint that never ends", () => {
    expect(() => readVarint(new Uint8Array(9).fill(0x80), 0)).toThrow(
      CorruptError,
    );
  });

  it.each([0, 1, -1, 63, -64, 2 ** 52, -(2 ** 52), Number.MAX_SAFE_INTEGER])(
    "zigzag %i and back",
    (value) => {
      expect(zigzag(value)).toBeGreaterThanOrEqual(0);
      expect(unzigzag(zigzag(value))).toBe(value);
    },
  );
});

describe("integers", () => {
  const boundaries = [
    0,
    127,
    -128,
    128,
    -129,
    32_767,
    -32_768,
    32_768,
    8_388_607,
    -8_388_608,
    8_388_608,
    2_147_483_647,
    -2_147_483_648,
    2_147_483_648,
    2 ** 47 - 1,
    -(2 ** 47),
    2 ** 47,
    Number.MAX_SAFE_INTEGER,
    Number.MIN_SAFE_INTEGER,
  ];

  it.each(boundaries)("round-trip %i in its smallest width", (value) => {
    const width = integerWidth(value);
    const writer = new ByteWriter();
    writer.integer(value, width);
    const bytes = writer.toBytes();
    expect(bytes).toHaveLength(width);
    expect(readInteger(new DataView(bytes.buffer), 0, width)).toBe(value);
  });

  it("pick widths at the two's complement limits", () => {
    const widths: IntegerWidth[] = [
      127,
      128,
      32_768,
      8_388_608,
      2 ** 31,
      2 ** 47,
    ].map((value) => integerWidth(value));
    expect(widths).toEqual([1, 2, 3, 4, 6, 8]);
  });
});
