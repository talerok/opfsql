import { describe, expect, it } from "vitest";
import { CorruptError } from "../../../errors.js";
import type { IntegerWidth } from "../bytes.js";
import {
  ByteWriter,
  integerWidth,
  readInteger,
  readVarint,
  readZigzagVarint,
  skipVarint,
  varintSize,
  zigzagVarintSize,
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

  // Past ±2^52 the zigzag value is past what a double holds exactly; a
  // magnitude that is a multiple of 64 borrows from the bits above.
  it.each([
    0,
    1,
    -1,
    63,
    -64,
    -128,
    2 ** 52,
    -(2 ** 52),
    -(2 ** 52) - 1,
    -(2 ** 53) + 64,
    Number.MAX_SAFE_INTEGER,
    Number.MIN_SAFE_INTEGER,
  ])("zigzag %i into the bytes the format says, and back", (value) => {
    const writer = new ByteWriter(1);
    writer.zigzagVarint(value);
    const bytes = writer.toBytes();
    expect([...bytes]).toEqual(zigzagReference(value));
    expect(bytes).toHaveLength(zigzagVarintSize(value));
    expect(readZigzagVarint(bytes, 0)).toBe(value);
    expect(skipVarint(bytes, 0)).toBe(bytes.length);
  });
});

/** The zigzag varint of docs/file-format.md, worked out exactly. */
function zigzagReference(value: number): number[] {
  const x = BigInt(value);
  let rest = x >= 0n ? 2n * x : -2n * x - 1n;
  const bytes: number[] = [];
  while (rest >= 0x80n) {
    bytes.push(Number(rest % 0x80n) | 0x80);
    rest /= 0x80n;
  }
  return [...bytes, Number(rest)];
}

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
    expect(readInteger(bytes, 0, width)).toBe(value);
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
