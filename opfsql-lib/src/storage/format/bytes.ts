import type { JsonValue } from "../../values/index.js";
import { CorruptError } from "../../errors.js";

// ---------------------------------------------------------------------------
// Byte-level building blocks of file format 5 (docs/file-format.md): varints,
// big-endian integers, a growable writer for encoding keys and records, and
// the parsing of the JSON text both store.
// ---------------------------------------------------------------------------

const TWO_32 = 2 ** 32;
const MAX_VARINT_BYTES = 8;

/** A big-endian view of exactly `bytes`. */
export function viewOf(bytes: Uint8Array): DataView {
  return new DataView(bytes.buffer, bytes.byteOffset, bytes.byteLength);
}

// Big-endian u32s are read and written byte by byte: they are read on every
// page and header, and a DataView for each would cost more than the read.

export function readUint32(bytes: Uint8Array, offset: number): number {
  const high = (bytes[offset] ?? 0) * 0x1_00_00_00;
  const rest =
    ((bytes[offset + 1] ?? 0) << 16) |
    ((bytes[offset + 2] ?? 0) << 8) |
    (bytes[offset + 3] ?? 0);
  return high + rest;
}

export function writeUint32(
  bytes: Uint8Array,
  offset: number,
  value: number,
): void {
  bytes[offset] = value >>> 24;
  bytes[offset + 1] = (value >>> 16) & 0xff;
  bytes[offset + 2] = (value >>> 8) & 0xff;
  bytes[offset + 3] = value & 0xff;
}

/** Bytes an unsigned LEB128 varint of `value` takes. */
export function varintSize(value: number): number {
  let size = 1;
  let rest = value;
  while (rest >= 0x80) {
    rest = Math.floor(rest / 0x80);
    size++;
  }
  return size;
}

/** Reads the unsigned varint at `offset`. */
export function readVarint(bytes: Uint8Array, offset: number): number {
  let value = 0;
  let scale = 1;
  for (let index = 0; index < MAX_VARINT_BYTES; index++) {
    const byte = bytes[offset + index];
    if (byte === undefined) break;
    value += (byte & 0x7f) * scale;
    if (byte < 0x80) return value;
    scale *= 0x80;
  }
  throw new CorruptError(`unterminated varint at byte ${String(offset)}`);
}

/** Offset just past the varint at `offset`. */
export function skipVarint(bytes: Uint8Array, offset: number): number {
  for (let index = 0; index < MAX_VARINT_BYTES; index++) {
    const byte = bytes[offset + index];
    if (byte === undefined) break;
    if (byte < 0x80) return offset + index + 1;
  }
  throw new CorruptError(`unterminated varint at byte ${String(offset)}`);
}

// A zigzag varint (docs/file-format.md) writes `2x` for x ≥ 0 and `−2x − 1`
// otherwise. For |x| near 2^53 that value is odd and above 2^53, so a double
// cannot hold it, and it is never formed: the first byte — the sign in its
// low bit, and six bits of the magnitude — is worked out from the magnitude,
// and the rest is the plain varint of what lies above it.

/** The zigzag value of `value` as its low seven bits and the integer above them. */
function zigzagGroups(value: number): { low: number; high: number } {
  const isNegative = value < 0;
  const magnitude = Math.abs(value);
  // 2·magnitude − sign = 128·quotient + (2·remainder − sign).
  const quotient = Math.floor(magnitude / 64);
  const low = 2 * (magnitude % 64) - (isNegative ? 1 : 0);
  return low < 0
    ? { low: low + 0x80, high: quotient - 1 }
    : { low, high: quotient };
}

/** Bytes the zigzag varint of `value` takes. */
export function zigzagVarintSize(value: number): number {
  const { high } = zigzagGroups(value);
  return high === 0 ? 1 : 1 + varintSize(high);
}

/** Reads the zigzag varint at `offset`: a safe integer, whatever its sign. */
export function readZigzagVarint(bytes: Uint8Array, offset: number): number {
  const first = bytes[offset] ?? 0;
  const low = first & 0x7f;
  const high = first < 0x80 ? 0 : readVarint(bytes, offset + 1);
  const sign = low & 1;
  // 128·high + low = 2·magnitude − sign.
  const magnitude = high * 64 + (low + sign) / 2;
  return sign === 1 ? -magnitude : magnitude;
}

/** Appends bytes to a buffer that grows as needed. */
export class ByteWriter {
  private buffer: Uint8Array;
  private view: DataView;
  private size = 0;

  constructor(capacity = 64) {
    this.buffer = new Uint8Array(capacity);
    this.view = new DataView(this.buffer.buffer);
  }

  get length(): number {
    return this.size;
  }

  /** The bytes written, copied out of the buffer. */
  toBytes(): Uint8Array {
    return this.buffer.slice(0, this.size);
  }

  byte(value: number): void {
    this.reserve(1);
    this.buffer[this.size++] = value;
  }

  bytes(value: Uint8Array): void {
    this.reserve(value.length);
    this.buffer.set(value, this.size);
    this.size += value.length;
  }

  varint(value: number): void {
    this.reserve(MAX_VARINT_BYTES);
    let rest = value;
    while (rest >= 0x80) {
      this.buffer[this.size++] = (rest % 0x80) | 0x80;
      rest = Math.floor(rest / 0x80);
    }
    this.buffer[this.size++] = rest;
  }

  zigzagVarint(value: number): void {
    const { low, high } = zigzagGroups(value);
    if (high === 0) {
      this.byte(low);
      return;
    }
    this.byte(low | 0x80);
    this.varint(high);
  }

  uint32(value: number): void {
    this.reserve(4);
    this.view.setUint32(this.size, value);
    this.size += 4;
  }

  float64(value: number): void {
    this.reserve(8);
    this.view.setFloat64(this.size, value);
    this.size += 8;
  }

  /** A safe integer in `width` bytes of big-endian two's complement. */
  integer(value: number, width: IntegerWidth): void {
    this.reserve(width);
    writeInteger(this.view, this.size, value, width);
    this.size += width;
  }

  private reserve(extra: number): void {
    const needed = this.size + extra;
    if (needed <= this.buffer.length) return;
    const grown = new Uint8Array(Math.max(needed, this.buffer.length * 2));
    grown.set(this.buffer.subarray(0, this.size));
    this.buffer = grown;
    this.view = new DataView(grown.buffer);
  }
}

export type IntegerWidth = 1 | 2 | 3 | 4 | 6 | 8;

/** Each width but the widest, and the magnitude its values stay below. */
const NARROW_WIDTHS: readonly (readonly [IntegerWidth, number])[] = [
  [1, 2 ** 7],
  [2, 2 ** 15],
  [3, 2 ** 23],
  [4, 2 ** 31],
  [6, 2 ** 47],
];

/** Fewest bytes of two's complement that hold `value`. */
export function integerWidth(value: number): IntegerWidth {
  for (const [width, limit] of NARROW_WIDTHS) {
    if (value >= -limit && value < limit) return width;
  }
  return 8;
}

export function writeInteger(
  view: DataView,
  offset: number,
  value: number,
  width: IntegerWidth,
): void {
  switch (width) {
    case 1: {
      view.setInt8(offset, value);
      return;
    }
    case 2: {
      view.setInt16(offset, value);
      return;
    }
    case 3: {
      view.setInt8(offset, Math.floor(value / 0x1_00_00));
      view.setUint16(offset + 1, value & 0xff_ff);
      return;
    }
    case 4: {
      view.setInt32(offset, value);
      return;
    }
    case 6: {
      view.setInt16(offset, highPart(value));
      view.setUint32(offset + 2, lowPart(value));
      return;
    }
    case 8: {
      view.setInt32(offset, highPart(value));
      view.setUint32(offset + 4, lowPart(value));
    }
  }
}

/** The safe integer in `width` bytes of big-endian two's complement at `offset`. */
export function readInteger(
  bytes: Uint8Array,
  offset: number,
  width: IntegerWidth,
): number {
  // The first byte carries the sign; each next one is a base-256 digit.
  let value = ((bytes[offset] ?? 0) << 24) >> 24;
  for (let index = 1; index < width; index++) {
    value = value * 0x1_00 + (bytes[offset + index] ?? 0);
  }
  if (!Number.isSafeInteger(value)) {
    throw new CorruptError(`integer at byte ${String(offset)} is not safe`);
  }
  return value;
}

/** One double, read through these bytes. */
const doubleView = new DataView(new ArrayBuffer(8));

/** The big-endian double at `offset`. */
export function readFloat64(bytes: Uint8Array, offset: number): number {
  for (let index = 0; index < 8; index++) {
    doubleView.setUint8(index, bytes[offset + index] ?? 0);
  }
  return doubleView.getFloat64(0);
}

/** The signed upper part of a wide integer, above its low 32 bits. */
function highPart(value: number): number {
  return Math.floor(value / TWO_32);
}

function lowPart(value: number): number {
  return value - highPart(value) * TWO_32;
}

/** JSON text that opfsql stored: text that does not parse means damaged files. */
export function parseStoredJson(text: string): JsonValue {
  try {
    return JSON.parse(text) as JsonValue;
  } catch (error) {
    throw new CorruptError("a stored JSON value does not parse", {
      cause: error,
    });
  }
}
