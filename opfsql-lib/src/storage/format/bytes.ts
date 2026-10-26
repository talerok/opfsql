import { CorruptError } from "../../errors.js";

// ---------------------------------------------------------------------------
// Byte-level building blocks of file format 5 (docs/file-format.md): varints,
// big-endian integers, and a growable writer for encoding keys and records.
// ---------------------------------------------------------------------------

const TWO_32 = 2 ** 32;
const MAX_VARINT_BYTES = 8;

/** A big-endian view of exactly `bytes`. */
export function viewOf(bytes: Uint8Array): DataView {
  return new DataView(bytes.buffer, bytes.byteOffset, bytes.byteLength);
}

export function readUint32(bytes: Uint8Array, offset: number): number {
  return viewOf(bytes).getUint32(offset);
}

export function writeUint32(
  bytes: Uint8Array,
  offset: number,
  value: number,
): void {
  viewOf(bytes).setUint32(offset, value);
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

/** Maps a signed integer to an unsigned one, small magnitudes to small values. */
export function zigzag(value: number): number {
  return value >= 0 ? value * 2 : -value * 2 - 1;
}

export function unzigzag(value: number): number {
  const half = Math.floor(value / 2);
  return value % 2 === 0 ? half : -half - 1;
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
