import { CorruptError } from "../../errors.js";

// ---------------------------------------------------------------------------
// Byte-level building blocks of file format 5 (docs/file-format.md): varints,
// big-endian integers, and a growable writer for encoding keys and records.
// ---------------------------------------------------------------------------

const TWO_32 = 2 ** 32;
const MAX_VARINT_BYTES = 8;

/** Bytes an unsigned LEB128 varint of `value` takes. */
export function varintSize(value: number): number {
  let size = 1;
  for (let rest = value; rest >= 0x80; rest = Math.floor(rest / 0x80)) size++;
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
  let end = offset;
  while ((bytes[end] ?? 0) >= 0x80) end++;
  return end + 1;
}

/** Maps a signed integer to an unsigned one, small magnitudes to small values. */
export function zigzag(value: number): number {
  return value >= 0 ? value * 2 : -value * 2 - 1;
}

export function unzigzag(value: number): number {
  return (value % 2 === 0 ? value : -(value + 1)) / 2;
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

/** Fewest bytes of two's complement that hold `value`. */
export function integerWidth(value: number): IntegerWidth {
  if (value >= -0x80 && value < 0x80) return 1;
  if (value >= -0x80_00 && value < 0x80_00) return 2;
  if (value >= -0x80_00_00 && value < 0x80_00_00) return 3;
  if (value >= -0x80_00_00_00 && value < 0x80_00_00_00) return 4;
  return Math.abs(value) < 2 ** 47 ? 6 : 8;
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
    case 6:
    case 8: {
      const high = Math.floor(value / TWO_32);
      if (width === 6) view.setInt16(offset, high);
      else view.setInt32(offset, high);
      view.setUint32(offset + width - 4, value - high * TWO_32);
    }
  }
}

export function readInteger(
  view: DataView,
  offset: number,
  width: IntegerWidth,
): number {
  switch (width) {
    case 1: {
      return view.getInt8(offset);
    }
    case 2: {
      return view.getInt16(offset);
    }
    case 3: {
      return view.getInt8(offset) * 0x1_00_00 + view.getUint16(offset + 1);
    }
    case 4: {
      return view.getInt32(offset);
    }
    case 6:
    case 8: {
      const high = width === 6 ? view.getInt16(offset) : view.getInt32(offset);
      const value = high * TWO_32 + view.getUint32(offset + width - 4);
      if (!Number.isSafeInteger(value)) {
        throw new CorruptError(`integer at byte ${String(offset)} is not safe`);
      }
      return value;
    }
  }
}
