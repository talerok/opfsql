import { CorruptError } from "../../errors.js";
import type { JsonValue, Value } from "../../values/index.js";
import { canonicalJson } from "../../values/index.js";
import { ByteWriter } from "./bytes.js";

// ---------------------------------------------------------------------------
// Index keys (docs/file-format.md, Index keys): values encoded so that
// comparing two keys byte by byte gives the order of docs/semantics.md.
// Each value starts with a tag in kind order and ends where it is complete,
// so a key is its values written one after another.
// ---------------------------------------------------------------------------

const TAG = {
  null: 0x10,
  false: 0x20,
  true: 0x21,
  number: 0x30,
  text: 0x40,
  blob: 0x50,
  json: 0x60,
} as const;

const TERMINATOR = 0x00;
const ESCAPED_ZERO = 0xff;
const NUMBER_SIZE = 9;

const encoder = new TextEncoder();
const decoder = new TextDecoder();

// One double, seen as a number and as its bytes.
const scratchBytes = new Uint8Array(8);
const scratch = new DataView(scratchBytes.buffer);

export function encodeKey(values: readonly Value[]): Uint8Array {
  const writer = new ByteWriter();
  for (const value of values) writeValue(writer, value);
  return writer.toBytes();
}

/** The first key after every key that starts with `prefix`. */
export function prefixEnd(prefix: Uint8Array): Uint8Array {
  const end = new Uint8Array(prefix.length + 1);
  end.set(prefix);
  end[prefix.length] = 0xff;
  return end;
}

/** The rowid an index entry ends with. */
export function keyRowid(key: Uint8Array): number {
  return readNumber(key, key.length - NUMBER_SIZE + 1);
}

export function decodeKey(key: Uint8Array): Value[] {
  const values: Value[] = [];
  let offset = 0;
  while (offset < key.length) {
    const [value, next] = readValue(key, offset);
    values.push(value);
    offset = next;
  }
  return values;
}

// --- Encoding ----------------------------------------------------------------

function writeValue(writer: ByteWriter, value: Value): void {
  if (value === null) {
    writer.byte(TAG.null);
  } else if (typeof value === "boolean") {
    writer.byte(value ? TAG.true : TAG.false);
  } else if (typeof value === "number") {
    writer.byte(TAG.number);
    writer.bytes(sortableNumber(value));
  } else if (typeof value === "string") {
    writeEscaped(writer, TAG.text, encoder.encode(value));
  } else if (value instanceof Uint8Array) {
    writeEscaped(writer, TAG.blob, value);
  } else {
    writeEscaped(writer, TAG.json, encoder.encode(canonicalJson(value)));
  }
}

/**
 * The double's bits, big-endian, with the sign bit flipped for zero and
 * positive numbers and every bit flipped for negative ones: then unsigned
 * byte order is numeric order.
 */
function sortableNumber(value: number): Uint8Array {
  scratch.setFloat64(0, value === 0 ? 0 : value);
  flipScratch((scratchBytes[0] ?? 0) >= 0x80);
  return scratchBytes;
}

/** Flips every bit of the scratch double when asked, else its sign bit. */
function flipScratch(isNegative: boolean): void {
  if (!isNegative) {
    scratchBytes[0] = (scratchBytes[0] ?? 0) ^ 0x80;
    return;
  }
  for (let index = 0; index < scratchBytes.length; index++) {
    scratchBytes[index] = (scratchBytes[index] ?? 0) ^ 0xff;
  }
}

function writeEscaped(
  writer: ByteWriter,
  tag: number,
  bytes: Uint8Array,
): void {
  writer.byte(tag);
  writer.bytes(escapeZeros(bytes));
  writer.byte(TERMINATOR);
}

/** `bytes` with every zero written as 0x00 0xFF, so 0x00 alone ends a value. */
function escapeZeros(bytes: Uint8Array): Uint8Array {
  if (!bytes.includes(TERMINATOR)) return bytes;
  const escaped: number[] = [];
  for (const byte of bytes) {
    escaped.push(byte);
    if (byte === TERMINATOR) escaped.push(ESCAPED_ZERO);
  }
  return Uint8Array.from(escaped);
}

// --- Decoding ----------------------------------------------------------------

function readValue(key: Uint8Array, offset: number): [Value, number] {
  const tag = key[offset];
  if (tag === undefined) throw new CorruptError("key ends inside a value");
  switch (tag) {
    case TAG.null: {
      return [null, offset + 1];
    }
    case TAG.false:
    case TAG.true: {
      return [tag === TAG.true, offset + 1];
    }
    case TAG.number: {
      return [readNumber(key, offset + 1), offset + NUMBER_SIZE];
    }
    case TAG.text:
    case TAG.blob:
    case TAG.json: {
      return readEscaped(key, tag, offset + 1);
    }
    default: {
      throw new CorruptError(`unknown key tag at byte ${String(offset)}`);
    }
  }
}

function readNumber(key: Uint8Array, offset: number): number {
  if (offset < 0 || offset + 8 > key.length) {
    throw new CorruptError("truncated number in key");
  }
  scratchBytes.set(key.subarray(offset, offset + 8));
  // An encoded negative number starts below 0x80: its sign bit was flipped to 0.
  flipScratch((scratchBytes[0] ?? 0) < 0x80);
  return scratch.getFloat64(0);
}

function readEscaped(
  key: Uint8Array,
  tag: typeof TAG.text | typeof TAG.blob | typeof TAG.json,
  start: number,
): [Value, number] {
  const { bytes, end } = unescapeZeros(key, start);
  if (tag === TAG.blob) return [bytes, end];
  const text = decoder.decode(bytes);
  return [tag === TAG.text ? text : (JSON.parse(text) as JsonValue), end];
}

/** The escaped bytes from `start` to their terminator, and where the value ends. */
function unescapeZeros(
  key: Uint8Array,
  start: number,
): { bytes: Uint8Array; end: number } {
  const bytes: number[] = [];
  let offset = start;
  while (!isTerminatorAt(key, offset)) {
    const byte = key[offset] ?? TERMINATOR;
    bytes.push(byte);
    // An escaped zero takes two bytes.
    offset += byte === TERMINATOR ? 2 : 1;
  }
  return { bytes: Uint8Array.from(bytes), end: offset + 1 };
}

function isTerminatorAt(key: Uint8Array, offset: number): boolean {
  if (offset >= key.length) throw new CorruptError("unterminated key value");
  return key[offset] === TERMINATOR && key[offset + 1] !== ESCAPED_ZERO;
}
