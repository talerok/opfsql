import { CorruptError } from "../../errors.js";
import type { JsonValue, Value } from "../../values/index.js";
import type { IntegerWidth } from "./bytes.js";
import {
  ByteWriter,
  integerWidth,
  readFloat64,
  readInteger,
  readUint32,
  readVarint,
  skipVarint,
  varintSize,
} from "./bytes.js";

// ---------------------------------------------------------------------------
// Records (docs/file-format.md, Records): a row's values in column order,
// behind a header of serial types, so one column decodes without the others.
//
// A serial type is a number standing for a field's shape: what it holds and
// how many bytes. Writing works with shapes, which serialTypeOf() turns into
// numbers; reading works on the numbers, through the functions of the
// "Serial types" section, which read them back.
// ---------------------------------------------------------------------------

export type ByteKind = "text" | "blob" | "json";

/** A value whose bytes live in a chain of overflow pages. */
export class OverflowValue {
  constructor(
    readonly holds: ByteKind,
    readonly length: number,
    readonly firstPage: number,
  ) {}
}

export type RecordField = Value | OverflowValue;

/** A field's shape, as its serial type says. */
type Shape =
  | { readonly kind: "constant"; readonly value: null | boolean | 0 | 1 }
  | { readonly kind: "integer"; readonly width: IntegerWidth }
  | { readonly kind: "float" }
  | {
      readonly kind: "bytes";
      readonly holds: ByteKind;
      readonly length: number;
    }
  | { readonly kind: "overflow"; readonly holds: ByteKind };

/** A field ready to write: its shape's kind, and its value. */
export type PreparedField =
  | { readonly kind: "constant"; readonly value: null | boolean | 0 | 1 }
  | {
      readonly kind: "integer";
      readonly width: IntegerWidth;
      readonly value: number;
    }
  | { readonly kind: "float"; readonly value: number }
  | {
      readonly kind: "bytes";
      readonly holds: ByteKind;
      readonly bytes: Uint8Array;
    }
  | { readonly kind: "overflow"; readonly reference: OverflowValue };

// Serial types, as docs/file-format.md lists them.
const CONSTANTS = [null, false, true, 0, 1] as const;
const INTEGER_WIDTHS: readonly IntegerWidth[] = [1, 2, 3, 4, 6, 8];
const FIRST_INTEGER = 5;
const FLOAT = 11;
const FIRST_OVERFLOW = 12;
const RESERVED = 15;
const FIRST_INLINE = 16;
const BYTE_KINDS: readonly ByteKind[] = ["text", "blob", "json"];

const FLOAT_SIZE = 8;
/** Bytes a field in overflow pages takes in its record. */
export const OVERFLOW_REFERENCE_SIZE = 8;

const encoder = new TextEncoder();
const decoder = new TextDecoder();
/** Text up to this many bytes is read by hand when it is ASCII. */
const SHORT_TEXT = 32;
const FIRST_NON_ASCII = 0x80;

// --- Writing -------------------------------------------------------------------

export function prepareField(field: RecordField): PreparedField {
  if (field === null || typeof field === "boolean") {
    return { kind: "constant", value: field };
  }
  if (typeof field === "number") return prepareNumber(field);
  if (typeof field === "string") {
    return { kind: "bytes", holds: "text", bytes: encoder.encode(field) };
  }
  if (field instanceof Uint8Array) {
    return { kind: "bytes", holds: "blob", bytes: field };
  }
  if (field instanceof OverflowValue) {
    return { kind: "overflow", reference: field };
  }
  const json = encoder.encode(JSON.stringify(field));
  return { kind: "bytes", holds: "json", bytes: json };
}

/** Bytes the prepared fields take as a record. */
export function recordSize(fields: readonly PreparedField[]): number {
  const shapes = fields.map((field) => shapeOf(field));
  const typesSize = sum(shapes, (shape) => varintSize(serialTypeOf(shape)));
  return headerSizeFor(typesSize) + sum(shapes, bodySize);
}

export function encodeRecord(fields: readonly PreparedField[]): Uint8Array {
  const types = new ByteWriter();
  for (const field of fields) types.varint(serialTypeOf(shapeOf(field)));

  const record = new ByteWriter(recordSize(fields));
  record.varint(headerSizeFor(types.length));
  record.bytes(types.toBytes());
  for (const field of fields) writeBody(record, field);
  return record.toBytes();
}

function prepareNumber(value: number): PreparedField {
  if (value === 0 || value === 1) return { kind: "constant", value };
  if (!Number.isSafeInteger(value)) return { kind: "float", value };
  return { kind: "integer", width: integerWidth(value), value };
}

function shapeOf(field: PreparedField): Shape {
  switch (field.kind) {
    case "constant": {
      return field;
    }
    case "integer": {
      return { kind: "integer", width: field.width };
    }
    case "float": {
      return { kind: "float" };
    }
    case "bytes": {
      return { kind: "bytes", holds: field.holds, length: field.bytes.length };
    }
    case "overflow": {
      return { kind: "overflow", holds: field.reference.holds };
    }
  }
}

function writeBody(record: ByteWriter, field: PreparedField): void {
  switch (field.kind) {
    case "constant": {
      return;
    }
    case "integer": {
      record.integer(field.value, field.width);
      return;
    }
    case "float": {
      record.float64(field.value);
      return;
    }
    case "bytes": {
      record.bytes(field.bytes);
      return;
    }
    case "overflow": {
      record.uint32(field.reference.length);
      record.uint32(field.reference.firstPage);
    }
  }
}

// --- Reading -------------------------------------------------------------------

/** Where each field of a record goes, by field number; undefined to skip it. */
export type FieldPlaces = readonly (number | undefined)[];

/** Every field of a record, in field order. */
export function decodeRecord(record: Uint8Array): RecordField[] {
  const fields: RecordField[] = [];
  decodeInto(record, null, fields);
  return fields;
}

/**
 * Decodes a record into `target`, in one pass over its serial types and
 * their bodies: field i to position `places[i]`, or i without places; a
 * field without a place has its body skipped. A field in overflow pages
 * goes as the OverflowValue that says where its bytes are, and then the
 * result is true: such a value is still to read.
 */
export function decodeInto(
  record: Uint8Array,
  places: FieldPlaces | null,
  target: RecordField[],
): boolean {
  const headerSize = readVarint(record, 0);
  let position = skipVarint(record, 0);
  let offset = headerSize;
  let field = 0;
  let hasOverflow = false;
  while (position < headerSize) {
    const serialType = readVarint(record, position);
    // Sized before it is read, so that a reserved type fails as itself.
    const size = bodySizeOf(serialType);
    const place = places === null ? field : places[field];
    if (place !== undefined) {
      target[place] = fieldAt(record, serialType, offset);
      hasOverflow ||= isOverflowType(serialType);
    }
    offset += size;
    position = skipVarint(record, position);
    field++;
  }
  if (offset !== record.length) {
    throw new CorruptError("record body does not match its header");
  }
  return hasOverflow;
}

/** The field of `serialType` whose body starts at `offset`. */
function fieldAt(
  record: Uint8Array,
  serialType: number,
  offset: number,
): RecordField {
  if (serialType < FIRST_INTEGER) return CONSTANTS[serialType] ?? null;
  if (serialType < FLOAT) {
    return readInteger(record, offset, integerWidthOf(serialType));
  }
  if (serialType === FLOAT) return readFloat64(record, offset);
  if (serialType < FIRST_INLINE) return overflowAt(record, serialType, offset);
  const end = offset + inlineLengthOf(serialType);
  const kind = inlineKindOf(serialType);
  if (kind === "text") return textAt(record, offset, end);
  return decodeBytes(kind, record.subarray(offset, end));
}

/**
 * The text from `start` to `end`. Short ASCII text, as most column values
 * are, is read by hand, with no view of its bytes and no call into
 * TextDecoder; anything else goes to TextDecoder.
 */
function textAt(record: Uint8Array, start: number, end: number): string {
  if (end - start > SHORT_TEXT)
    return decoder.decode(record.subarray(start, end));
  let text = "";
  for (let index = start; index < end; index++) {
    const byte = record[index] ?? 0;
    if (byte >= FIRST_NON_ASCII)
      return decoder.decode(record.subarray(start, end));
    text += String.fromCodePoint(byte);
  }
  return text;
}

/** Where the bytes of an overflow field are, as its reference says. */
function overflowAt(
  record: Uint8Array,
  serialType: number,
  offset: number,
): OverflowValue {
  const holds = byteKind(serialType - FIRST_OVERFLOW);
  const length = readUint32(record, offset);
  const firstPage = readUint32(record, offset + 4);
  return new OverflowValue(holds, length, firstPage);
}

/** The value a TEXT, BLOB or JSON field's bytes hold, inline or read back from overflow pages. */
export function decodeBytes(kind: ByteKind, bytes: Uint8Array): Value {
  switch (kind) {
    case "blob": {
      return bytes.slice();
    }
    case "text": {
      return decoder.decode(bytes);
    }
    case "json": {
      return JSON.parse(decoder.decode(bytes)) as JsonValue;
    }
  }
}

// --- Serial types ----------------------------------------------------------------

function serialTypeOf(shape: Shape): number {
  switch (shape.kind) {
    case "constant": {
      return CONSTANTS.indexOf(shape.value);
    }
    case "integer": {
      return FIRST_INTEGER + INTEGER_WIDTHS.indexOf(shape.width);
    }
    case "float": {
      return FLOAT;
    }
    case "bytes": {
      const kind = BYTE_KINDS.indexOf(shape.holds);
      return FIRST_INLINE + shape.length * BYTE_KINDS.length + kind;
    }
    case "overflow": {
      return FIRST_OVERFLOW + BYTE_KINDS.indexOf(shape.holds);
    }
  }
}

/** Bytes the body of a field of `serialType` takes. */
function bodySizeOf(serialType: number): number {
  if (serialType < FIRST_INTEGER) return 0;
  if (serialType < FLOAT) return integerWidthOf(serialType);
  if (serialType === FLOAT) return FLOAT_SIZE;
  if (serialType < RESERVED) return OVERFLOW_REFERENCE_SIZE;
  if (serialType === RESERVED) {
    throw new CorruptError(`serial type ${String(RESERVED)} is reserved`);
  }
  return inlineLengthOf(serialType);
}

function isOverflowType(serialType: number): boolean {
  return serialType >= FIRST_OVERFLOW && serialType < RESERVED;
}

function integerWidthOf(serialType: number): IntegerWidth {
  return INTEGER_WIDTHS[serialType - FIRST_INTEGER] ?? 8;
}

function inlineLengthOf(serialType: number): number {
  return Math.floor((serialType - FIRST_INLINE) / BYTE_KINDS.length);
}

function inlineKindOf(serialType: number): ByteKind {
  return byteKind((serialType - FIRST_INLINE) % BYTE_KINDS.length);
}

function bodySize(shape: Shape): number {
  switch (shape.kind) {
    case "constant": {
      return 0;
    }
    case "integer": {
      return shape.width;
    }
    case "float": {
      return FLOAT_SIZE;
    }
    case "bytes": {
      return shape.length;
    }
    case "overflow": {
      return OVERFLOW_REFERENCE_SIZE;
    }
  }
}

function byteKind(index: number): ByteKind {
  const kind = BYTE_KINDS[index];
  if (kind === undefined) throw new CorruptError("unknown byte kind");
  return kind;
}

/** Header size: the serial types, and the varint stating the size itself. */
function headerSizeFor(typesSize: number): number {
  let size = typesSize + 1;
  while (typesSize + varintSize(size) !== size) {
    size = typesSize + varintSize(size);
  }
  return size;
}

function sum<T>(items: readonly T[], size: (item: T) => number): number {
  let total = 0;
  for (const item of items) total += size(item);
  return total;
}
