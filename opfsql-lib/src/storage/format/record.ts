import { CorruptError } from "../../errors.js";
import type { JsonValue, Value } from "../../values/index.js";
import type { IntegerWidth } from "./bytes.js";
import {
  ByteWriter,
  integerWidth,
  readInteger,
  readVarint,
  skipVarint,
  varintSize,
  viewOf,
} from "./bytes.js";

// ---------------------------------------------------------------------------
// Records (docs/file-format.md, Records): a row's values in column order,
// behind a header of serial types, so one column decodes without the others.
//
// A serial type is a number standing for a field's shape: what it holds and
// how many bytes. shapeOfSerialType() and serialTypeOf() are the only places that know
// the numbers, and bodySize() the only one that knows the sizes; everything
// else works with shapes.
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

interface FieldEntry {
  readonly shape: Shape;
  /** Where the field's body starts in the record. */
  readonly offset: number;
}

/** Reads a record's fields one at a time, decoding only what is asked. */
export class RecordReader {
  private readonly entries: FieldEntry[] = [];
  private readonly view: DataView;

  constructor(private readonly bytes: Uint8Array) {
    this.view = viewOf(bytes);
    const headerSize = readVarint(bytes, 0);
    let position = skipVarint(bytes, 0);
    let offset = headerSize;
    while (position < headerSize) {
      const shape = shapeOfSerialType(readVarint(bytes, position));
      this.entries.push({ shape, offset });
      offset += bodySize(shape);
      position = skipVarint(bytes, position);
    }
    if (offset !== bytes.length) {
      throw new CorruptError("record body does not match its header");
    }
  }

  /** Number of fields stored; columns added later are not among them. */
  get length(): number {
    return this.entries.length;
  }

  isOverflow(index: number): boolean {
    return this.entry(index).shape.kind === "overflow";
  }

  /** The value of field `index`, which must not be in overflow pages. */
  value(index: number): Value {
    const { shape, offset } = this.entry(index);
    switch (shape.kind) {
      case "constant": {
        return shape.value;
      }
      case "integer": {
        return readInteger(this.view, offset, shape.width);
      }
      case "float": {
        return this.view.getFloat64(offset);
      }
      case "bytes": {
        const bytes = this.bytes.subarray(offset, offset + shape.length);
        return decodeBytes(shape.holds, bytes);
      }
      case "overflow": {
        throw new CorruptError(`field ${String(index)} is in overflow pages`);
      }
    }
  }

  /** Where the bytes of field `index` live, when it is in overflow pages. */
  overflow(index: number): OverflowValue {
    const { shape, offset } = this.entry(index);
    if (shape.kind !== "overflow") {
      throw new CorruptError(`field ${String(index)} is not in overflow pages`);
    }
    const length = this.view.getUint32(offset);
    const firstPage = this.view.getUint32(offset + 4);
    return new OverflowValue(shape.holds, length, firstPage);
  }

  private entry(index: number): FieldEntry {
    const entry = this.entries[index];
    if (entry === undefined) {
      throw new CorruptError(`record has no field ${String(index)}`);
    }
    return entry;
  }
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

function shapeOfSerialType(serialType: number): Shape {
  if (serialType < FIRST_INTEGER) {
    return { kind: "constant", value: CONSTANTS[serialType] ?? null };
  }
  if (serialType < FLOAT) {
    const width = INTEGER_WIDTHS[serialType - FIRST_INTEGER] ?? 8;
    return { kind: "integer", width };
  }
  if (serialType === FLOAT) return { kind: "float" };
  if (serialType < RESERVED) {
    return { kind: "overflow", holds: byteKind(serialType - FIRST_OVERFLOW) };
  }
  if (serialType === RESERVED) {
    throw new CorruptError(`serial type ${String(RESERVED)} is reserved`);
  }
  const inline = serialType - FIRST_INLINE;
  const length = Math.floor(inline / BYTE_KINDS.length);
  const holds = byteKind(inline % BYTE_KINDS.length);
  return { kind: "bytes", holds, length };
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
