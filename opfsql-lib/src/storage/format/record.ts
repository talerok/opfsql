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
} from "./bytes.js";

// ---------------------------------------------------------------------------
// Records (docs/file-format.md, Records): a row's values in column order,
// behind a header of serial types, so one column decodes without the others.
// ---------------------------------------------------------------------------

export type OverflowKind = "text" | "blob" | "json";

/** A value whose bytes live in a chain of overflow pages. */
export class OverflowValue {
  constructor(
    readonly kind: OverflowKind,
    readonly length: number,
    readonly firstPage: number,
  ) {}
}

export type RecordField = Value | OverflowValue;

const SERIAL = {
  null: 0,
  false: 1,
  true: 2,
  zero: 3,
  one: 4,
  float: 11,
  overflow: { text: 12, blob: 13, json: 14 },
  inline: 16,
} as const;

const INTEGER_SERIAL: Record<IntegerWidth, number> = {
  1: 5,
  2: 6,
  3: 7,
  4: 8,
  6: 9,
  8: 10,
};

const WIDTH_OF_SERIAL: Readonly<Partial<Record<number, IntegerWidth>>> = {
  5: 1,
  6: 2,
  7: 3,
  8: 4,
  9: 6,
  10: 8,
};

const OVERFLOW_BODY_SIZE = 8;
const INLINE_KINDS: readonly OverflowKind[] = ["text", "blob", "json"];

const encoder = new TextEncoder();
const decoder = new TextDecoder();

/** A field ready to write: its serial type and the body that follows. */
export type PreparedField =
  | { readonly serialType: number; readonly body: "none" }
  | {
      readonly serialType: number;
      readonly body: "integer";
      readonly value: number;
      readonly width: IntegerWidth;
    }
  | {
      readonly serialType: number;
      readonly body: "float";
      readonly value: number;
    }
  | {
      readonly serialType: number;
      readonly body: "bytes";
      readonly kind: OverflowKind;
      readonly bytes: Uint8Array;
    }
  | {
      readonly serialType: number;
      readonly body: "overflow";
      readonly value: OverflowValue;
    };

export function prepareField(field: RecordField): PreparedField {
  if (field === null) return { serialType: SERIAL.null, body: "none" };
  if (typeof field === "boolean") {
    return { serialType: field ? SERIAL.true : SERIAL.false, body: "none" };
  }
  if (typeof field === "number") return prepareNumber(field);
  if (typeof field === "string")
    return inlineBytes("text", encoder.encode(field));
  if (field instanceof Uint8Array) return inlineBytes("blob", field);
  if (field instanceof OverflowValue) {
    return {
      serialType: SERIAL.overflow[field.kind],
      body: "overflow",
      value: field,
    };
  }
  return inlineBytes("json", encoder.encode(JSON.stringify(field)));
}

/** Bytes the prepared fields take as a record. */
export function recordSize(fields: readonly PreparedField[]): number {
  let header = 0;
  let body = 0;
  for (const field of fields) {
    header += varintSize(field.serialType);
    body += bodySize(field);
  }
  return headerSizeWith(header) + body;
}

export function encodeRecord(fields: readonly PreparedField[]): Uint8Array {
  const types = new ByteWriter();
  for (const field of fields) types.varint(field.serialType);

  const writer = new ByteWriter(recordSize(fields));
  writer.varint(headerSizeWith(types.length));
  writer.bytes(types.toBytes());
  for (const field of fields) writeBody(writer, field);
  return writer.toBytes();
}

/** Reads a record's fields one at a time, decoding only what is asked. */
export class RecordReader {
  private readonly serialTypes: number[] = [];
  private readonly offsets: number[] = [];
  private readonly view: DataView;

  constructor(private readonly bytes: Uint8Array) {
    this.view = new DataView(bytes.buffer, bytes.byteOffset, bytes.byteLength);
    const headerSize = readVarint(bytes, 0);
    let position = skipVarint(bytes, 0);
    let bodyOffset = headerSize;
    while (position < headerSize) {
      const serialType = readVarint(bytes, position);
      this.serialTypes.push(serialType);
      this.offsets.push(bodyOffset);
      bodyOffset += serialBodySize(serialType);
      position = skipVarint(bytes, position);
    }
    if (bodyOffset !== bytes.length) {
      throw new CorruptError("record body does not match its header");
    }
  }

  /** Number of fields stored; columns added later are not among them. */
  get length(): number {
    return this.serialTypes.length;
  }

  isOverflow(index: number): boolean {
    const serialType = this.serialType(index);
    return (
      serialType >= SERIAL.overflow.text && serialType <= SERIAL.overflow.json
    );
  }

  /** The value of field `index`, which must not be in overflow pages. */
  value(index: number): Value {
    const serialType = this.serialType(index);
    const offset = this.offsets[index] ?? 0;
    if (serialType >= SERIAL.inline)
      return this.inlineValue(serialType, offset);
    const width = WIDTH_OF_SERIAL[serialType];
    return width === undefined
      ? this.constantValue(serialType, offset)
      : readInteger(this.view, offset, width);
  }

  /** Where the bytes of field `index` live, when it is in overflow pages. */
  overflow(index: number): OverflowValue {
    const serialType = this.serialType(index);
    const kind = INLINE_KINDS[serialType - SERIAL.overflow.text];
    if (kind === undefined || !this.isOverflow(index)) {
      throw new CorruptError(`field ${String(index)} is not in overflow pages`);
    }
    const offset = this.offsets[index] ?? 0;
    return new OverflowValue(
      kind,
      this.view.getUint32(offset),
      this.view.getUint32(offset + 4),
    );
  }

  private serialType(index: number): number {
    const serialType = this.serialTypes[index];
    if (serialType === undefined) {
      throw new CorruptError(`record has no field ${String(index)}`);
    }
    return serialType;
  }

  private constantValue(serialType: number, offset: number): Value {
    switch (serialType) {
      case SERIAL.null: {
        return null;
      }
      case SERIAL.false:
      case SERIAL.true: {
        return serialType === SERIAL.true;
      }
      case SERIAL.zero:
      case SERIAL.one: {
        return serialType - SERIAL.zero;
      }
      case SERIAL.float: {
        return this.view.getFloat64(offset);
      }
      default: {
        throw new CorruptError(`unknown serial type ${String(serialType)}`);
      }
    }
  }

  private inlineValue(serialType: number, offset: number): Value {
    const kind = INLINE_KINDS[(serialType - SERIAL.inline) % 3];
    const bytes = this.bytes.subarray(
      offset,
      offset + serialBodySize(serialType),
    );
    return decodeBytes(kind ?? "blob", bytes);
  }
}

/** The value overflow bytes hold, once read back from their pages. */
export function decodeBytes(kind: OverflowKind, bytes: Uint8Array): Value {
  switch (kind) {
    case "blob": {
      return Uint8Array.from(bytes);
    }
    case "text": {
      return decoder.decode(bytes);
    }
    case "json": {
      return JSON.parse(decoder.decode(bytes)) as JsonValue;
    }
  }
}

// --- Sizes ---------------------------------------------------------------------

export function serialBodySize(serialType: number): number {
  if (serialType >= SERIAL.inline)
    return Math.floor((serialType - SERIAL.inline) / 3);
  if (
    serialType >= SERIAL.overflow.text &&
    serialType <= SERIAL.overflow.json
  ) {
    return OVERFLOW_BODY_SIZE;
  }
  if (serialType === SERIAL.float) return 8;
  const width = WIDTH_OF_SERIAL[serialType];
  if (width !== undefined) return width;
  if (serialType <= SERIAL.one) return 0;
  throw new CorruptError(`unknown serial type ${String(serialType)}`);
}

function bodySize(field: PreparedField): number {
  switch (field.body) {
    case "none": {
      return 0;
    }
    case "integer": {
      return field.width;
    }
    case "float":
    case "overflow": {
      return 8;
    }
    case "bytes": {
      return field.bytes.length;
    }
  }
}

/** Header size counting the varint that states it. */
function headerSizeWith(serialTypesSize: number): number {
  let size = serialTypesSize + 1;
  while (varintSize(size) + serialTypesSize !== size) {
    size = serialTypesSize + varintSize(size);
  }
  return size;
}

// --- Encoding ------------------------------------------------------------------

function prepareNumber(value: number): PreparedField {
  if (!Number.isSafeInteger(value)) {
    return { serialType: SERIAL.float, body: "float", value };
  }
  if (value === 0 || value === 1) {
    return { serialType: SERIAL.zero + value, body: "none" };
  }
  const width = integerWidth(value);
  return { serialType: INTEGER_SERIAL[width], body: "integer", value, width };
}

function inlineBytes(kind: OverflowKind, bytes: Uint8Array): PreparedField {
  return {
    serialType: SERIAL.inline + bytes.length * 3 + INLINE_KINDS.indexOf(kind),
    body: "bytes",
    kind,
    bytes,
  };
}

function writeBody(writer: ByteWriter, field: PreparedField): void {
  switch (field.body) {
    case "none": {
      return;
    }
    case "integer": {
      writer.integer(field.value, field.width);
      return;
    }
    case "float": {
      writer.float64(field.value);
      return;
    }
    case "bytes": {
      writer.bytes(field.bytes);
      return;
    }
    case "overflow": {
      writer.uint32(field.value.length);
      writer.uint32(field.value.firstPage);
    }
  }
}
