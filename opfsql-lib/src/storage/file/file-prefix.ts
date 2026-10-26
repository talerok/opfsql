import { CorruptError, UnsupportedFormatError } from "../../errors.js";
import { readUint32, writeUint32 } from "../format/bytes.js";

// ---------------------------------------------------------------------------
// Every file of a database starts with 8 ASCII bytes naming its kind and the
// format version as a u32 (docs/file-format.md). The version covers all three
// files: they change format together.
// ---------------------------------------------------------------------------

export const FORMAT_VERSION = 5;
export const PREFIX_SIZE = 12;

export type FileKind = "database" | "wal" | "shm";

const MAGIC: Record<FileKind, string> = {
  database: "OPFSQLDB",
  wal: "OPFSQLWL",
  shm: "OPFSQLSM",
};

/** Until format 3, the version was the magic's last two characters. */
const LEGACY_MAGIC = /^OPFSQL(\d\d)$/u;

const MAGIC_SIZE = 8;
const VERSION_OFFSET = MAGIC_SIZE;

/** What the start of a file says about it. */
type PrefixState =
  | { readonly kind: "current" }
  | { readonly kind: "other-version"; readonly version: number }
  | { readonly kind: "foreign" };

const encoder = new TextEncoder();
const decoder = new TextDecoder();

/** Each kind's magic as the bytes a file starts with. */
const MAGIC_BYTES: Record<FileKind, Uint8Array> = {
  database: encoder.encode(MAGIC.database),
  wal: encoder.encode(MAGIC.wal),
  shm: encoder.encode(MAGIC.shm),
};

export function writePrefix(header: Uint8Array, kind: FileKind): void {
  header.set(encoder.encode(MAGIC[kind]), 0);
  writeUint32(header, VERSION_OFFSET, FORMAT_VERSION);
}

/** Throws unless `header` starts like a `kind` file of this format. */
export function checkPrefix(
  header: Uint8Array,
  kind: FileKind,
  fileName: string,
): void {
  const state = prefixState(header, kind);
  switch (state.kind) {
    case "current": {
      return;
    }
    case "other-version": {
      throw new UnsupportedFormatError(fileName, state.version, FORMAT_VERSION);
    }
    case "foreign": {
      throw new CorruptError(`"${fileName}" is not an opfsql ${kind} file`);
    }
  }
}

export function hasPrefix(header: Uint8Array, kind: FileKind): boolean {
  return prefixState(header, kind).kind === "current";
}

function prefixState(header: Uint8Array, kind: FileKind): PrefixState {
  if (startsWith(header, MAGIC_BYTES[kind])) return versionState(header);
  // Not this kind's magic: an earlier format, or not an opfsql file at all.
  const magic = decoder.decode(header.subarray(0, MAGIC_SIZE));
  const legacy = LEGACY_MAGIC.exec(magic);
  if (legacy !== null) {
    return { kind: "other-version", version: Number(legacy[1]) };
  }
  return { kind: "foreign" };
}

/** The state of a file whose magic is this kind's: its version decides. */
function versionState(header: Uint8Array): PrefixState {
  const version = readUint32(header, VERSION_OFFSET);
  if (version !== FORMAT_VERSION) return { kind: "other-version", version };
  return { kind: "current" };
}

function startsWith(bytes: Uint8Array, prefix: Uint8Array): boolean {
  return prefix.every((byte, index) => bytes[index] === byte);
}
