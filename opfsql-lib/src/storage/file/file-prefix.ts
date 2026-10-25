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

const VERSION_OFFSET = 8;

const encoder = new TextEncoder();
const decoder = new TextDecoder();

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
  const problem = prefixProblem(header, kind, fileName);
  if (problem !== null) throw problem;
}

export function hasPrefix(header: Uint8Array, kind: FileKind): boolean {
  return prefixProblem(header, kind, kind) === null;
}

function prefixProblem(
  header: Uint8Array,
  kind: FileKind,
  fileName: string,
): Error | null {
  const magic = decoder.decode(header.subarray(0, 8));
  const legacy = LEGACY_MAGIC.exec(magic);
  if (legacy !== null) {
    return new UnsupportedFormatError(
      fileName,
      Number(legacy[1]),
      FORMAT_VERSION,
    );
  }
  if (magic !== MAGIC[kind]) {
    return new CorruptError(`"${fileName}" is not an opfsql ${kind} file`);
  }
  const version = readUint32(header, VERSION_OFFSET);
  return version === FORMAT_VERSION
    ? null
    : new UnsupportedFormatError(fileName, version, FORMAT_VERSION);
}
