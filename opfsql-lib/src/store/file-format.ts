import { StorageError, UnsupportedFormatError } from "./errors.js";

// ---------------------------------------------------------------------------
// Every file of a database starts with the same prefix:
//
//   [0..7]   magic           8 ASCII bytes naming the kind of file
//   [8..11]  format version  u32 BE
//
// The version covers the whole database — all its files change format
// together — so opening any file written in another format fails with
// UnsupportedFormatError instead of misreading it.
// ---------------------------------------------------------------------------

export const FORMAT_VERSION = 4;
export const FORMAT_PREFIX_SIZE = 12;

export type FileKind = "database" | "wal" | "shm";

const MAGIC: Record<FileKind, string> = {
  database: "OPFSQLDB",
  wal: "OPFSQLWL",
  shm: "OPFSQLSM",
};

/** Up to format 3 the version was the last two characters of the magic. */
const LEGACY_MAGIC = /^OPFSQL(\d\d)$/;

export function writeFormatPrefix(header: Uint8Array, kind: FileKind): void {
  header.set(new TextEncoder().encode(MAGIC[kind]), 0);
  new DataView(header.buffer, header.byteOffset).setUint32(
    8,
    FORMAT_VERSION,
    false,
  );
}

/** Throws unless `header` starts with the prefix of a `kind` file in this format. */
export function checkFormatPrefix(
  header: Uint8Array,
  kind: FileKind,
  file: string,
): void {
  const problem = prefixProblem(header, kind, file);
  if (problem) throw problem;
}

/** True if `header` is a valid prefix of a `kind` file in this format. */
export function hasFormatPrefix(header: Uint8Array, kind: FileKind): boolean {
  return prefixProblem(header, kind, kind) === null;
}

function prefixProblem(
  header: Uint8Array,
  kind: FileKind,
  file: string,
): StorageError | null {
  const magic = new TextDecoder().decode(header.subarray(0, 8));
  const legacy = LEGACY_MAGIC.exec(magic);
  if (legacy) {
    return new UnsupportedFormatError(file, Number(legacy[1]), FORMAT_VERSION);
  }
  if (magic !== MAGIC[kind]) {
    return new StorageError(`"${file}" is not an opfsql ${kind} file`);
  }
  const version = new DataView(header.buffer, header.byteOffset).getUint32(
    8,
    false,
  );
  return version === FORMAT_VERSION
    ? null
    : new UnsupportedFormatError(file, version, FORMAT_VERSION);
}
